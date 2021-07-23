CREATE OR REPLACE FUNCTION bucardo.validate_sync(text,integer)
RETURNS TEXT
LANGUAGE plperlu
SECURITY DEFINER
AS
$bc$

## Connect to all (active) databases used in a sync
## Verify table structures are the same
## Add delta relations as needed

use strict;
use warnings;
use DBI;
use Try::Tiny;

my $syncname = shift;

elog(LOG, "Starting validate_sync for $syncname");

## If force is set, we don't hesitate to drop indexes, etc.
my $force = shift || 0;

## Common vars
my ($rv,$SQL,%cache,$msg);

## Grab information about this sync from the database
(my $safesyncname = $syncname) =~ s/'/''/go;
$SQL = "SELECT * FROM sync WHERE name = '$safesyncname'";
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, "No such sync: $syncname");
}

my $info = $rv->{rows}[0];

## Does this herd exist?
(my $herd = $info->{herd}) =~ s/'/''/go;
$SQL = qq{SELECT 1 FROM herd WHERE name = '$herd'};
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(ERROR, "No such relgroup: $herd");
}

## Grab information on all members of this herd
$SQL = qq{
        SELECT id, db, schemaname, tablename, pkey, pkeytype, reltype, 
               autokick AS goatkick,
               pg_catalog.quote_ident(db)         AS safedb,
               pg_catalog.quote_ident(schemaname) AS safeschema,
               pg_catalog.quote_ident(tablename)  AS safetable,
               pg_catalog.quote_ident(pkey)       AS safepkey
        FROM   goat g, herdmap h
        WHERE  g.id = h.goat
        AND    h.herd = '$herd'
    };
$rv = spi_exec_query($SQL);
if (!$rv->{processed}) {
    elog(WARNING, "Relgroup has no members: $herd");
    return qq{Herd "$herd" for sync "$syncname" has no members: cannot validate};
}

my $number_sync_relations = $rv->{processed};

## Create a simple hash so we can look up the information by schema then table name
my %goat;
for my $x (@{$rv->{rows}}) {
    $goat{$x->{schemaname}}{$x->{tablename}} = $x;
}

## Map to the actual table names used by looking at the customname table
my %customname;
$SQL = q{SELECT goat,newname,db,COALESCE(db,'') AS db, COALESCE(sync,'') AS sync FROM bucardo.customname};
$rv = spi_exec_query($SQL);
for my $x (@{$rv->{rows}}) {
    ## Ignore if this is for some other sync
    next if length $x->{sync} and $x->{sync} ne $syncname;

    $customname{$x->{goat}}{$x->{db}} = $x->{newname};
}

## Grab information from each of the databases
my %db;
(my $dbs = $info->{dbs}) =~ s/'/''/go;
$SQL = qq{
        SELECT m.db, m.role, pg_catalog.quote_ident(m.db) AS safedb, d.status, d.dbtype
        FROM   dbmap m
        JOIN   db d ON (d.name = m.db)
        WHERE  dbgroup = '$dbs'
    };
$rv = spi_exec_query($SQL);
if (!@{$rv->{rows}}) {
    elog(ERROR, qq{Could not find a dbgroup of $dbs});
}

## We also want to count up each type of role
my %role = (
    source => 0,
    target => 0,
    fullcopy => 0,
);

for (@{$rv->{rows}}) {
    $db{$_->{db}} = {
        safename => $_->{safedb},
        role => $_->{role},
        status => $_->{status},
        dbtype => $_->{dbtype},
    };
    $role{$_->{role}}++;
}

## No source databases? Does not compute!
if ($role{source} < 1) {
    die "Invalid dbgroup: at least one database must have a role of 'source'!\n";
}

## Unless we are fullcopy, we must have PKs on each table
my $is_fullcopy = (! $role{target} and $role{fullcopy}) ? 1 : 0;
if (! $is_fullcopy) {
    for my $schema (sort keys %goat) {
        for my $table (sort keys %{$goat{$schema}}) {
            next if $goat{$schema}{$table}{reltype} ne 'table';
            if (! $goat{$schema}{$table}{pkey}) {
                elog(ERROR, qq{Table "$schema.$table" must specify a primary key!});
            }
        }
    }
}

my $run_sql = sub {
    my ($sql,$dbh) = @_;
    elog(DEBUG, "SQL: $sql");
    $dbh->do($sql);
};


my $fetch1_sql = sub {
    my ($sql,$dbh,@items) = @_;
    $sql =~ s/\t/    /gsm;
    if ($sql =~ /^(\s+)/m) {
        (my $ws = $1) =~ s/[^ ]//g;
        my $leading = length($ws);
        $sql =~ s/^\s{$leading}//gsm;
    }
    my $sth = $dbh->prepare_cached($sql);
    $sth->execute(@items);
    return $sth->fetchall_arrayref()->[0][0];
};

## Determine the name of some functions we may need
my $namelen = length($syncname);
my $kickfunc = $namelen <= 48
    ? "bucardo_kick_$syncname" : $namelen <= 62
    ? "bkick_$syncname"
    : sprintf 'bucardo_kick_%d', int (rand(88888) + 11111);

## Not used yet, but will allow for selective recreation of various items below
my %force;

## Open a connection to each active database
## Create the bucardo superuser if needed
## Install the plpgsql language if needed
## We do the source ones first as all their columns must exist on all other databases
for my $dbname (sort { ($db{$b}{role} eq 'source') <=> ($db{$a}{role} eq 'source') } keys %db) {

    ## Skip if this database is not active
    next if $db{$dbname}{status} ne 'active';

    ## Skip if this is a flatfile
    next if $db{$dbname}{dbtype} =~ /flat/;

    ## Skip if this is a non-supported database
    next if $db{$dbname}{dbtype} =~ /drizzle|mariadb|mongo|mysql|oracle|redis|sqlite|firebird/;

    ## Figure out how to connect to this database
    my $rv = spi_exec_query("SELECT bucardo.db_getconn('$dbname') AS conn");
    $rv->{processed} or elog(ERROR, qq{Error: Could not find a database named "$dbname"\n});
    my ($dbtype,$dsn,$user,$pass,$ssp) = split /\n/ => $rv->{rows}[0]{conn};
    $dsn =~ s/^DSN://;
    elog(DEBUG, "Connecting to $dsn as $user inside bucardo_validate_sync for language check");
    my $dbh;
    eval {
        ## Cache this connection so we only have to connect one time
        $dbh = $cache{dbh}{$dbname} = DBI->connect
            ($dsn, $user, $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
    };
    if ($@) {
        ## If the error might be because the bucardo user does not exist yet,
        ## try again with the postgres user (and create the bucardo user!)
        if ($@ =~ /"bucardo"/ and $user eq 'bucardo') {
            elog(DEBUG, 'Failed connection, trying as user postgres');
            my $tempdbh = DBI->connect($dsn, 'postgres', $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
            $tempdbh->do('SET TRANSACTION READ WRITE');
            $tempdbh->do('CREATE USER bucardo SUPERUSER');
            $tempdbh->commit();
            $tempdbh->disconnect();

            ## Reconnect the same as above, with the new bucardo user
            $dbh = $cache{dbh}{$dbname} = DBI->connect
                ($dsn, $user, $pass, {AutoCommit=>0, RaiseError=>1, PrintError=>0});
            warn "Created superuser bucardo on database $dbname\n";
        } else {
            ## Any other connection error is a simple exception
            die $@;
        }
    }

    ## If server_side_prepares is off for this database, set it now
    $ssp or $dbh->{pg_server_prepare} = 0;

    ## Just in case this database is set to read-only
    $dbh->do('SET TRANSACTION READ WRITE');

    ## To help comparisons, remove any unknown search_paths
    $dbh->do('SET LOCAL search_path = pg_catalog');

    ## Prepare some common SQL:
    my (%sth,$sth,$count,$x,%col);

    ## Does a named schema exist?
    $SQL = q{SELECT 1 FROM pg_namespace WHERE nspname = ?};
    $sth{hazschema} = $dbh->prepare($SQL);

    ## Does a named column exist on a specific table?
    $SQL = q{SELECT 1 FROM pg_attribute WHERE attrelid = }
          .q{(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid=c.relnamespace)}
          .q{ AND nspname=? AND relname=?) AND attname = ?};
    $sth{hazcol} = $dbh->prepare($SQL);

    ## Get a list of all tables and indexes in the bucardo schema for ease below
    $SQL = q{SELECT c.oid,relkind,relname FROM pg_class c JOIN pg_namespace n ON (n.oid=c.relnamespace) WHERE nspname='bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%btableoid, %bindexoid);
    for my $row (@{$sth->fetchall_arrayref()}) {
        if ($row->[1] eq 'r') {
            $btableoid{$row->[2]} = $row->[0];
        }
        if ($row->[1] eq 'i') {
            $bindexoid{$row->[2]} = $row->[0];
        }
    }

    ## We may need to optimize some calls below for very large numbers of relations
    ## Thus, it helps to know how many this database has in total
    $sth = $dbh->prepare(q{SELECT count(*) FROM pg_class WHERE relkind IN ('r','S')});
    $sth->execute();
    my $relation_count = $sth->fetchall_arrayref()->[0][0];
 
    ## Get a list of all functions in the bucardo schema
    $SQL = q{SELECT p.oid,proname FROM pg_proc p JOIN pg_namespace n ON (n.oid=p.pronamespace) WHERE nspname='bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%bfunctionoid);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $bfunctionoid{$row->[1]} = $row->[0];
    }

    ## Get a list of all triggers that start with 'bucardo'
    $SQL = q{SELECT nspname, relname, tgname FROM pg_trigger t
       JOIN pg_class c ON (c.oid=t.tgrelid)
       JOIN pg_namespace n ON (n.oid = c.relnamespace)
       WHERE tgname ~ '^bucardo'};
    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my (%btriggerinfo);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $btriggerinfo{$row->[0]}{$row->[1]}{$row->[2]} = 1;
    }

    ## Unless we are strictly fullcopy, put plpgsql in place on all source dbs
    ## We also will need a bucardo schema
    my $role = $db{$dbname}{role};
    if ($role eq 'source' and ! $is_fullcopy) {
        ## Perform the check for plpgsql
        $SQL = q{SELECT count(*) FROM pg_language WHERE lanname = 'plpgsql'};
        my $count = $dbh->selectall_arrayref($SQL)->[0][0];
        if ($count < 1) {
            $dbh->do('CREATE LANGUAGE plpgsql');
            $dbh->commit();
            warn "Created language plpgsql on database $dbname\n";
        }

        ## Create the bucardo schema as needed
        $sth = $sth{hazschema};
        $count = $sth->execute('bucardo');
        $sth->finish();
        if ($count < 1) {
            $dbh->do('CREATE SCHEMA bucardo');
        }
        my $newschema = $count < 1 ? 1 : 0;

my @functions = (

{ name => 'bucardo_tablename_maker', args => 'text', returns => 'text', vol => 'immutable', body => q{
DECLARE
  tname TEXT;
  newname TEXT;
  hashed TEXT;
BEGIN
  -- Change the first period to an underscore
  SELECT INTO tname REPLACE($1, '.', '_');
  -- Assumes max_identifier_length is 63
  -- Because even if not, we'll still abbreviate for consistency and portability
  SELECT INTO newname SUBSTRING(tname FROM 1 FOR 57);
  IF (newname != tname) THEN
    SELECT INTO newname SUBSTRING(tname FROM 1 FOR 46)
      || '!'
      || SUBSTRING(MD5(tname) FROM 1 FOR 10);
  END IF;
  -- We let Postgres worry about the quoting details
  SELECT INTO newname quote_ident(newname);
  RETURN newname;
END;
}
},

{ name => 'bucardo_tablename_maker', args => 'text, text', returns => 'text', vol => 'immutable', body => q{
DECLARE
  newname TEXT;
BEGIN
  SELECT INTO newname bucardo.bucardo_tablename_maker($1);

  -- If it has quotes around it, we expand the quotes to include the prefix
  IF (POSITION('"' IN newname) >= 1) THEN
    newname = REPLACE(newname, '"', '');
    newname = '"' || $2 || newname || '"';
  ELSE
    newname = $2 || newname;
  END IF;

  RETURN newname;
END;
}
},

{ name => 'bucardo_delta_names_helper', args => '', returns => 'trigger', vol => 'immutable', body => q{
BEGIN
  IF NEW.deltaname IS NULL THEN
    NEW.deltaname = bucardo.bucardo_tablename_maker(NEW.tablename, 'delta_');
  END IF;
  IF NEW.trackname IS NULL THEN
    NEW.trackname = bucardo.bucardo_tablename_maker(NEW.tablename, 'track_');
  END IF;
  RETURN NEW;
END;
}
},

## Function to do a quick check of all deltas for a given sync
{ name => 'bucardo_delta_check', args => 'text, text', returns => 'SETOF TEXT', body => q{
DECLARE
  myst TEXT;
  myrec RECORD;
  mycount INT;
BEGIN
  FOR myrec IN
    SELECT * FROM bucardo.bucardo_delta_names
      WHERE sync = $1 
      ORDER BY tablename
  LOOP

    RAISE DEBUG 'GOT % and %', myrec.deltaname, myrec.tablename;

    myst = $$
      SELECT  1
      FROM    bucardo.$$ || myrec.deltaname || $$ d
      WHERE   NOT EXISTS (
        SELECT 1
        FROM   bucardo.$$ || myrec.trackname || $$ t
        WHERE  d.txntime = t.txntime
        AND    (t.target = '$$ || $2 || $$'::text OR t.target ~ '^T:')
      ) LIMIT 1$$;
    EXECUTE myst;
    GET DIAGNOSTICS mycount = ROW_COUNT;

    IF mycount>=1 THEN
      RETURN NEXT '1,' || myrec.tablename;
    ELSE
      RETURN NEXT '0,' || myrec.tablename;
    END IF;

  END LOOP;
  RETURN;
END;
}
},

## Function to write to the tracking table upon a truncation
{ name => 'bucardo_note_truncation', args => '', returns => 'trigger', body => q{
DECLARE
  mytable TEXT;
  myst TEXT;
BEGIN
  INSERT INTO bucardo.bucardo_truncate_trigger(tablename,sname,tname,sync)
    VALUES (TG_RELID, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_ARGV[0]);

  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'delta_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'track_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  -- Not strictly necessary, but nice to have a clean slate
  SELECT INTO mytable
    bucardo.bucardo_tablename_maker(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 'stage_');
  myst = 'TRUNCATE TABLE bucardo.' || mytable;
  EXECUTE myst;

  RETURN NEW;
END;
}
},

## Function to remove duplicated entries from the bucardo_delta tables
{ name => 'bucardo_compress_delta', args => 'text, text', returns => 'text', body => q{
DECLARE
  mymode TEXT;
  myoid OID;
  myst TEXT;
  got2 bool;
  drows BIGINT = 0;
  trows BIGINT = 0;
  rnames TEXT;
  rname TEXT;
  rnamerec RECORD;
  ids_where TEXT;
  ids_sel TEXT;
  ids_grp TEXT;
  idnum TEXT;
BEGIN

  -- Are we running in a proper mode?
  SELECT INTO mymode current_setting('transaction_isolation');
  IF (mymode <> 'serializable' AND mymode <> 'repeatable read') THEN
    RAISE EXCEPTION 'This function must be run in repeatable read mode';
  END IF;

  -- Grab the oid of this schema/table combo
  SELECT INTO myoid
    c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE nspname = $1 AND relname = $2;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'No such table: %.%', $1, $2;
  END IF;

  ids_where = 'COALESCE(rowid,''NULL'') = COALESCE(id, ''NULL'')';
  ids_sel = 'rowid AS id';
  ids_grp = 'rowid';
  FOR rnamerec IN SELECT attname FROM pg_attribute WHERE attrelid =
    (SELECT oid FROM pg_class WHERE relname = 'bucardo_delta'
     AND relnamespace =
     (SELECT oid FROM pg_namespace WHERE nspname = 'bucardo') AND attname ~ '^rowid'
    ) LOOP
    rname = rnamerec.attname;
    rnames = COALESCE(rnames || ' ', '') || rname ;
    SELECT INTO idnum SUBSTRING(rname FROM '[[:digit:]]+');
    IF idnum IS NOT NULL THEN
      ids_where = ids_where 
      || ' AND (' 
      || rname
      || ' = id'
      || idnum
      || ' OR ('
      || rname
      || ' IS NULL AND id'
      || idnum
      || ' IS NULL))';
      ids_sel = ids_sel
      || ', '
      || rname
      || ' AS id'
      || idnum;
      ids_grp = ids_grp
      || ', '
      || rname;
    END IF;
  END LOOP;

  myst = 'DELETE FROM bucardo.bucardo_delta 
    USING (SELECT MAX(txntime) AS maxt, '||ids_sel||'
    FROM bucardo.bucardo_delta
    WHERE tablename = '||myoid||'
    GROUP BY ' || ids_grp || ') AS foo
    WHERE tablename = '|| myoid || ' AND ' || ids_where ||' AND txntime <> maxt';
  RAISE DEBUG 'Running %', myst;
  EXECUTE myst;

  GET DIAGNOSTICS drows := row_count;

  myst = 'DELETE FROM bucardo.bucardo_track'
    || ' WHERE NOT EXISTS (SELECT 1 FROM bucardo.bucardo_delta d WHERE d.txntime = bucardo_track.txntime)';
  EXECUTE myst;

  GET DIAGNOSTICS trows := row_count;

  RETURN 'Compressed '||$1||'.'||$2||'. Rows deleted from bucardo_delta: '||drows||
    ' Rows deleted from bucardo_track: '||trows;
END;
} ## end of bucardo_compress_delta body
},

{ name => 'bucardo_compress_delta', args => 'text', returns => 'text', language => 'sql', body => q{
SELECT bucardo.bucardo_compress_delta(n.nspname, c.relname)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE relname = $1 AND pg_table_is_visible(c.oid);
}
},

{ name => 'bucardo_compress_delta', args => 'oid', returns => 'text', language => 'sql', body => q{
SELECT bucardo.bucardo_compress_delta(n.nspname, c.relname)
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.oid = $1;
}
},

## The main vacuum function to clean up the delta and track tables
{ name => 'bucardo_purge_delta_oid', 'args' => 'text, oid', returns => 'text', body => q{
DECLARE
  deltatable TEXT;
  tracktable TEXT;
  dtablename TEXT;
  myst TEXT;
  drows BIGINT = 0;
  trows BIGINT = 0;
BEGIN
  -- Store the schema and table name
  SELECT INTO dtablename
    quote_ident(nspname)||'.'||quote_ident(relname)
    FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)
    WHERE c.oid = $2;

  -- See how many dbgroups are being used by this table
  SELECT INTO drows 
    COUNT(DISTINCT target)
    FROM bucardo.bucardo_delta_targets
    WHERE tablename = $2;
  RAISE DEBUG 'delta_targets rows found for %: %', dtablename, drows;

  -- If no dbgroups, no point in going on, as we will never purge anything
  IF drows < 1 THEN
    RETURN 'Nobody is using table '|| dtablename ||', according to bucardo_delta_targets';
  END IF;

  -- Figure out the names of the delta and track tables for this relation
  SELECT INTO deltatable
    bucardo.bucardo_tablename_maker(dtablename, 'delta_');
  SELECT INTO tracktable
    bucardo.bucardo_tablename_maker(dtablename, 'track_');

  -- Delete all txntimes from the delta table that:
  -- 1) Have been used by all dbgroups listed in bucardo_delta_targets
  -- 2) Have a matching txntime from the track table
  -- 3) Are older than the first argument interval
  myst = 'DELETE FROM bucardo.'
  || deltatable
  || ' USING (SELECT txntime AS tt FROM bucardo.'
  || tracktable 
  || ' GROUP BY 1 HAVING COUNT(*) = '
  || drows
  || ') AS foo'
  || ' WHERE txntime = tt'
  || ' AND txntime < now() - interval '
  || quote_literal($1);

  EXECUTE myst;

  GET DIAGNOSTICS drows := row_count;

  -- Now that we have done that, we can remove rows from the track table
  -- which have no match at all in the delta table
  myst = 'DELETE FROM bucardo.'
  || tracktable
  || ' WHERE NOT EXISTS (SELECT 1 FROM bucardo.'
  || deltatable
  || ' d WHERE d.txntime = bucardo.'
  || tracktable
  || '.txntime)';

  EXECUTE myst;

  GET DIAGNOSTICS trows := row_count;

  RETURN 'Rows deleted from '
  || deltatable
  || ': '
  || drows
  || ' Rows deleted from '
  || tracktable
  || ': '
  || trows;

END;
} ## end of bucardo_purge_delta_oid body
},

{ name => 'bucardo_purge_delta', args => 'text', returns => 'text', body => q{
DECLARE
  myrec RECORD;
  myrez TEXT;
  total INTEGER = 0;
BEGIN

  SET LOCAL search_path = pg_catalog;

  -- Grab all potential tables to be vacuumed by looking at bucardo_delta_targets
  FOR myrec IN SELECT DISTINCT tablename FROM bucardo.bucardo_delta_targets where tablename in (select oid from pg_class where relkind='r') LOOP
    SELECT INTO myrez
      bucardo.bucardo_purge_delta_oid($1, myrec.tablename);
    RAISE NOTICE '%', myrez;
    total = total + 1;
  END LOOP;

  RETURN 'Tables processed: ' || total::text;

END;
} ## end of bucardo_purge_delta body
},

{ name => 'bucardo_purge_sync_track', args => 'text', returns => 'text', body => q{
DECLARE
  myrec RECORD;
  myst  TEXT;
BEGIN
  PERFORM 1 FROM bucardo.bucardo_delta_names WHERE sync = $1 LIMIT 1;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'No sync found named %', $1;
  END IF;

  FOR myrec IN SELECT DISTINCT tablename, deltaname, trackname
    FROM bucardo.bucardo_delta_names WHERE sync = $1
    ORDER BY tablename LOOP

    myst = 'INSERT INTO bucardo.'
    || myrec.trackname
    || ' SELECT DISTINCT txntime, '
    || quote_literal($1)
    || ' FROM bucardo.'
    || myrec.deltaname;

    RAISE DEBUG 'Running: %', myst;

    EXECUTE myst;

  END LOOP;

  RETURN 'Complete';

END;
} ## end of bucardo_purge_sync_track body
},


); ## end of %functions

   for my $info (@functions) {
       my $funcname = $info->{name};
       my ($oldmd5,$newmd5) = (0,1);
       $SQL = 'SELECT md5(prosrc), md5(?) FROM pg_proc WHERE proname=? AND oidvectortypes(proargtypes)=?';
       my $sthmd5 = $dbh->prepare($SQL);
       $count = $sthmd5->execute(" $info->{body} ", $funcname, $info->{args});
       if ($count < 1) {
           $sthmd5->finish();
       }
       else {
           ($oldmd5,$newmd5) = @{$sthmd5->fetchall_arrayref()->[0]};
       }
       if ($oldmd5 ne $newmd5) {
           my $language = $info->{language} || 'plpgsql';
           my $volatility = $info->{vol} || 'VOLATILE';
           $SQL = "
CREATE OR REPLACE FUNCTION bucardo.$funcname($info->{args})
RETURNS $info->{returns}
LANGUAGE $language
$volatility
SECURITY DEFINER
AS \$clone\$ $info->{body} \$clone\$";
           elog(DEBUG, "Writing function $funcname($info->{args})");
           $run_sql->($SQL,$dbh);
       }
   }

        ## Create the 'kickfunc' function as needed
        if (exists $bfunctionoid{$kickfunc}) {
            ## We may want to recreate this function
            if ($force{all} or $force{funcs} or $force{kickfunc}) {
                $dbh->do(qq{DROP FUNCTION bucardo."$kickfunc"()});
                delete $bfunctionoid{$kickfunc};
            }
        }

        if (! exists $bfunctionoid{$kickfunc}) {
            ## We may override this later on with a custom function from bucardo_custom_trigger
            ## and we may not even use it all, but no harm in creating the stock one here
            my $notice = $dbh->{pg_server_version} >= 90000
                ? qq{bucardo, 'kick_sync_$syncname'}
                : qq{"bucardo_kick_sync_$syncname"};
            $SQL = qq{
                  CREATE OR REPLACE FUNCTION bucardo."$kickfunc"()
                  RETURNS TRIGGER
                  VOLATILE
                  LANGUAGE plpgsql
                  AS \$notify\$
                  BEGIN
                    EXECUTE \$nn\$NOTIFY $notice\$nn\$;
                  RETURN NEW;
                  END;
                  \$notify\$;
                 };
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_delta_names table as needed
        if (! exists $btableoid{'bucardo_delta_names'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_delta_names (
                        sync TEXT,
                        tablename TEXT,
                        deltaname TEXT,
                        trackname TEXT,
                        cdate TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = qq{CREATE UNIQUE INDEX bucardo_delta_names_unique ON bucardo.bucardo_delta_names (sync,tablename)};
            $run_sql->($SQL,$dbh);

            $SQL = qq{
CREATE TRIGGER bucardo_delta_namemaker
BEFORE INSERT OR UPDATE
ON bucardo.bucardo_delta_names
FOR EACH ROW EXECUTE PROCEDURE bucardo.bucardo_delta_names_helper();
            };
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_delta_targets table as needed
        if (! exists $btableoid{'bucardo_delta_targets'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_delta_targets (
                        tablename  OID         NOT NULL,
                        target     TEXT        NOT NULL,
                        cdate      TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);
        }

        ## Rename the target column from 'sync' as older versions used that
        $sth = $sth{hazcol};
        $count = $sth->execute('bucardo', 'bucardo_delta_targets', 'sync');
        $sth->finish();
        if (1 == $count) {
            ## Change the name!
            $SQL = qq{ALTER TABLE bucardo.bucardo_delta_targets RENAME sync TO target};
            $run_sql->($SQL,$dbh);
        }

        ## Check for missing 'target' column in the bucardo_delta_target table
        $sth = $sth{hazcol};
        $count = $sth->execute('bucardo', 'bucardo_delta_targets', 'target');
        $sth->finish();
        if ($count < 1) {
            ## As the new column cannot be null, we have to delete existing entries!
            ## However, missing this column is a pretty obscure corner-case
            $SQL = qq{DELETE FROM bucardo.bucardo_delta_targets};
            $run_sql->($SQL,$dbh);
            $SQL = qq{
                    ALTER TABLE bucardo.bucardo_delta_targets
                      ADD COLUMN target TEXT NOT NULL;
            };
            $run_sql->($SQL,$dbh);
        }

        ## Get a list of oids and relkinds for all of our goats
        ## This is much faster than doing individually
        $SQL = q{SELECT n.nspname,c.relname,relkind,c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)};

        ## If this is a very large statement, it might be more efficient to not use a WHERE clause!
        if ($relation_count > 1000 and $number_sync_relations / $relation_count > 0.05) {
            elog(DEBUG, "Too many relations for a WHERE clause! (would ask for $number_sync_relations or $relation_count rows)");
            $sth = $dbh->prepare($SQL);
            $sth->execute();
        }
        else {
            $SQL .= ' WHERE ';
            my @args;
            for my $schema (sort keys %goat) {
                for my $table (sort keys %{$goat{$schema}}) {
                    $SQL .= '(nspname = ? AND relname = ?) OR ';
                    push @args => $schema, $table;
                }
            }
            $SQL =~ s/OR $//;
            $sth = $dbh->prepare($SQL);
            $sth->execute(@args);
        }
 
        my %tableoid;
        my %sequenceoid;
        for my $row (@{$sth->fetchall_arrayref()}) {
            if ($row->[2] eq 'r') {
                $tableoid{"$row->[0].$row->[1]"} = $row->[3];
            }
            if ($row->[2] eq 'S') {
                $sequenceoid{"$row->[0].$row->[1]"} = $row->[3];
            }
        }

        ## Grab all the information inside of bucardo_delta_targets
        my $targetname = "dbgroup $info->{dbs}";
        $SQL = 'SELECT DISTINCT tablename FROM bucardo.bucardo_delta_targets WHERE target = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($targetname);
        my $targetoid = $sth->fetchall_hashref('tablename');

        ## Populate bucardo_delta_targets with this dbgroup name
        $SQL = 'INSERT INTO bucardo.bucardo_delta_targets(tablename,target) VALUES (?,?)';
        my $stha = $dbh->prepare($SQL);
        for my $schema (sort keys %goat) {
            for my $table (sort keys %{$goat{$schema}}) {
                next if ! exists $tableoid{"$schema.$table"};
                my $oid = $tableoid{"$schema.$table"};
                next if exists $targetoid->{$oid};
                $stha->execute($oid, $targetname);
            }
        }

        ## Delete any tables that are no longer in the database.
        $dbh->do(q{
            DELETE FROM bucardo.bucardo_delta_targets
             WHERE NOT EXISTS (SELECT oid FROM pg_class WHERE oid = tablename)
        });

        ## Create the bucardo_truncate_trigger table as needed
        if (! exists $btableoid{'bucardo_truncate_trigger'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_truncate_trigger (
                        tablename   OID         NOT NULL,
                        sname       TEXT        NOT NULL,
                        tname       TEXT        NOT NULL,
                        sync        TEXT        NOT NULL,
                        replicated  TIMESTAMPTZ     NULL,
                        cdate       TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = q{CREATE INDEX bucardo_truncate_trigger_index ON }
                . q{bucardo.bucardo_truncate_trigger (sync, tablename) WHERE replicated IS NULL};
            $run_sql->($SQL,$dbh);
        }

        ## Create the bucardo_truncate_trigger_log table as needed
        if (! exists $btableoid{'bucardo_truncate_trigger_log'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_truncate_trigger_log (
                        tablename   OID         NOT NULL,
                        sname       TEXT        NOT NULL,
                        tname       TEXT        NOT NULL,
                        sync        TEXT        NOT NULL,
                        target      TEXT        NOT NULL,
                        replicated  TIMESTAMPTZ NOT NULL,
                        cdate       TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                };
            $run_sql->($SQL,$dbh);
        }

        if (exists $btableoid{'bucardo_sequences'}) {
            ## Check for older version of bucardo_sequences table
            $SQL = q{SELECT count(*) FROM pg_attribute WHERE attname = 'targetname' }
                  .q{ AND attrelid = (SELECT c.oid FROM pg_class c, pg_namespace n }
                  .q{ WHERE n.oid = c.relnamespace AND n.nspname = 'bucardo' }
                  .q{ AND c.relname = 'bucardo_sequences')};
            if ($dbh->selectall_arrayref($SQL)->[0][0] < 1) {
                warn "Dropping older version of bucardo_sequences, then recreating empty\n";
                $dbh->do('DROP TABLE bucardo.bucardo_sequences');
                delete $btableoid{'bucardo_sequences'};
            }
        }
        if (! exists $btableoid{'bucardo_sequences'}) {
            $SQL = qq{
                    CREATE TABLE bucardo.bucardo_sequences (
                        schemaname   TEXT        NOT NULL,
                        seqname      TEXT        NOT NULL,
                        syncname     TEXT        NOT NULL,
                        targetname   TEXT        NOT NULL,
                        last_value   BIGINT      NOT NULL,
                        start_value  BIGINT      NOT NULL,
                        increment_by BIGINT      NOT NULL,
                        max_value    BIGINT      NOT NULL,
                        min_value    BIGINT      NOT NULL,
                        is_cycled    BOOL        NOT NULL,
                        is_called    BOOL        NOT NULL
                    );
                };
            $run_sql->($SQL,$dbh);

            $SQL = q{CREATE UNIQUE INDEX bucardo_sequences_tablename ON }
                . q{bucardo.bucardo_sequences (schemaname, seqname, syncname, targetname)};
            $run_sql->($SQL,$dbh);
        }

    } ## end not fullcopy / all global items

    ## Build another list of information for each table
    ## This saves us multiple lookups
    $SQL = q{SELECT n.nspname,c.relname,relkind,c.oid FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace) WHERE };
    my $SQL2 = q{INSERT INTO bucardo.bucardo_delta_names VALUES };

    my (@args,@tablelist);

    for my $schema (sort keys %goat) {
        for my $table (sort keys %{$goat{$schema}}) {

            ## Map to the actual table name used, via the customname table
            my ($remoteschema,$remotetable) = ($schema,$table);

            ## The internal ID for this table
            my $id = $goat{$schema}{$table}{id};

            ## Is this a source or target database?
            ## Only pure targets can have a customname
            my $is_target = $role eq 'target';

            if ($is_target and exists $customname{$id}) {
                ## If there is an entry for this particular database, use that
                ## Otherwise, if there is a database-wide one, use that
                if (exists $customname{$id}{$dbname} or exists $customname{$id}{''}) {
                    $remotetable = $customname{$id}{$dbname} || $customname{$id}{''};

                    ## If this has a dot, change the schema as well
                    ## Otherwise, we simply use the existing schema
                    if ($remotetable =~ s/(.+)\.//) {
                        $remoteschema = $1;
                    }
                }
            }

            $SQL .= '(nspname = ? AND relname = ?) OR ';
            push @args => $remoteschema, $remotetable;
            if ($goat{$schema}{$table}{reltype} eq 'table') {
                push @tablelist => $syncname, $remoteschema, $remotetable;
            }

        } ## end each table

    } ## end each schema

    $SQL =~ s/OR $//;

    $sth = $dbh->prepare($SQL);
    $sth->execute(@args);

    my (%goatoid,@tableoids);
    for my $row (@{$sth->fetchall_arrayref()}) {
        $goatoid{"$row->[0].$row->[1]"} = [$row->[2],$row->[3]];
        push @tableoids => $row->[3] if $row->[2] eq 'r';
    }

    ## Populate the bucardo_delta_names table for this sync
    if ($role eq 'source' and ! $is_fullcopy and @tablelist) {
        $SQL = 'DELETE FROM bucardo.bucardo_delta_names WHERE sync = ?';
        $sth = $dbh->prepare($SQL);
        $sth->execute($syncname);
        $SQL = $SQL2;
        my $number = @tablelist / 3;
        $SQL .= q{(?,quote_ident(?)||'.'||quote_ident(?)),} x $number;
        chop $SQL;
        $sth = $dbh->prepare($SQL);
        $sth->execute(@tablelist);
    }
   
    ## Get column information about all of our tables
    $SQL = q{
            SELECT   attrelid, attname, quote_ident(attname) AS qattname, atttypid, format_type(atttypid, atttypmod) AS ftype,
                     attnotnull, atthasdef, attnum,
                     (SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef WHERE adrelid=attrelid
                      AND adnum=attnum AND atthasdef) AS def
            FROM     pg_attribute
            WHERE    attrelid IN (COLIST) AND attnum > 0 AND NOT attisdropped
            ORDER BY attnum
        };
    my $columninfo;
    if (@tableoids) {
        $SQL =~ s/COLIST/join ',' => @tableoids/e;
        $sth = $dbh->prepare($SQL);
        $sth->execute();
        for my $row (@{ $sth->fetchall_arrayref({}) }) {
            my $oid = $row->{attrelid};
            $columninfo->{$oid}{$row->{attname}} = $row;
        }
    }

    ## Check out each table in turn

  SCHEMA: for my $schema (sort keys %goat) {

        ## Does this schema exist?
        $sth = $sth{hazschema};
        $count = $sth->execute($schema);
        $sth->finish();
        if ($count < 1) {
            die qq{Could not find schema "$schema" in database "$dbname"!\n};
        }

      TABLE: for my $table (sort keys %{$goat{$schema}}) {

         ## Map to the actual table name used, via the customname table
         my ($remoteschema,$remotetable) = ($schema,$table);

         ## The internal ID for this table
         my $id = $goat{$schema}{$table}{id};

         ## Is this a source or target database?
         ## Only pure targets can have a customname
         my $is_target = $role eq 'target';

         if ($is_target and exists $customname{$id}) {
           ## If there is an entry for this particular database, use that
           ## Otherwise, if there is a database-wide one, use that
           if (exists $customname{$id}{$dbname} or exists $customname{$id}{''}) {
             $remotetable = $customname{$id}{$dbname} || $customname{$id}{''};

             ## If this has a dot, change the schema as well
             ## Otherwise, we simply use the existing schema
             if ($remotetable =~ s/(.+)\.//) {
               $remoteschema = $1;
             }
           }
         }

         if (! exists $goatoid{"$remoteschema.$remotetable"}) {
             die qq{Could not find "$remotetable" inside the "$remoteschema" schema on database "$dbname"!\n};
         }
         my ($relkind,$oid) = @{ $goatoid{"$remoteschema.$remotetable"} };

         ## Verify that this is the kind of relation we expect it to be
         my $tinfo = $goat{$schema}{$table};
         if ('r' eq $relkind) {
             if ('table' ne $tinfo->{reltype}) {
                 die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's a table, not a $tinfo->{reltype}!};
             }
         }
         elsif ('S' eq $relkind) {
             if ('sequence' ne $tinfo->{reltype}) {
                 die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's a sequence, not a $tinfo->{reltype}!};
             }
         }
         else {
             die qq{Found "$remoteschema.$remotetable" on database "$dbname", but it's neither a table nor a sequence!};
         }

         ## Nothing further needed if it's a sequence
         next TABLE if $tinfo->{reltype} eq 'sequence';

         ## Get the escaped version of things
         my $safeschema = $tinfo->{safeschema};
         my $safetable = $tinfo->{safetable};

         ## Go through each column in the tables to check against the other databases

         if (! exists $columninfo->{$oid}) {
             $sth->finish();
             die qq{Could not determine column information for table "$remoteschema.$remotetable"!\n};
         }

         my $colinfo = $columninfo->{$oid};
            ## Allow for 'dead' columns in the attnum ordering
            ## Turn the old keys (attname) into new keys (number)
            $x=1;
            for (sort { $colinfo->{$a}{attnum} <=> $colinfo->{$b}{attnum} } keys %$colinfo) {
                $colinfo->{$_}{realattnum} = $x++;
            }

            ## Things that will cause it to fail this sync
            my @problem;

            ## Things that are problematic but not a show-stopper
            my @warning;

            ## Is this the first time we've seen this table?
            ## If so, this becomes canonical entry
            my $t = "$schema.$table";
            if (! exists $col{$t}) {
                $col{$t} = $colinfo; ## hashref: key is column name
                $col{db} = $dbname;
            }
            else { ## Seen this before, so check against canonical list

                ## First, any columns that exist on a source but not this one is not allowed
                for my $c1 (sort keys %{$col{$t}}) {
                    if (! exists $colinfo->{$c1}) {
                        push @problem => "Column $t.$c1 exists on db $col{db} but not on db $dbname";
                    }
                }

                ## Any columns that exist here but not the original source may be a problem
                for my $c2 (sort keys %$colinfo) {
                    if (! exists $col{$t}{$c2}) {
                        my $msg = "Column $t.$c2 exists on db $dbname but not on db $col{db}";
                        if ($role eq 'source') {
                            push @problem => $msg;
                        } else {
                            push @warning => $msg;
                        }
                        next;    ## Skip to next column
                    }
                    my $c1 = $col{$t}{$c2};

                    ## Must be in the same order so we can COPY smoothly
                    ## Someday we can consider using a custom COPY list if the server supports it
                    if ($c1->{realattnum} != $c2->{realattnum}) {
                        push @problem => "Column $t.$c1 is in position $c2->{realattnum} on db $col{db}"
                            . " but in position $c1->{realattnum} on db $dbname";
                    }

                    ## Must be the same (or very compatible) datatypes
                    if ($c1->{ftype} ne $c2->{ftype}) {
                        $msg = "Column $t.$c1 is type $c1->{ftype} on db $col{db} but type $c2->{ftype} on db $dbname";
                                ## Carve out some known exceptions (but still warn about them)
                                ## Allowed: varchar == text
                        if (($c1->{ftype} eq 'character varying' and $c2->{ftype} eq 'text') or
                                ($c2->{ftype} eq 'character varying' and $c1->{ftype} eq 'text')) {
                            push @warning => $msg;
                        } else {
                            push @problem => $msg;
                        }
                    }

                    ## Warn of a notnull mismatch
                    if ($c1->{attnotnull} != $c2->{attnotnull}) {
                        push @warning => sprintf 'Column %s on db %s is %s but %s on db %s',
                            "$t.$c1", $col{db},
                                $c1->{attnotnull} ? 'NOT NULL' : 'NULL',
                                    $c2->{attnotnull} ? 'NOT NULL' : 'NULL',
                                        $dbname;
                    }

                    ## Warn of DEFAULT existence mismatch
                    if ($c1->{atthasdef} != $c2->{atthasdef}) {
                        push @warning => sprintf 'Column %s on db %s %s but %s on db %s',
                            "$t.$c1", $col{db},
                                $c1->{atthasdef} ? 'has a DEFAULT value' : 'has no DEFAULT value',
                                    $c2->{attnotnull} ? 'has none' : 'does',
                                        $dbname;
                    }

                }                ## end each column to check

            }              ## end check this against previous source db

            if (@problem) {
                $msg = "Column verification failed:\n";
                $msg .= join "\n" => @problem;
                die $msg;
            }

            if (@warning) {
                $msg = "Warnings found on column verification:\n";
                $msg .= join "\n" => @warning;
                warn $msg;
            }

            ## If this is not a source database, we don't need to go any further
            next if $role ne 'source';

            ## If this is a fullcopy only sync, also don't need to go any further
            next if $is_fullcopy;

            ## This is a source database and we need to track changes.
            ## First step: a way to add things to the bucardo_delta table

            ## We can only put a truncate trigger in if the database is 8.4 or higher
            if ($dbh->{pg_server_version} >= 80400) {
                ## Figure out the name of this trigger
                my $trunctrig = $namelen <= 42
                    ? "bucardo_note_trunc_$syncname" : $namelen <= 54
                        ? "btrunc_$syncname"
                            : sprintf 'bucardo_note_trunc_%d', int (rand(88888) + 11111);
                if (! exists $btriggerinfo{$schema}{$table}{$trunctrig}) {
                    $SQL = qq{
          CREATE TRIGGER "$trunctrig"
          AFTER TRUNCATE ON "$schema"."$table"
          FOR EACH STATEMENT EXECUTE PROCEDURE bucardo.bucardo_note_truncation('$syncname')
        };
                    try {
                        # Initial commit of all preceding logic so that dependencies are present.
                        $dbh->commit();
                        $run_sql->($SQL,$dbh);
                        # Commit the trigger.
                        $dbh->commit();
                    } catch {
                        elog(WARNING,"bucardo_note_trunc trigger on " . $schema . "." . $table . " could not be added.\n");
                        $dbh->rollback;
                    }
                }
            }

            $SQL = "SELECT bucardo.bucardo_tablename_maker(?)";
            my $makername = $fetch1_sql->($SQL,$dbh,$schema.'_'.$table);
            ## Create this table if needed, with one column per PK columns
            my $delta_table = "delta_$makername";
            my $index1_name = "dex1_$makername";
            my $index2_name = "dex2_$makername";
            my $deltafunc = "delta_$makername";
            my $track_table = "track_$makername";
            my $index3_name = "dex3_$makername";
            my $stage_table = "stage_$makername";
            ## Need to account for quoted versions, e.g. names with spaces
            if ($makername =~ s/"//g) {
              $delta_table = qq{"delta_$makername"};
              $index1_name = qq{"dex1_$makername"};
              $index2_name = qq{"dex2_$makername"};
              $deltafunc = qq{"delta_$makername"};
              $track_table = qq{"track_$makername"};
              $index3_name = qq{"dex3_$makername"};
              $stage_table = qq{"stage_$makername"};
            }
            ## Also need non-quoted versions to feed to execute()
            (my $noquote_delta_table = $delta_table) =~ s/^"(.+)"$/$1/;
            (my $noquote_index1_name = $index1_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_index2_name = $index2_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_deltafunc = $deltafunc) =~ s/^"(.+)"$/$1/;
            (my $noquote_track_table = $track_table) =~ s/^"(.+)"$/$1/;
            (my $noquote_index3_name = $index3_name) =~ s/^"(.+)"$/$1/;
            (my $noquote_stage_table = $stage_table) =~ s/^"(.+)"$/$1/;

            if (! exists $btableoid{$noquote_delta_table}) {
               ## Create that table!
               my $pkcols = join ',' => map { qq{"$_"} } split (/\|/ => $tinfo->{pkey});
               $SQL = qq{
                   CREATE TABLE bucardo.$delta_table
                     AS SELECT $pkcols, now()::TIMESTAMPTZ AS txntime
                        FROM "$schema"."$table" LIMIT 0
               };
               $run_sql->($SQL,$dbh);
               $SQL = qq{
                   ALTER TABLE bucardo.$delta_table
                     ALTER txntime SET NOT NULL,
                     ALTER txntime SET DEFAULT now()
               };
               $run_sql->($SQL, $dbh);
            }

            ## Need an index on the txntime column
            if (! exists $bindexoid{$noquote_index1_name}) {
                $SQL = qq{CREATE INDEX $index1_name ON bucardo.$delta_table(txntime)};
                $run_sql->($SQL, $dbh);
            }

            ## Need an index on all other columns
            if (! exists $bindexoid{$noquote_index2_name}) {
                my $pkcols = join ',' => map { qq{"$_"} } split (/\|/ => $tinfo->{pkey});
                $SQL = qq{CREATE INDEX $index2_name ON bucardo.$delta_table($pkcols)};
                $run_sql->($SQL, $dbh);
            }

            ## Track any change (insert/update/delete) with an entry in bucardo_delta

            ## Trigger function to add any changed primary key rows to this new table
            ## TODO: Check for too long of a name
            ## Function is same as the table name?

            my @pkeys = split (/\|/ => $tinfo->{pkey});

         if (! exists $bfunctionoid{$noquote_deltafunc}) {
                 my $new = join ',' => map { qq{NEW."$_"} } @pkeys;
                 my $old = join ',' => map { qq{OLD."$_"} } @pkeys;
                 my $clause = join ' OR ' => map { qq{OLD."$_" <> NEW."$_"} } @pkeys;
                $SQL = qq{
        CREATE OR REPLACE FUNCTION bucardo.$deltafunc()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SECURITY DEFINER
        VOLATILE
        AS
        \$clone\$
        BEGIN
        IF (TG_OP = 'INSERT') THEN
          INSERT INTO bucardo.$delta_table VALUES ($new);
        ELSIF (TG_OP = 'UPDATE') THEN
          INSERT INTO bucardo.$delta_table VALUES ($old);
          IF ($clause) THEN
            INSERT INTO bucardo.$delta_table VALUES ($new);
          END IF;
        ELSE
          INSERT INTO bucardo.$delta_table VALUES ($old);
        END IF;
        RETURN NULL;
        END;
        \$clone\$;
      };
                $run_sql->($SQL,$dbh);
            }

            ## Check if the bucardo_delta is a custom function, and create if needed
            $SQL = qq{SELECT trigger_language,trigger_body FROM bucardo_custom_trigger 
              WHERE goat=$tinfo->{id} 
              AND status='active' 
              AND trigger_type='delta'
    };
            elog(DEBUG, "Running $SQL");
            $rv = spi_exec_query($SQL);
            my $customdeltafunc = '';
            if ($rv->{processed}) {
                my $customdeltafunc = "bucardo_delta_$tinfo->{id}";

                if (! exists $bfunctionoid{$customdeltafunc}) {
                    $SQL = qq{
               CREATE OR REPLACE FUNCTION bucardo."$customdeltafunc"()
               RETURNS TRIGGER
               LANGUAGE $rv->{rows}[0]{trigger_language}
               SECURITY DEFINER
               VOLATILE
               AS
               \$clone\$
         };
                    $SQL .= qq{ $rv->{rows}[0]{trigger_body} };
                    $SQL .= qq{ \$clone\$; };
                    $run_sql->($SQL,$dbh);
                }
            }

            if (! exists $btriggerinfo{$schema}{$table}{'bucardo_delta'}) {
                my $func = $customdeltafunc || $deltafunc;
                $SQL = qq{
        CREATE TRIGGER bucardo_delta
        AFTER INSERT OR UPDATE OR DELETE ON "$schema"."$table"
        FOR EACH ROW EXECUTE PROCEDURE bucardo.$func()
      };
                try {
                    $run_sql->($SQL,$dbh);
                    $dbh->commit();
                } catch {
                    elog(WARNING,"bucardo_delta trigger on " . $schema . "." . $table . " could not be added.\n");
                    $dbh->rollback;
                }
            }


            ## Now the 'track' table
            if (! exists $btableoid{$noquote_track_table}) {
                $SQL = qq{
                   CREATE TABLE bucardo.$track_table (
                      txntime    TIMESTAMPTZ,
                      target     TEXT
                   );
                };
                $run_sql->($SQL,$dbh);
            }

            ## Need to index both columns of the txntime table
            if (! exists $bindexoid{$noquote_index3_name}) {
                $SQL = qq{CREATE INDEX $index3_name ON bucardo.$track_table(target text_pattern_ops, txntime)};
                $run_sql->($SQL,$dbh);
            }

            ## The 'stage' table, which feeds 'track' once targets have committed
            if (! exists $btableoid{$noquote_stage_table}) {
                my $unlogged = $dbh->{pg_server_version} >= 90100 ? 'UNLOGGED' : '';
                $SQL = qq{
                   CREATE $unlogged TABLE bucardo.$stage_table (
                      txntime    TIMESTAMPTZ,
                      target     TEXT
                   );
                };
                $run_sql->($SQL,$dbh);
            }

            my $indexname = 'bucardo_delta_target_unique';
            if (! exists $bindexoid{$indexname}) {
                $dbh->do(qq{CREATE INDEX $indexname ON bucardo.bucardo_delta_targets(tablename,target)});
                $bindexoid{$indexname} = 1;
            }

            ## Override the 'autokick' kick trigger if needed
            $SQL = qq{SELECT trigger_language,trigger_body,trigger_level FROM bucardo_custom_trigger
              WHERE goat=$tinfo->{id}
              AND status='active'
              AND trigger_type='triggerkick'
            };
            elog(DEBUG, "Running $SQL");
            $rv = spi_exec_query($SQL);
            if ($rv->{processed}) {
                my $custom_function_name = "bucardo_triggerkick_$tinfo->{id}";
                if (! exists $bfunctionoid{$custom_function_name}) {
                    my $custom_trigger_level = $rv->{rows}[0]{trigger_level};
                    $SQL = qq{
               CREATE OR REPLACE FUNCTION bucardo."$custom_function_name"()
               RETURNS TRIGGER 
               LANGUAGE $rv->{rows}[0]{trigger_language}
               AS \$notify\$
        };
                    $SQL .= qq{ $rv->{rows}[0]{trigger_body} };
                    $SQL .= qq{ \$notify\$; };
                }
            }

            ## Add in the autokick triggers as needed
            ## Skip if autokick is false
            if ($info->{autokick} eq 'f') {
                if (exists $btriggerinfo{$schema}{$table}{$kickfunc}) {
                    $SQL = qq{DROP TRIGGER "$kickfunc" ON $safeschema.$safetable};
                    ## This is important enough that we want to be verbose about it:
                    warn "Dropped trigger $kickfunc from table $safeschema.$safetable\n";
                    $run_sql->($SQL,$dbh);
                    delete $btriggerinfo{$schema}{$table}{$kickfunc};
                }
                next TABLE;
            }
            if (! exists $btriggerinfo{$schema}{$table}{$kickfunc}) {
                my $ttrig = $dbh->{pg_server_version} >= 80400 ? ' OR TRUNCATE' : '';
                my $custom_trigger_level = '';
                my $custom_function_name = '';
                if ($custom_trigger_level && $custom_function_name) {
                    $SQL = qq{
                    CREATE TRIGGER "$kickfunc" FIXMENAME
                    AFTER INSERT OR UPDATE OR DELETE$ttrig ON $safeschema.$safetable
                    FOR EACH $custom_trigger_level EXECUTE PROCEDURE bucardo."$custom_function_name"()
                    };
                }
                else {
                    $SQL = qq{
                    CREATE TRIGGER "$kickfunc"
                    AFTER INSERT OR UPDATE OR DELETE$ttrig ON $safeschema.$safetable
                    FOR EACH STATEMENT EXECUTE PROCEDURE bucardo."$kickfunc"()
                    };
                }
                try {
                    $run_sql->($SQL,$dbh);
                    $dbh->commit();
                } catch {
                    elog(WARNING,"bucardo_kick trigger on " . $schema . "." . $table . " could not be added.\n");
                    $dbh->rollback;
                }

            }
        } ## end each TABLE
    }     ## end each SCHEMA

    $dbh->commit();

}         ## end connecting to each database

## Gather information from bucardo_config
my $config;
$SQL = 'SELECT name,setting FROM bucardo_config';
$rv = spi_exec_query($SQL);
for my $row (@{$rv->{rows}}) {
    $config->{$row->{setting}} = $row->{value};
}


## Update the bucardo_delta_targets table as needed
## FIXME FROM old
#if ($info->{synctype} eq 'swap') {
    ## Add source to the target(s)
    ## MORE FIXME
#}

## Disconnect from all our databases
for (values %{$cache{dbh}}) {
    $_->disconnect();
}

## Let anyone listening know that we just finished the validation
$SQL = qq{NOTIFY "bucardo_validated_sync_$syncname"};
spi_exec_query($SQL);

elog(LOG, "Ending validate_sync for $syncname");

return 'MODIFY';

$bc$;
-- end of validate_sync


