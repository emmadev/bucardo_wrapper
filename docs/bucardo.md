# Overview

The bucardo plugin provides a wrapper around the bucardo command line tool that
allows you to set up replication without knowing bucardo syntax, and with
minimal concern for implementation details.

# Usage

Add `indexes` to the list of plugins in the config file.

Update the config file with connection info, desired tables/namespaces for
replication, a name for the replication task, and `copy_data`. 

The `copy_data` parameter tells bucardo whether to bulk load all existing data
from the primary to the secondary before beginning replication, or just to sync
new changes as they come in. Possible values are 'always', 'never', and
'empty'. 'Empty' will copy the data for a table only if that table is empty on
the secondary. The other two values should be self-explanatory.

Run `python wrapper.py`.

Run `bucardo.install` to create the bucardo database and other metadata.

Run `bucardo.add_triggers` to add triggers to the database.

Run `bucardo.start` to kick off replication (and possibly a data copy,
depending on your configuration).

Run `bucardo.stop` to stop replication. New writes will be logged to the
`bucardo` schema on the primary, but not replicated to the secondary.

Run `bucardo.drop_triggers` to drop triggers.

Run `bucardo.uninstall` to drop the bucardo schema and bucardo database.

# Cascading Replication

Cascading replication is when you want changes to replicate from database A to B to C.

In order to accomplish that with this tool, you'll need to enable the
concurrency plugin. At a high level, you'll set up two completely independent
replication jobs, A -> B and B -> C. The steps are outlined in detail below.

Note that these are the simple steps. If you want to use the retry plugin, you
can replace `bucardo.add_triggers` with `retry.install` plus
`retry.add_triggers` as per the usual retry practice. This is strongly
recommended, as cascading adds an extra step that has to take out an exclusive
lock on each table.

## Config

Create two config files, one for A -> B and one for B -> C. Make sure `cascade`
is set to `True` for database B. This means:

- In the A -> B config, set `cascade` to `True` for the secondary database.

- In the B -> C config, set `cascade` to `True` for the primary database.

As always with the concurrency plugin, make sure the configs have unique
replication names and bucardo database names.

## Replication Setup

You will need to set up replication for both stages before you start replication on either.

### A -> B

For the A -> B stage, do the following:

1. `concurrency.install`
2. `bucardo.install`
3. `bucardo.add_triggers`

### B -> C

For the B -> C stage, do the following:

1. `concurrency.install`
2. `bucardo.install`
3. `bucardo.add_triggers`

## Kick Off Replication

Note that it doesn't make sense to kick off the replication for B -> C until
the replication for A -> B is complete, as you will then be replicating an
empty B database.

Likewise, if A -> B isn't complete, then searching for large indexes on B -> C
won't find anything.

Both `bucardo.start` and `indexes.drop` thus need to wait until A -> B is complete.

Since Bucardo doesn't support cascading replication, the current setup is
basically two completely separate replication jobs, from A -> B and B -> C,
with the bare minimum of hacks to make sure new changes on A are propagated to C.

### A -> B

For the A -> B stage, do the following:

1. `bucardo.start`

### B -> C

Once the A -> B data copy is complete, for the B -> C stage, do the following:

1. `bucardo.start`

You should now have cascading replication. Once data copying is complete,
double check that changes on A are appearing on C.

## Changing Topology

If you have an A -> B -> C setup, and you run `bucardo.uninstall` from A to B,
B -> C replication will continue to function as expected.

But if you run `bucardo.uninstall` from B to C, A -> B will stop working. This is
because A -> B is configured to propagate its changes to C, only there is no C,
and it doesn't want C to fall behind, so it stops replicating and throws errors
that you can see in the logs.

If you want A -> B replication to continue working, there are a couple of
manual steps you must follow.

1. Connect to the bucardo database for A -> B. This is the one specified in
`databases: bucardo:` in the YAML config file for A -> B replication.

2. Run

```
UPDATE db SET makedelta = 'f' WHERE name = 'secondary_db';
```

3. Then run the bucardo wrapper script again, using the A -> B config:

```
python wrapper.py
```

4. Using the wrapper, restart bucardo:

```
bucardo.restart
```

Check the logs. After the restart, you should now have a working A -> B
replication cluster.

Changing `makedelta` tells bucardo not to pass the delta, i.e., the new changes
to be replicated, on to a downstream database beyond the immediate replication
setup.

# Functions

## Replication Setup

## install

The `install` function in the bucardo plugin creates a database named
`bucardo_database` in the `bucardo_host` cluster, and a schema named bucardo on
the primary database. It will populate the database and schema with tables and
basic data needed to manage replication.

If you don't know of any reason to make changes here, select 'P' to proceed. If
`bucardo_database` doesn't already exist on the specified host, the script
will switch to the `postgres` database. Enter 'P' again to continue.

## uninstall

The `uninstall` function in the bucardo plugin drops the bucardo database on
the bucardo host, as well as the `bucardo` schema on the primary database. It
does not have any outage potential, unless you haven't dropped your triggers
yet. (It is advisable to drop triggers separately in production.)

The Bucardo package and all of the config files, etc., will remain installed on
the server. This function only uninstalls Bucardo from the databases.

### add\_triggers

The `add_triggers` function in the bucardo plugin does the following:

1. Adds the tables and sequences you've chosen to replicate to the replication
management metadata.
2. Puts triggers on the tables. The presence of triggers causes copies of every
write to the tables to be logged in the `bucardo` schema on the primary
database.

This step does not begin transferring any data to the standby.

You can configure which tables and sequences are replicated in
`replication_objects` in the `bucardo` section of the config file.  Using the
`include` and `exclude` lists, you can blacklist or whitelist schemas (also
called namespaces) or tables, and if you've whitelisted a schema, you can
blacklist tables within it and vice versa. If these options are all left blank,
all tables and sequences in all schemas will be replicated.

Because adding triggers to a table requires taking out an exclusive lock on
that table, this stage can conflict with user traffic. You can control-c to
return to the prompt, but the add trigger query in Postgres will still be
running and blocking traffic. You will need to find that and terminate it
manually. If this is a concern, see the retry plugin.

## drop\_triggers

The `drop_triggers` function in the bucardo plugin works a lot like
`add_triggers`, so see that function for more details and warnings.

Dropping and adding triggers need to take out the same lock on the tables that
have triggers, and so the `drop_triggers` has the same potential as
`add_triggers` to cause problems for users. However, because Bucardo, unlike
Slony, puts triggers only on the primary when setting up replication, if you're
dropping the triggers after a successful migration, there should be no
conflicting traffic on the tables and dropping should be easy.

If, on the other hand, you need to drop triggers from a database that has
production traffic and are concerned about lock contention, see the retry
plugin.

## Daemon Management

### change\_config
The `change_config` function in the bucardo plugin changes a setting in the
bucardo config. The user will be prompted to enter the name of the setting they
wish to change, such as 'log\_level', and the new value for the setting, such
as 'verbose'. No input validation is performed by the wrapper. The name and new
value are passed to bucardo, which attempts to make the change. If the value or
setting are not valid, bucardo will throw an error.

The change will not take effect until the config is reloaded, or, for some
settings, until the daemon is restarted. See the `reload` and `restart` functions.

### reload
The `reload` function in the bucardo plugin reloads the bucardo config. The
most common time to run this is after `change_config`.

### restart

The `restart` function in the bucardo plugin stops the daemon and restarts it.
Replication should resume smoothly after the restart. This is what you want to
do if you change the schema of a table being replicated. A schema change will
stall replication until the daemon is restarted, which allows the config and
schema to be reloaded.

### start

The `start` function in the bucardo plugin starts up the Bucardo daemon. If
Bucardo has been configured to do an initial data copy from the primary to the
standby, that will begin now. The logs that you're most likely to be interested
in at this stage will live at `/var/log/bucardo/log.bucardo.$replication_name`,
where `replication_name` is taken from the config file.

### status

The `status` function in the bucardo plugin checks the status of replication.
Note that a status of 'Good' does not mean replication is caught up. It only
means that the latest batch was replicated successfully. There may be many
batches still waiting in the queue.

## stop

The `stop` function in the bucardo plugin stops the Bucardo daemon. This means
replication is no longer copying data from the primary to the standby. However,
because Bucardo is trigger-based replication, a record of every write to the
database is still being made in the `bucardo` schema on the primary database.
Be aware of this, as this can affect your i/o, disk space, etc. on the
database. In order to remove these triggers, you will need to run
`drop_triggers`.

# Reducing outage risks

If you have high-volume write traffic consisting of many parallel COMMITs,
bucardo can DDOS your database. In this scenario, the number of rows per commit
doesn't matter, it's the number of simultaneous commits that cause the problem.

The problem is that each time a commit happens, the kick trigger fires. The
kick trigger issues a NOTIFY. In Postgres, NOTIFY takes out a lock of this
type: "AccessExclusiveLock on object 0 of class 1262 of database 0". There is,
as far as I can tell, only one such object. So too much concurrent activity all
trying to get that same lock can cause a bottleneck.

To avoid that, you can set `disable_kicking` in the `bucardo:primary` section
of the config. What that will do is disable the kick trigger during the
`add_triggers` action, and spawn a background process during `start` and
`restart` to do the kicking instead. Kicking will be done once per second,
using `watch`. This should be sufficient if you receive traffic consistently
throughout the day, or if you don't mind bucardo checking for changes and not
always finding them. (There is a small performance overhead to any needless
activity.)

The pid will be printed when `start` or `restart` is run. This pid isn't stored
by the script and isn't managed by it. A new process is spawned each time you
run `restart`. It's up to you to kill it.

TODO: Write the pid to a pidfile and manage the process.

# Logs and pidfiles

Bucardo logs are written to `/var/log/bucardo`. This directory must exist and
the user running this script must have permissions to write to it. Individual
logs will incorporate the `replication_name` parameter from the config file
into their name. This contributes to the ability to run multiple Bucardo jobs
simultaneously on the same server.

The most interesting log is `log.bucardo.[whatever your replication name is]`.
That will log almost everything that has to do with replication. The reason
file logs just indicate the stopping and starting of the daemon (but that
information is also logged in the regular log file).

Bucardo pidfiles are written to `/var/run/bucardo`. This directory must exist
and the user running this script must have permissions to write to it.
Individual files will incorporate the `replication_name` parameter from the
config file into their name. This contributes to the ability to run multiple
Bucardo jobs simultaneously on the same server.

# Dependencies

- bucardo 5.6.0
- /var/run/bucardo (writable by the user running this script)
- /var/log/bucardo (writable by the user running this script)
- A server with OS access (not RDS/Aurora) for the bucardo database
- A Postgres instance (on the bucardo host)
- postgresql-plperl (on the bucardo host)
