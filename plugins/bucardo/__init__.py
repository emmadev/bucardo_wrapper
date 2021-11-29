"""This module contains the logic for replicating a database via bucardo.

Bucardo is a third-party, open-source Postgres replication tool written by
Greg Mullane.  You can find the source code at https://github.com/bucardo/bucardo.

Classes exported:
Bucardo: Basic bucardo replication functionality.
"""

import os
import time
from subprocess import Popen, DEVNULL

import psycopg2
from psycopg2 import sql

from plugins import Plugin


class Bucardo(Plugin):
    """Replication functionality built into the open source Bucardo tool.

    A child class of the Plugin class.

    This class contains all the functionality necessary to execute a basic database
    migration of tables and sequences from one Postgres database to another using
    bucardo, assuming the schema is already present on the secondary database.  It
    will not update materialized views; for that, use the mat_views plugin.  It is
    prone to causing outages when adding triggers.  To reduce that risk, use the
    retry plugin.

    Exported methods:
    add_triggers: add triggers to tables on primary database
    change_config: change bucardo config setting
    drop_triggers: drop triggers from primary database
    install: install bucardo metadata
    reload: reload bucardo daemon
    restart: restart bucardo daemon
    start: start bucardo daemon
    status: report status of bucardo daemon
    stop: stop bucardo daemon
    uninstall: uninstall bucardo metadata
    """

    def __init__(self, cfg):
        """Create connection strings for the bucardo tool.

        The bucardo tool accepts connection strings that are slightly different
        than those of psycopg2, so we create those here.

        Keyword arguments:
        cfg: contents of the config file as a dictionary

        Variables exported:
        bucardo_conn_bucardo_format: bucardo db connection string
        bucardo_opts: flags passed to the bucardo command line tool
        cfg: contents of the config file as a dictionary
        primary_conn_bucardo_format: primary db connection string
        secondary_conn_bucardo_format: secondary db connection string
        repl_name: arbitrary, user-selected name given to the replication task
        """
        super(Bucardo, self).__init__(cfg)

        # The bucardo command line util has its own format for database
        # connections, so those connection strings are formed here.

        # Regular bucardo database.
        self.bucardo_conn_bucardo_format = self._connect(
            self.bucardo,
            include_dashes=True,
            prefix='db',
            user=self.bucardo['database_owner'],
        )

        # Bucardo stores primary and secondary connection info in yet a third, slightly different format.
        self.primary_conn_bucardo_format = self._connect(
            self.primary,
            prefix='db',
            user=self.primary['database_owner'],
        )

        self.secondary_conn_bucardo_format = self._connect(
            self.secondary,
            prefix='db',
            user=self.secondary['database_owner'],
        )

        self.repl_name = cfg['bucardo']['replication_name']
        self.bucardo_opts = f'--no-bucardorc --logextension {self.repl_name}'
        self.cfg = cfg

        self.piddir = f'/var/run/bucardo/{self.repl_name}_piddir'
        self.autokick_pidfile = f'{self.piddir}/autokick_sync.pid'

    def _add_table_sequence_metadata(self):
        """Update bucardo metadata with list of tables and sequences to replicate.

        The `_add_table_sequence_metadata()` method consults the config file to see
        what tables and schemas the user wants to replicate, connects to the primary
        database and finds the tables and sequences that match the user criteria, and
        passes these tables and sequences to the bucardo command line tool.  The bucardo
        commad line tool connects to the bucardo database and populates the tables
        there with metadata about these tables and sequences.
        """
        print('Finding tables and sequences')
        # 'r' is for relation in pg_class.relkind.
        schema_tables = self._find_objects('r', self.cfg['bucardo']['replication_objects'])
        # 'S' is for sequence in pg_class.relkind.
        schema_sequences = self._find_objects('S', self.cfg['bucardo']['replication_objects'])

        # Reformat [(schema1, table1), (schema2, table2)] (psycopg2 output)
        # into 'schema1.table1 schema2.table2' (bucardo input).
        table_names = ['%s.%s' % name for name in schema_tables]
        sequence_names = ['%s.%s' % name for name in schema_sequences]

        tables = ' '.join(table_names)
        sequences = ' '.join(sequence_names)

        print('Storing metadata.')
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'add tables {tables} db=primary_db'
        )
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'add sequences {sequences} db=primary_db'
        )
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'add herd {self.repl_name} {tables} {sequences}'
        )

    def _async_kick_start(self):
        """Automatically kick replication sync every second.

        This means the database doesn't have to kick synchronously every time there's a commit.
        Asynchronous kicking can be less resource-intensive than synchronous and prevent outages.
        """
        # Run 'kick' in the background forever.
        pid = Popen(
            [
                'watch', '-n 1',
                'bucardo', f'{self.bucardo_opts} {self.bucardo_conn_bucardo_format} kick {self.repl_name}'
            ],
            stdout=DEVNULL
        ).pid

        # Manage the process using a pidfile.
        pid = str(pid)
        with open(self.autokick_pidfile, 'w') as pidfile:
            pidfile.write(pid)

        print(
            f'The bucardo sync is being kicked every second. See {self.autokick_pidfile}'
        )

    def _configure_bucardo(self):
        """Tell bucardo where to log and write pidfiles.

        The `_configure_bucardo()` function calls the bucardo command line tool, which
        connects to the bucardo database and updates the config with information about
        where to log and write pidfiles.  We use the `repl_name` variable in the file
        names so that the user can run multiple daemons on the same server.
        """
        # Tell bucardo where to log.
        logdir = '/var/log/bucardo/'
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set reason_file="{logdir}{self.repl_name}_reason_file"'
        )
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set log_conflict_file="{logdir}{self.repl_name}_log_conflict_file"'
        )
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set warning_file="{logdir}{self.repl_name}_warning_file"'
        )

        # Tell bucardo where to write pidfiles and stopfiles.
        os.system(f'mkdir -p /var/run/bucardo/{self.repl_name}_piddir')
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set piddir="/var/run/bucardo/{self.repl_name}_piddir"'
        )
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set stopfile="{self.repl_name}_stopfile"'
        )

    def _toggle_kick_triggers(self, enable_or_disable):
        """Enable or disable the kick triggers.

        The kick triggers are the ones that publish a NOTIFY event indicating
        that new data has just come in and is available to be replicated.

        Reasons for disabling:

        Firing a NOTIFY takes out a lock on an object needed by all occurrences of
        NOTIFY, even on different tables.  Too many concurrent writes can cause lock
        contention on that shared object.  This can cause a self-DDOS.

        If you disable automatic, synchronous kicking, you must run NOTIFY some other
        way, or replication will fall behind. See the `_async_kick_start` function for how this
        plugin does it.

        Reasons for enabling always:

        In order to enable cascading replication from database A to B to C, the B -> C
        replication stage needs to know that a change has just come into B.  Normally,
        changes trigger a notification to fire.  But on the B database, changes from
        A -> B treat B as a replica, which means all triggers are disabled.  We get
        around this by enabling the kick bucardo trigger to fire 'always', which means
        regardless of whether the database is an origin or a replica, on database B.
        """

        if enable_or_disable == 'disable':
            tgenabled = 'D'
        elif enable_or_disable == 'enable always':
            tgenabled = 'A'
        else:
            raise ValueError(
                f'Invalid enable_or_disable value {enable_or_disable}. '
                'Accepted values: "enable always", "disable".'
            )

        # Get the list of tables.
        # 'r' is for relation in pg_class.relkind.
        tables = self._find_objects('r', self.cfg['bucardo']['replication_objects'])

        # The bucardo trigger that will kick off a notification when
        # changes come in to the primary database
        # (which is B in an A -> B-> C topology).
        trigger = f'bucardo_kick_{self.repl_name}'
        trigger_needs_toggling = False
        conn = psycopg2.connect(self.primary_schema_owner_conn_pg_format)
        # Update the trigger on each table.
        for table in tables:
            # See if there's a trigger whose enabled value isn't the one we want.
            query = sql.SQL(
                """SELECT TRUE FROM pg_catalog.pg_trigger pt
                JOIN pg_catalog.pg_class pc ON pc.OID = pt.tgrelid
                    JOIN pg_catalog.pg_namespace pn ON pn.OID = pc.relnamespace
                WHERE pn.nspname = %s
                    AND pc.relname = %s
                    AND pt.tgname = %s
                    AND pt.tgenabled <> %s"""
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, [table[0], table[1], trigger, tgenabled])
                    trigger_needs_toggling = cur.fetchall()
            except Exception:
                conn.close()
                raise

            if trigger_needs_toggling:
                enabled = "DISABLE TRIGGER" if enable_or_disable == 'disable' else "ENABLE ALWAYS TRIGGER"
                query = sql.SQL('ALTER TABLE {schema}.{table} {enabled} {trigger}').format(
                    schema=sql.Identifier(table[0]),
                    table=sql.Identifier(table[1]),
                    enabled=sql.SQL(enabled),
                    trigger=sql.Identifier(trigger),
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                # If lock_timeout is blocking enabling, abort this attempt but continue gracefully to next table.
                except psycopg2.errors.LockNotAvailable:
                    print(f'Could not modify {trigger} on {table[0]}.{table[1]}')
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    conn.close()
                    raise
        conn.close()
        print('Kick triggers disabled.')

    def add_triggers(self):
        """Add triggers to tables on primary database."""
        print('Adding triggers. Warning: this may cause an outage.')
        # Default to never doing the initial data copy onto the secondary.
        one_time_copy = 0

        # Allow the user to override the copy setting.
        map_copy_values = {
            'never': 0,
            'always': 1,
            'empty': 2
        }

        if self.cfg['bucardo']['copy_data'] in map_copy_values:
            one_time_copy = map_copy_values[self.cfg['bucardo']['copy_data']]

        # Actually configure replication, including trigger adding.
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'add sync {self.repl_name} relgroup={self.repl_name} '
            f'dbs=primary_db:source,secondary_db:target onetimecopy={one_time_copy}'
        )
        # Need to tweak one of the triggers if we want to replicate changes
        # that have come in from an upstream primary.
        if self.cfg['databases']['primary'].get('cascade'):
            self._toggle_kick_triggers('enable always')
        # Need to disable one of the triggers if the user wants to reduce outage risk.
        if self.cfg['bucardo'].get('asynchronous_kicking'):
            self._toggle_kick_triggers('disable')
        print('Done adding triggers.')

    def change_config(self):
        """Change bucardo config setting. Prompts for user input."""
        # Prompt for input.
        self.setting_name = input('Name of the setting to change: ')
        self.new_value = input('New value for the setting: ')
        # Update the config using the bucardo daemon.
        os.system(
            f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
            f'set {self.setting_name}={self.new_value}'
        )
        print('You will need to reload or restart bucardo for the change to take effect.')

    def drop_triggers(self):
        """Drop triggers from primary database."""
        print('Dropping triggers. Warning: this may cause an outage. Ctrl-c to abort.')
        # Get the list of tables.
        # 'r' is for relation in pg_class.relkind.
        tables = self._find_objects('r', self.cfg['bucardo']['replication_objects'])

        # Bucardo puts three triggers on each table, with predictable names.
        triggers = ['bucardo_delta', f'bucardo_kick_{self.repl_name}', f'bucardo_note_trunc_{self.repl_name}']
        conn = psycopg2.connect(self.primary_schema_owner_conn_pg_format)
        # Drop each trigger from each table.
        for table in tables:
            for trigger in triggers:
                query = sql.SQL('DROP TRIGGER IF EXISTS {trigger} ON {schema}.{table}').format(
                    trigger=sql.Identifier(trigger),
                    schema=sql.Identifier(table[0]),
                    table=sql.Identifier(table[1]),
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                except psycopg2.errors.LockNotAvailable:
                    print(f'Could not drop {trigger} from {table[0]}.{table[1]}')
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    conn.close()
                    raise
        conn.close()

        print('Triggers dropped.')

    def install(self):
        """Install bucardo metadata."""
        print('Installing bucardo.')

        return_code = os.WEXITSTATUS(
            os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} install')
        )

        if return_code:
            print('Bucardo not installed.')
            return
        else:
            print('Configuring logging.')
            self._configure_bucardo()

            print('Storing database connection info.')
            os.system(
                f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
                f'add db primary_db {self.primary_conn_bucardo_format}'
            )

            # Makedelta is a bucardo flag that is one component of setting up cascading replication.
            makedelta = ''
            if self.cfg['databases']['secondary'].get('cascade'):
                makedelta = 'makedelta=1'
            os.system(
                f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} '
                f'add db secondary_db {self.secondary_conn_bucardo_format} {makedelta}'
            )

            print('Adding metadata about the tables and sequences that will be replicated.')
            self._add_table_sequence_metadata()

            print('Bucardo installed.')

    def reload(self):
        """Reload bucardo config."""
        os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} reload_config')

    def restart(self):
        """Restart bucardo daemon."""
        print('Restarting daemon.')
        os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} restart')
        if self.cfg['bucardo'].get('asynchronous_kicking'):
            self._async_kick_start()
        print('Daemon restarted.')

    def start(self):
        """Start bucardo daemon."""
        print('Starting daemon.')
        os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} start')
        if self.cfg['bucardo'].get('asynchronous_kicking'):
            self._async_kick_start()
        print('Daemon started.')

    def status(self):
        """Report status of bucardo daemon."""
        print('Checking status of bucardo.')
        os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} status')

    def stop(self):
        """Stop bucardo daemon."""
        print('Stopping daemon.')
        os.system(f'bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} stop')
        print('Daemon stopped.')

    def uninstall(self):
        """Uninstall bucardo metadata.

        If no bucardo database or schema is present, the uninstall method will
        gracefully perform no-op commands.
        """
        print('Uninstalling bucardo.')

        print('Dropping the bucardo database.')
        drop_db_cmd = sql.SQL('DROP DATABASE IF EXISTS {bucardo_db}').format(
            bucardo_db=sql.Identifier(self.bucardo['dbname'])
        )

        conn = psycopg2.connect(self.bucardo_fallback_conn_pg_format)
        # Explicit autocommit is necessary, because databases can't be dropped
        # inside a transaction block.
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(drop_db_cmd)
        finally:
            conn.close()

        print('Dropping the bucardo schema inside the primary database.')
        drop_schema_cmd = 'DROP SCHEMA IF EXISTS bucardo CASCADE'

        conn = psycopg2.connect(self.primary_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(drop_schema_cmd)
                conn.commit()
        finally:
            conn.close()

        if self.cfg['databases']['primary'].get('cascade'):
            print('\n\033[1mWarning:\033[0m You may have just broken replication from A to B in your A->B->C setup.')
            print('To get it working again, see docs/bucardo.md, Cascading Replication, Changing Topology.')
            time.sleep(3)

        print('Uninstalled bucardo.')
