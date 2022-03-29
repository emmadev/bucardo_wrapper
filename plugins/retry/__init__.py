"""This module contains retry logic for bucardo replication.

The adding and dropping of triggers in bucardo causes the potential for a
customer-facing outage due to lock contention.  The retry module allows bucardo
to attempt to get a lock, gracefully move on if it can't get one quickly
enough, and retry later.

Classes exported:
Retry: Add or remove bucardo triggers with graceful timeout and retry options.
"""
import psycopg2

from plugins import Plugin
from plugins.bucardo import Bucardo


class Retry(Plugin):
    """Add or remove bucardo triggers with graceful timeout and retry options.

    The user defines a timeout setting in the config.  If bucardo is unable to get a
    lock in that time, it will stop trying and move on.

    Methods exported:
    add_triggers: add as many triggers as possible within user-defined limits
    drop_triggers: drop as many triggers as possible within user-defined limits
    install: install dependencies for bucardo retry logic
    """

    def __init__(self, cfg):
        """Create aliases for configuration settings and instantiate Bucardo.

        In the interests of keeping lines shorter and more readable, some variables are
        assigned to config settings.

        A Bucardo class is instantiated so that its methods can be invoked as part of
        the methods of the Retry class.

        Keyword arguments:
        cfg: contents of the config file as a dictionary

        Variables exported:
        bucardo_instance: an instantiation of the Bucardo class
        cfg: a dictionary containing the config file
        primary_user: the owner of the primary database
        repl_name: an arbitrary name given by the user to the replication task
        timeout: number of milliseconds to wait for a lock before giving up
        """
        super(Retry, self).__init__(cfg)
        self.bucardo_instance = Bucardo(cfg)
        self.cfg = cfg
        self.primary_user = cfg['databases']['primary']['database_owner']
        self.repl_name = cfg['bucardo']['replication_name']
        self.timeout = cfg['retry']['timeout']

    def _check_for_syncs(self):
        """Check the bucardo database to see if a sync exists.  Return True or False."""
        query = 'SELECT * FROM bucardo.sync LIMIT 1'
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                syncs = cur.fetchone()
        finally:
            conn.close()
        if syncs:
            return True
        else:
            return False

    def _set_timeout(self, user, timeout_value):
        """Change the lock_timeout setting on the primary database.

        The lock_timeout setting will cause the query to fail if the lock cannot be
        obtained in the specified number of milliseconds.  By setting it for the user,
        we can ensure that when the bucardo command-line tool will have this setting
        when it connects.  This is only a problem for user traffic if the application
        connects as the database owner, which is already a serious security flaw, so
        this script counts on that not being the case.

        Keyword arguments:
        user -- the postgres user whose timeout value should be changed
        timeout_value -- the new timeout value (either DEFAULT or some number of milliseconds)
        """
        query = f'ALTER ROLE {user} SET lock_timeout = {timeout_value}'
        conn = psycopg2.connect(self.primary_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        finally:
            conn.close()

    def add_triggers(self):
        """Add replication triggers to tables on the primary database.

        The script updates the timeout value to whatever is in the config.

        Then it adds however many triggers it can without hitting the timeout limit,
        which applies on a per-trigger basis.

        Then it sets the timeout value back to the default.  If the user already had a
        non-default lock_timeout value, then this may cause the wrong value to be set.
        You are welcome to submit a PR in that case.
        """
        print('Adding triggers.')
        self._set_timeout(self.primary_user, self.timeout)

        # Which bucardo function to call depends on whether we just need to add
        # some triggers that timed out, or whether this is the first attempt at
        # adding them.
        sync_already_added = self._check_for_syncs()

        if sync_already_added:
            query = f"SELECT bucardo.validate_sync('{self.repl_name}', 0)"
            conn = psycopg2.connect(self.bucardo_conn_pg_format)
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    for notice in conn.notices:
                        print(notice, end='')
            finally:
                conn.close()

            # Enable and disable triggers as needed.
            self.bucardo_instance._manage_triggers()

        else:
            self.bucardo_instance.add_triggers()

        self._set_timeout(self.primary_user, 'DEFAULT')

        print(
            'Attempted to add triggers. Check the output above for warnings about missing triggers. '
            'If you see any, just run add_triggers again. If there are no warnings, you should be good to go.'
        )

    def drop_triggers(self):
        """Drop triggers on replicated tables on the primary database.
        The script updates the timeout value to whatever is in the config.

        Then it drops however many triggers it can without hitting the timeout limit,
        which applies on a per-trigger basis.

        Then it sets the timeout value back to the default.  If the user already had a
        non-default lock_timeout value, then this may cause the wrong value to be set.
        You are welcome to submit a PR in that case.
        """
        print('Dropping triggers.')
        self._set_timeout(self.primary_user, self.timeout)
        self.bucardo_instance.drop_triggers()
        self._set_timeout(self.primary_user, 'DEFAULT')
        print(
            'Attempted to drop triggers. Check the output above for warnings about missing triggers. '
            'If you see any, just run try_drop again. If there are no warnings, you should be good to go.'
        )

    def install(self):
        """Install the dependencies for retry logic on the bucardo database.

        Strictly speaking, this function only installs one of two dependencies.  It
        loads a slightly modified copy of a bucardo function provided by the Bucardo
        project to the bucardo database.

        It does not install the Perl Try::Tiny module, which is used by the forked
        version of the function and is not installed with bucardo.  You must install
        Try::Tiny yourself.
        """
        print('Installing dependencies.')
        # Read in the definition of a SQL stored procedure.
        custom_logic_file = 'plugins/retry/custom_validate_sync.sql'
        with open(custom_logic_file, 'r') as file:
            query = file.read()

        # Load the stored procedure into bucardo, replacing the one provided by the Bucardo project.
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        finally:
            conn.close()
        print(
            'Dependencies installed, except for the Perl Try::Tiny module. '
            'Make sure that module is installed before proceeding.'
        )

    def uninstall(self):
        """Uninstall the dependencies for retry logic on the bucardo database.

        Strictly speaking, this function only uninstalls one of two dependencies.  It
        loads the original copy of a bucardo function provided by the Bucardo project
        to the bucardo database.

        It does not remove the Perl Try::Tiny module, which is used by the forked
        version of the function and is not installed with bucardo.  You must uninstall
        Try::Tiny yourself.
        """
        print('Uninstalling dependencies.')
        # Read in the definition of a SQL stored procedure.
        custom_logic_file = 'plugins/retry/original_validate_sync.sql'
        with open(custom_logic_file, 'r') as file:
            query = file.read()

        # Load the stored procedure into bucardo, replacing the modified one.
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        finally:
            conn.close()
        print(
            'Dependencies removed, except for the Perl Try::Tiny module. '
        )
