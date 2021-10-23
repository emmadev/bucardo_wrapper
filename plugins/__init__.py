"""This module contains the base class for a plugin in the bucardo wrapper script package.

Each plugin should be created as a subclass of the Plugin class.

Classes exported:
Plugin: Basic bucardo and postgres functionality for plugins.
"""

import inspect

import psycopg2
import psycopg2.extras
from psycopg2 import sql


class Plugin():
    """Parent class for individual bucardo plugins.

    Each plugin should be created as a subclass of the Plugin class.

    Example:
    `class Bucardo(Plugin):`

    Methods exported:
    _connect: Return a connection string suitable for psycopg2 or bucardo use.
    _find_objects: Query Postgres for relevant tables/indexes/etc.
    _menu_options: Discover methods that should be exposed to the user.
    _set_inheritable_params: Allow child plugins to use or override certain parent objects.

    Despite the leading underscore, these methods are used by the wrapper script
    and/or intended for use by plugin child classes.  The leading underscore signals
    that the method should not be exposed to the end user in the menu.
    """

    def __init__(self, cfg):
        """Set psycopg2 connection strings used by multiple plugins.

        Keyword arguments:
        cfg: contents of the config file as a dictionary

        Variables exported:
        bucardo: config data for bucardo database connection
        bucardo_conn_pg_format: bucardo db connection string
        bucardo_fallback_conn_pg_format: bucardo fallback db connection string
        cfg: contents of the config file as a dictionary
        primary: config data for primary database connection
        primary_conn_pg_format: primary db connection string
        primary_schema_owner_conn_pg_format: primary db connection string
        secondary: config data for secondary database connection
        secondary_db_owner_conn_pg_format: secondary db connection string
        secondary_schema_owner_conn_pg_format: secondary db connection string
        """
        # Default dbname value, the one that the open source Bucardo project requires.
        # This can only be overridden by using the concurrency plugin.
        if (cfg.get('concurrency')
                and 'bucardo_dbname' in cfg['concurrency']
                and 'concurrency' in cfg['plugins']):
            # Allow the user to set a custom bucardo database, but only if they've
            # invoked the concurrency plugin.
            cfg['databases']['bucardo']['dbname'] = cfg['concurrency']['bucardo_dbname']
        else:
            # Custom bucardo databases won't work out of the box, because
            # bucardo doesn't support them, so hard-code to 'bucardo'.
            cfg['databases']['bucardo']['dbname'] = 'bucardo'

        # Convert parts of the config data to variables with simpler names.
        self.bucardo = cfg['databases']['bucardo']
        self.primary = cfg['databases']['primary']
        self.secondary = cfg['databases']['secondary']
        self.repl_name = cfg['bucardo']['replication_name']

        # This is a psycopg2 connection string the regular bucardo database.  The
        # regular bucardo db is used for most purposes.
        self.bucardo_conn_pg_format = self._connect(self.bucardo, user=self.bucardo['database_owner'])

        # This is the conn string for the fallback database, used when the regular
        # bucardo database doesn't exist yet, or we need to drop it.
        self.bucardo_fallback_conn_pg_format = self._connect(
            self.bucardo,
            dbname=self.bucardo['fallback_db'],
            user=self.bucardo['database_owner'],
        )
        self.primary_conn_pg_format = self._connect(self.primary, user=self.primary['database_owner'])
        # This is a psycopg2 conn string for the primary database, used when we need to
        # perform DDL on replicated tables, there isn't a superuser because it's RDS,
        # and the "superuser" doesn't have the necessary permissions on the relevant
        # tables.
        self.primary_schema_owner_conn_pg_format = \
            self._connect(self.primary, user=self.primary['schema_owner'])
        # This is a psycogp2 conn string for the secondary database, used when we need
        # "superuser" privs on an RDS database.
        self.secondary_db_owner_conn_pg_format = self._connect(self.secondary, user=self.secondary['database_owner'])

        # This is a psycopg2 conn string for the secondary, used when we need to perform
        # DDL on replicated tables, there isn't a superuser because it's RDS, and the
        # "superuser" doesn't have the necessary permissions on the relevant tables.
        self.secondary_schema_owner_conn_pg_format = \
            self._connect(self.secondary, user=self.secondary['schema_owner'])

        # Allow users to ctrl-c out of a psycopg2 query.
        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

        self.cfg = cfg

    def _compute_where_clause(self, replication_objects):
        """Accept a dictionary containing up to four lists and return a WHERE clause and dictionary.

        The config file allows the user to tune which namespaces and tables they do and
        do not wish to replicate.  This results in up to four lists being populated.  The
        `_compute_where_clause()` method turns these into a string formatted for the
        psycogp2 `execute()` method.  Since anywhere from 0 to 4 of the lists may be
        populated, we need anywhere from 0-4 `AND` sub-clauses in the WHERE clause.
        Each of them contains a placeholder that correlates to the name of the key in
        the dictionary.  The value in the dictionary is a tuple of namepsaces or tables
        that will need escaping in psycopg2.  The `_compute_where_clause()` method
        returns the string containing the constructed WHERE clause, and the dictionary.
        psycopg2 will accept the dictionary in the `execute()` method and do the
        escaping safely.

        Keyword arguments:
        replication_objects -- a dictionary containing the replication_objects from the config file
        """
        replication_object_lists = {}
        where_clause = ''

        if replication_objects['namespace_include']:
            where_clause = f'{where_clause}\n\t\tAND pn.nspname IN %(namespace_include)s'
            replication_object_lists['namespace_include'] = tuple(replication_objects['namespace_include'])

        if replication_objects['namespace_exclude']:
            where_clause = f'{where_clause}\n\t\tAND pn.nspname NOT IN %(namespace_exclude)s'
            replication_object_lists['namespace_exclude'] = tuple(replication_objects['namespace_exclude'])

        if replication_objects['table_include']:
            where_clause = f'{where_clause}\n\t\tAND pc.relname IN %(table_include)s'
            replication_object_lists['table_include'] = tuple(replication_objects['table_include'])

        if replication_objects['table_exclude']:
            where_clause = f'{where_clause}\n\t\tAND pc.relname NOT IN %(table_exclude)s'
            replication_object_lists['table_exclude'] = tuple(replication_objects['table_exclude'])

        return where_clause, replication_object_lists

    def _connect(self, conn, prefix='', include_dashes=False, **kwargs):
        """Return a connection string suitable for psycopg2 or bucardo use.

        psycopg2 format: dbname=my_db user=my_user...
        bucardo command-line format: --dbname=my_db --dbuser=my_user
        bucardo database storage format: dbname=my_db dbuser=my_user

        Keyword arguments:
        conn -- a dictionary containing user, dbname, host, and port
        prefix -- an optional prefix for the keys, such as 'db' to turn
            'user' into 'dbuser' (default '')
        include_dashes -- whether to prefix each key with two dashes in the
            connection string (default False)
        kwargs -- key-value pairs that override individual pairs in the conn dictionary

        """
        user = conn['user'] if kwargs.get('user') is None else kwargs.get('user')
        dbname = conn['dbname'] if kwargs.get('dbname') is None else kwargs.get('dbname')
        host = conn['host'] if kwargs.get('host') is None else kwargs.get('host')
        port = conn['port'] if kwargs.get('port') is None else kwargs.get('port')
        dashes = '--' if include_dashes else ''
        return (
            f'{dashes}dbname={dbname} {dashes}{prefix}user={user} '
            f'{dashes}{prefix}host={host} {dashes}{prefix}port={port}'
        )

    def _find_objects(self, datatype, namespaces_tables):
        """Query for selected objects in the database and return a list of tuples.

        There are various places in the code where we need to fetch the tables,
        sequences, indexes, materialized views, etc. that the user is interested in
        performing operations on.  The `_find_objects()` method accepts a string
        telling it what kind of objects to return (tables, etc.), and a dictionary
        telling it what namespaces or tables to query to narrow down which objects in
        the database to return.

        Despite the leading underscore, this method is intended for use by plugin
        developers.  The leading underscore signals that it should not be exposed to the
        end user.

        Keyword arguments:
        datatype: a single character corresponding to a value in pg_class.relkind
        namespaces_tables: a dictionary of lists for filtering on namespaces and/or tables
        """

        where_clause, replication_object_lists = self._compute_where_clause(namespaces_tables)
        query = sql.SQL(
            f"""SELECT * FROM (
                SELECT pn.nspname, pc.relname AS fully_qual_name
                FROM pg_class pc
                    JOIN pg_namespace pn ON pn.OID = pc.relnamespace
                    LEFT OUTER JOIN pg_inherits pi ON pi.inhparent = pc.OID
                WHERE pc.relkind = '{datatype}'
                    AND pi.inhrelid IS NULL
                    {where_clause}
                ) results
            WHERE fully_qual_name IS NOT NULL"""
        )
        conn = psycopg2.connect(self.primary_conn_pg_format)

        try:
            with conn.cursor() as cur:
                cur.execute(query, replication_object_lists)
                objects = cur.fetchall()
        finally:
            conn.close()
        return objects

    def _menu_options(self):
        """Return a list of methods that should be exposed to the end user.

        The `_menu_options()` method is intended to dynamically identify the methods
        for a plugin that should be displayed to the end user running the wrapper
        script.  The user is presented with a menu, and by entering the name of any of
        the methods, can cause it to execute.

        Unfortunately, I was unable to find a way to return only the methods of the
        child class and not of the parent Plugin class.  This means that to avoid
        possible clashes with methods in plugins that future developers might write, I
        chose to prefix all methods in the Plugin class with leading underscores, to
        keep them from being exposed to the end user, though some of them are intended
        for use by plugin developers.  I have tried to signal in the docstring which
        ones are meant for plugin developers to call directly.
        """
        return [
            method
            for method in dict(inspect.getmembers(self, inspect.isfunction))
            if not method.startswith(('_'))  # Filter out semiprivate methods.
        ]

    def _set_inheritable_params(self, plugin_class):
        """Allow plugins to inherit configuration settings from the bucardo plugin.

        If an inheritable configuration setting isn't found in the config section for
        this plugin, it will inherit from bucardo.  Otherwise, the values in the
        dedicated plugin configuration section will be used.  This is intended to allow
        flexibility of use cases while minimizing the amount of duplication in the
        config file.

        Despite the leading underscore, this method is intended for use by plugin
        developers.  The leading underscore signals that it should not be exposed to the
        end user.

        Keyword arguments:
        plugin_class -- the name of the plugin class that needs to inherit settings
        """
        overridable_params = ['replication_objects']
        for param in overridable_params:
            # If we haven't overridden the param, use the one in bucardo.
            if param not in self.cfg[plugin_class] and param in self.cfg['bucardo']:
                self.cfg[plugin_class][param] = self.cfg['bucardo'][param]
            # If we haven't overridden and the param doesn't exist in bucardo, throw an error.
            elif param not in self.cfg[plugin_class]:
                raise Exception(
                    f'In the config file, either "bucardo" or "{plugin_class}" must have a "{param}" value. '
                    'Please make sure that is set and try again.'
                )
        self.repl_objects = self.cfg[plugin_class]['replication_objects']
