"""This module contains logic for finding, dropping, and recreating large indexes.

A bulk data load is orders of magnitude faster if large indexes are created on
already populated tables, instead of data being loaded into indexed tables.
Finding, deleting, and recreating sufficiently large indexes can be tedious and
error-prone, though.  This module automates that process, with a tunable
threshold for what constitutes a large index.  The threshold is obtained from
the config module, which is populated by the end user of the script.

Classes exported:
Indexes: Identify large indexes, drop them on the secondary db, and recreate them.
"""
import re

import psycopg2
import quantities as pq
from plugins import Plugin
from psycopg2 import sql


class Indexes(Plugin):
    """Identify large indexes and temporarily drop them on the secondary while initial copy is in sync.

    This module was designed for use in tandem with a bucardo replication sync,
    but can be invoked as a standalone for any kind of bulk loading.

    Before the index management can begin, certain dependencies have to be
    created on the bucardo database.

    The first step is to find the large indexes on the primary.

    The second step is to store their definitions in the bucardo database.

    The third step is to drop the indexes on the secondary.

    Then the user has the option of monitoring a bucardo log to detect when the
    bulk copy is done and recreate indexes automatically at that point.

    Alternatively, the user can elect to run the recreate function when the
    indexes are ready to be created and to choose the option of immediate
    recreation.

    Methods exported:
    drop: find large indexes on the primary and drop them on the secondary
    install: install dependencies for the dropping and recreating of indexes
    recreate: query the list of dropped indexes and recreate them on the secondary
    """

    def __init__(self, cfg):
        """Create configuration settings that may not already be set.

        The user can either define the relevant namespaces and tables specifically for
        the indexes plugin, or the indexes plugin can draw on the settings in the
        bucardo section of the config.  If neither exists, the script will throw an
        error.

        The user can define a threshold for what constitutes a 'large' index.  If none
        is set, a default of all indexes on a table 10 GB or larger will be set.

        Keyword arguments:
        cfg: contents of the config file as a dictionary
        """
        super(Indexes, self).__init__(cfg)

        # Override or inherit certain params from the parent, depending on the config.
        self._set_inheritable_params("indexes")

        # Default to dropping indexes on tables larger than 10 GB.
        if "larger_than" not in cfg["indexes"]:
            cfg["indexes"]["larger_than"] = "10 GB"
        self.cfg = cfg

    def _check_num_indexes(self, schemaname, tablename):
        """Return the number of index definitions backed up for a table.

        This method queries the bucardo database to see how many index
        definitions exist for this table.
        """
        query = sql.SQL(
            """SELECT COUNT(*)
            FROM manage_indexes.index_definitions id
            WHERE id.repl_name = %s
                AND id.schemaname = %s
                AND id.tablename = %s
            """
        )
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query, [self.repl_name, schemaname, tablename])
                num_indexes = cur.fetchone()[0]
        finally:
            conn.close()
        return num_indexes

    def _convert_units(self, larger_than):
        """Accept a human readable string like '8 MB' and return the number of bytes."""

        # Assume a larger_than value consists of digits followed by an optional space followed by letters.
        decomposed_larger_than = re.split("([a-zA-Z]+)", larger_than.replace(" ", ""))
        larger_than_magnitude = int(decomposed_larger_than[0])
        larger_than_unit = decomposed_larger_than[1]
        # Use the quantities module (pq) for conversion.
        converted_unit = larger_than_magnitude * getattr(pq, larger_than_unit)
        converted_unit.units = "byte"
        # The quantities module produces output in the format of '10.0 B'.  We just want the 10.
        converted_unit = re.sub(r"\..*", "", str(converted_unit))
        return converted_unit

    def _execute_ddl(self, recreate_or_drop):
        """Recreate the dropped indexes on the secondary."""

        # Retrieve the DDL from bucardo.
        if recreate_or_drop == "recreate":
            query = sql.SQL("SELECT id.create_ddl FROM manage_indexes.index_definitions id WHERE id.repl_name = %s")
            success_message = "Indexes and uniqueness constraints recreated."
        elif recreate_or_drop == "drop":
            query = sql.SQL("SELECT id.drop_ddl FROM manage_indexes.index_definitions id WHERE id.repl_name = %s")
            success_message = "Indexes and uniqueness constraints dropped."

        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query, [self.repl_name])
                index_definitions = cur.fetchall()
        except Exception:
            conn.close()
            raise
        conn.close()

        # Execute the retrieved DDL on the secondary to drop or recreate the indexes.
        conn = psycopg2.connect(self.secondary_schema_owner_conn_pg_format)
        try:
            with conn.cursor() as cur:
                for index in index_definitions:
                    cur.execute(index[0])
                    conn.commit()
        except Exception:
            conn.close()
            raise
        conn.close()

        print(success_message)

    def _get_constraint_defs(self, table):
        """Given a table, return a list of constraint definitions for uniqueness constraints on that table.

        The table must be larger in size than the threshold for a large table,
        which is defined in `__init__`.
        replication.
        """

        query = sql.SQL(
            """SELECT pn.nspname, prel.relname, pc.conname
                    , 'ALTER TABLE ' || pn.nspname || '.' || prel.relname ||
                      ' ADD CONSTRAINT ' || pc.conname || ' ' || pg_get_constraintdef(pc.OID) AS create_ddl
                    , 'ALTER TABLE ' || pn.nspname || '.' || prel.relname ||
                      ' DROP CONSTRAINT ' || pc.conname AS drop_ddl
                FROM pg_constraint pc
                    JOIN pg_namespace pn ON pn.OID = pc.connamespace
                    JOIN pg_class prel ON prel.OID = pc.conrelid
                WHERE pn.nspname = %s
                    AND prel.relname = %s
                    AND pc.contype = 'u'
                    AND pg_relation_size(pn.nspname || '.' || prel.relname) > %s
            """
        )

        # Convert the threshold to bytes with no unit specified, for passing to Postgres.
        larger_than = self._convert_units(self.cfg["indexes"]["larger_than"])

        conn = psycopg2.connect(self.primary_conn_pg_format)
        constraint_definitions = []
        try:
            with conn.cursor() as cur:
                cur.execute(query, [table[0], table[1], larger_than])
                constraint_definitions = cur.fetchall()
        finally:
            conn.close()
        return constraint_definitions

    def _get_index_constraint_defs(self, table, index_or_constraint):
        """Given a table, return a list of definitions for indexes and uniquness constraint on that table.

        The table must be larger in size than the threshold for a large table, which is
        defined in `__init__`.  The index must not be associated with a primary key,
        because bucardo requires all tables have primary keys in order to perform its
        replication.
        """

        if index_or_constraint == "index":
            query = sql.SQL(
                """SELECT pi.schemaname, pi.tablename, pi.indexname, pi.indexdef AS create_ddl
                    , 'DROP INDEX ' || pi.schemaname || '.' || pi.indexname AS drop_ddl
                FROM pg_indexes pi
                WHERE pi.schemaname = %s AND pi.tablename = %s
                AND NOT EXISTS (
                    SELECT pc.conname
                    FROM pg_constraint pc
                    WHERE pc.conrelid = (pi.schemaname || '.' || pi.tablename)::regclass::oid
                        AND pc.conname = pi.indexname
                        AND pc.contype IN ('p','u')
                )
                AND pg_relation_size(pi.schemaname || '.' || pi.tablename) > %s
                """
            )
        elif index_or_constraint == "constraint":
            query = sql.SQL(
                """SELECT pn.nspname, prel.relname, pc.conname
                        , 'ALTER TABLE ' || pn.nspname || '.' || prel.relname ||
                          ' ADD CONSTRAINT ' || pc.conname || ' ' || pg_get_constraintdef(pc.OID) AS create_ddl
                        , 'ALTER TABLE ' || pn.nspname || '.' || prel.relname ||
                          ' DROP CONSTRAINT ' || pc.conname AS drop_ddl
                    FROM pg_constraint pc
                        JOIN pg_namespace pn ON pn.OID = pc.connamespace
                        JOIN pg_class prel ON prel.OID = pc.conrelid
                    WHERE pn.nspname = %s
                        AND prel.relname = %s
                        AND pc.contype = 'u'
                        AND pg_relation_size(pn.nspname || '.' || prel.relname) > %s
                """
            )

        # Convert the threshold to bytes with no unit specified, for passing to Postgres.
        larger_than = self._convert_units(self.cfg["indexes"]["larger_than"])

        conn = psycopg2.connect(self.primary_conn_pg_format)
        index_definitions = []
        try:
            with conn.cursor() as cur:
                cur.execute(query, [table[0], table[1], larger_than])
                index_definitions = cur.fetchall()
        finally:
            conn.close()
        return index_definitions

    def _store_index_defs(self, index_definitions):
        """Given a list of DDL statements, store them in the bucardo database.

        This allows the user to run the DDL to recreate the indexes on the standby
        when they're needed again.
        """
        # Write definitions to the bucardo database.
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        for index in index_definitions:
            query = sql.SQL(
                """INSERT INTO manage_indexes.index_definitions (
                    schemaname
                    , tablename
                    , indexname
                    , create_ddl
                    , drop_ddl
                    , repl_name
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (schemaname, indexname, repl_name) DO NOTHING
                """
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query, [index[0], index[1], index[2], index[3], index[4], self.repl_name])
                    conn.commit()
            except Exception:
                conn.close()
                raise
        conn.close()

    def drop(self):
        """Drop large indexes on the secondary database.

        First, this method finds the tables being replicated, by referring to the
        config for schemas and tables.

        Then it finds the indexes on each table that are larger than the threshold set
        in the config.

        Then it loads the DDL for the indexes into the dedicated table in the bucardo
        database.  If the total number of index definitions stored in the bucardo
        database for each table is not equal to the number of indexes found for that
        table, the script will abort without dropping indexes.

        If everything checks out, the script executes the drops.

        It then repeats this logic for uniqueness constraints, which have unique
        indexes under the hood.
        """

        # Find all the tables being replicated.  'r' is for "relation".
        tables = self._find_objects("r", self.repl_objects)
        safe_to_drop = False
        if tables:
            for table in tables:
                # See if there's anything to drop for this table.
                index_definitions = self._get_index_constraint_defs(table, "index")
                constraint_definitions = self._get_index_constraint_defs(table, "constraint")
                all_indexes = index_definitions + constraint_definitions

                if all_indexes:
                    self._store_index_defs(all_indexes)

                    # Check that we backed up the same number of indexes and constraints as we plan to drop.
                    # Abort if not.
                    expected_indexes_count = len(all_indexes)

                    stored_indexes_count = self._check_num_indexes(table[0], table[1])
                    if expected_indexes_count == stored_indexes_count:
                        print(f"DDL for {table[0]}.{table[1]} backed up in the bucardo database.")
                        safe_to_drop = True
                    else:
                        raise Exception(
                            f"Tried to store {expected_indexes_count} index(es) for {table[0]}.{table[1]} "
                            "in manage_indexes.index_definitions on the bucardo database, "
                            f"but {stored_indexes_count} index definition(s) were stored instead. "
                            "Aborting without dropping indexes. Please investigate."
                        )
                else:
                    print(f"No large indexes or uniqueness constraints found on {table[0]}.{table[1]}.")

        else:
            print("No tables found.")
        # Drop the indexes and constraints.
        if safe_to_drop:
            print("\nDropping large indexes and constraints...")
            self._execute_ddl("drop")

    def install(self):
        """Install the dependencies for index management on the bucardo database.

        Creates a schema and a table in the bucardo database by loading a file
        that contains the DDL.
        """
        print("Installing dependencies.")
        with open("plugins/indexes/custom_manage_indexes.sql", "r") as file:
            sql = file.read()
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
        finally:
            conn.close()
        print("Dependencies installed. You may now drop indexes and recreate them later.")

    def recreate(self):
        """Recreate dropped indexes on the secondary database.

        If the bulk copy isn't complete and it's being loaded via bucardo, the
        user should run bucardo.wait_for_copy first.

        The definitions for the indexes to be recreated are fetched from the
        `manage_indexes` schema on the bucardo database.
        """
        self._execute_ddl("recreate")

    def _validate_install(self):
        print("Check: The manage_indexes.indexes_definitions table is present...", end="")
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM manage_indexes.index_definitions")
                conn.commit()
        except Exception:
            print("Fail.")
            print("ERROR: Unable to find the manage_indexes.index_definitions table in the bucardo metadata database.")
            raise Exception()
        else:
            print("Pass.")
        finally:
            conn.close()

    def _validate_drop(self):
        print("Check: Looking for indexes to drop...", end="")
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM manage_indexes.index_definitions LIMIT 1")
                indexes = cur.fetchone()
        except Exception:
            print("Fail.")
            print("ERROR: Unable to query the manage_indexes.index_definitions table in the bucardo metadata database.")
            raise Exception()
        finally:
            conn.close()

        if cur.rowcount:
            # We found indexes to drop.
            print("Pass")
            print("Check: Spot checking an index to see if it was dropped...", end="")
            conn = psycopg2.connect(self.secondary_schema_owner_conn_pg_format)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM pg_indexes WHERE schemaname = %s AND tablename = %s AND indexname = %s",
                        [indexes[0], indexes[2], indexes[1]],
                    )
                    rowcount = cur.rowcount
            except Exception as e:
                print("Fail")
                print(f"ERROR: {e})")
                raise Exception()
            finally:
                conn.close()
            if rowcount:
                print("Fail.")
                print(f"ERROR: unexpected index {indexes[1]} found on {indexes[0]}.{indexes[2]}.")
                raise Exception()
            else:
                print("Pass.")
        else:
            print("None.")
            print("No indexes found to drop. Treating this as a pass.")

    def _validate_recreate(self):
        print("Check: Looking for indexes to recreate...", end="")
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM manage_indexes.index_definitions LIMIT 1")
                indexes = cur.fetchone()
        except Exception:
            print("Fail.")
            print("ERROR: Unable to query the manage_indexes.index_definitions table in the bucardo metadata database.")
            raise Exception()
        finally:
            conn.close()

        if cur.rowcount:
            # We found indexes to drop.
            print("Pass")
            print("Check: Spot checking an index to see if it was recreated...", end="")
            conn = psycopg2.connect(self.secondary_schema_owner_conn_pg_format)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM pg_indexes WHERE schemaname = %s AND tablename = %s AND indexname = %s",
                        [indexes[0], indexes[2], indexes[1]],
                    )
                    rowcount = cur.rowcount
            except Exception as e:
                print("Fail")
                print(f"ERROR: {e})")
                raise Exception()
            finally:
                conn.close()
            if rowcount:
                print("Pass.")
            else:
                print("Fail.")
                print(f"ERROR: index {indexes[1]} missing from {indexes[0]}.{indexes[2]}.")
                raise Exception()
        else:
            print("None.")
            print("No indexes found to recreate. Treating this as a pass.")
