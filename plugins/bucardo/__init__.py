"""This module contains the logic for replicating a database via bucardo.

Bucardo is a third-party, open-source Postgres replication tool written by
Greg Mullane.  You can find the source code at https://github.com/bucardo/bucardo.

Classes exported:
Bucardo: Basic bucardo replication functionality.
"""

import os
import signal
import time
from subprocess import DEVNULL, Popen

import psycopg2
from plugins import Plugin
from psycopg2 import sql


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
            prefix="db",
            user=self.bucardo["database_owner"],
        )

        # Bucardo stores primary and secondary connection info in yet a third, slightly different format.
        self.primary_conn_bucardo_format = self._connect(
            self.primary,
            prefix="db",
            user=self.primary["database_owner"],
        )

        self.secondary_conn_bucardo_format = self._connect(
            self.secondary,
            prefix="db",
            user=self.secondary["database_owner"],
        )

        self.repl_name = cfg["bucardo"]["replication_name"]
        self.bucardo_opts = f"--no-bucardorc --logextension {self.repl_name}"
        self.cfg = cfg

        self.piddir = f"/var/run/bucardo/{self.repl_name}_piddir"
        self.autokick_pidfile = f"{self.piddir}/autokick_sync.pid"

    def _add_table_sequence_metadata(self):
        """Update bucardo metadata with list of tables and sequences to replicate.

        The `_add_table_sequence_metadata()` method consults the config file to see
        what tables and schemas the user wants to replicate, connects to the primary
        database and finds the tables and sequences that match the user criteria, and
        passes these tables and sequences to the bucardo command line tool.  The bucardo
        commad line tool connects to the bucardo database and populates the tables
        there with metadata about these tables and sequences.
        """
        print("Finding tables and sequences")
        # 'r' is for relation in pg_class.relkind.
        schema_tables = self._find_objects("r", self.cfg["bucardo"]["replication_objects"])
        # 'p' is for partition in pg_class.relkind.
        schema_tables = schema_tables + self._find_objects("p", self.cfg["bucardo"]["replication_objects"])

        # 'S' is for sequence in pg_class.relkind.
        schema_sequences = self._find_objects("S", self.cfg["bucardo"]["replication_objects"])

        # Reformat [(schema1, table1), (schema2, table2)] (psycopg2 output)
        # into 'schema1.table1 schema2.table2' (bucardo input).
        table_names = ["%s.%s" % name for name in schema_tables]
        sequence_names = ["%s.%s" % name for name in schema_sequences]

        tables = " ".join(table_names)
        sequences = " ".join(sequence_names)

        print("Storing metadata.")
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} " f"add tables {tables} db=primary_db"
        )
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f"add sequences {sequences} db=primary_db"
        )
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f"add herd {self.repl_name} {tables} {sequences}"
        )

    def _async_kick_start(self):
        """Automatically kick replication sync every second.

        This means the database doesn't have to kick synchronously every time there's a commit.
        Asynchronous kicking can be less resource-intensive than synchronous and prevent outages.
        """
        # Run 'kick' in the background forever.
        pid = Popen(
            [
                "watch",
                "-n 1",
                "bucardo",
                f"{self.bucardo_opts} {self.bucardo_conn_bucardo_format} kick {self.repl_name}",
            ],
            stdout=DEVNULL,
        ).pid

        # Manage the process using a pidfile.
        pid = str(pid)
        with open(self.autokick_pidfile, "w") as pidfile:
            pidfile.write(pid)

        print(f"The bucardo sync is being kicked every second. See {self.autokick_pidfile}")

    def _async_kick_stop(self):
        """Kill the process that kicks the replication sync every second.

        See _async_kick_start for details.
        """
        try:
            with open(self.autokick_pidfile) as pidfile:
                for pid in pidfile:
                    pid = int(pid)
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        print("Unable to kill asynchronous kick process. You must manage it manually.")
                    else:
                        os.remove(self.autokick_pidfile)
        except FileNotFoundError:
            print(
                f"Expected {self.autokick_pidfile} not found. "
                "If the asynchronous kick process is running, you must manage it manually."
            )

    def _configure_bucardo(self):
        """Tell bucardo where to log and write pidfiles.

        The `_configure_bucardo()` function calls the bucardo command line tool, which
        connects to the bucardo database and updates the config with information about
        where to log and write pidfiles.  We use the `repl_name` variable in the file
        names so that the user can run multiple daemons on the same server.
        """
        # Tell bucardo where to log and how much to log.
        logdir = "/var/log/bucardo/"
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f'set reason_file="{logdir}{self.repl_name}_reason_file"'
        )
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f'set log_conflict_file="{logdir}{self.repl_name}_log_conflict_file"'
        )
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f'set warning_file="{logdir}{self.repl_name}_warning_file"'
        )
        # Tell bucardo where to write pidfiles and stopfiles.
        os.system(f"mkdir -p {self.piddir}")
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} " f'set piddir="{self.piddir}"')
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f'set stopfile="{self.repl_name}_stopfile"'
        )

    def _manage_triggers(self):
        """Determine which triggers need to be enabled or disabled, based on the config.

        See _toggle_triggers() for more details on the logic.
        """

        async_kick = self.cfg["bucardo"].get("asynchronous_kicking")
        cascade = self.cfg["databases"]["primary"].get("cascade")

        # Always disable kicking if configured to do so. This can prevent outages.
        if async_kick:
            self._toggle_triggers("disable", f"bucardo_kick_{self.repl_name}")

        # Always enable truncate triggers if needed to propagate truncates downstream.
        # This prevents data loss.
        if cascade:
            self._toggle_triggers("enable always", f"bucardo_note_trunc_{self.repl_name}")

            # If we need to propagate changes downstream (cascade),
            # and we don't have another method of propagation (async kicking),
            # enable the kick trigger to always fire on database B in an A->B->C replication topology.
            if not async_kick:
                self._toggle_triggers("enable always", f"bucardo_kick_{self.repl_name}")

    def _toggle_triggers(self, enable_or_disable, trigger):
        """Enable or disable triggers.

        KICK:

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

        TRUNC:

        The truncate triggers are the ones that propagate a TRUNCATE issued on the primary.

        Reasons for enabling always:

        If a truncate is issued on the primary in an A->B->C cascading replication topology,
        the truncate trigger on database B must be enabled always in order for the truncate
        to be propagated downstream to C.
        """

        if enable_or_disable == "disable":
            tgenabled = "D"
        elif enable_or_disable == "enable always":
            tgenabled = "A"
        else:
            raise ValueError(
                f"Invalid enable_or_disable value {enable_or_disable}. " 'Accepted values: "enable always", "disable".'
            )

        # Get the list of tables.
        # 'r' is for relation in pg_class.relkind.
        tables = self._find_objects("r", self.cfg["bucardo"]["replication_objects"])

        # The bucardo trigger that will kick off a notification when
        # changes come in to the primary database
        # (which is B in an A -> B-> C topology).
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
                enabled = "DISABLE TRIGGER" if enable_or_disable == "disable" else "ENABLE ALWAYS TRIGGER"
                query = sql.SQL("ALTER TABLE {schema}.{table} {enabled} {trigger}").format(
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
                    print(f"Could not modify {trigger} on {table[0]}.{table[1]}")
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    conn.close()
                    raise
        conn.close()
        print("Triggers toggled.")

    def add_triggers(self):
        """Add triggers to tables on primary database."""
        print("Adding triggers. Warning: this may cause an outage.")
        # Default to never doing the initial data copy onto the secondary.
        one_time_copy = 0

        # Allow the user to override the copy setting.
        map_copy_values = {"never": 0, "always": 1, "empty": 2}

        if self.cfg["bucardo"]["copy_data"] in map_copy_values:
            one_time_copy = map_copy_values[self.cfg["bucardo"]["copy_data"]]

        # Actually configure replication, including trigger adding.
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f"add sync {self.repl_name} relgroup={self.repl_name} "
            f"dbs=primary_db:source,secondary_db:target onetimecopy={one_time_copy}"
        )

        self._manage_triggers()

        print("Done adding triggers.")

    def change_config(self):
        """Change bucardo config setting. Prompts for user input."""
        # Prompt for input.
        self.setting_name = input("Name of the setting to change: ")
        self.new_value = input("New value for the setting: ")
        # Update the config using the bucardo daemon.
        os.system(
            f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
            f"set {self.setting_name}={self.new_value}"
        )
        print("You will need to reload or restart bucardo for the change to take effect.")

    def drop_triggers(self):
        """Drop triggers from primary database."""
        print("Dropping triggers. Warning: this may cause an outage. Ctrl-c to abort.")
        # Get the list of tables.
        # 'r' is for relation in pg_class.relkind.
        tables = self._find_objects("r", self.cfg["bucardo"]["replication_objects"])

        # Bucardo puts three triggers on each table, with predictable names.
        triggers = ["bucardo_delta", f"bucardo_kick_{self.repl_name}", f"bucardo_note_trunc_{self.repl_name}"]
        conn = psycopg2.connect(self.primary_schema_owner_conn_pg_format)
        # Drop each trigger from each table.
        for table in tables:
            for trigger in triggers:
                query = sql.SQL("DROP TRIGGER IF EXISTS {trigger} ON {schema}.{table}").format(
                    trigger=sql.Identifier(trigger),
                    schema=sql.Identifier(table[0]),
                    table=sql.Identifier(table[1]),
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        conn.commit()
                except psycopg2.errors.LockNotAvailable:
                    print(f"Could not drop {trigger} from {table[0]}.{table[1]}")
                    conn.rollback()
                except Exception:
                    conn.rollback()
                    conn.close()
                    raise
        conn.close()

        print("Triggers dropped.")

    def install(self):
        """Install bucardo metadata."""
        print("Installing bucardo.")

        # The EOF sends input that the user would otherwise be prompted for.
        # All it's doing is telling the install to proceed with the default values.
        return_code = os.WEXITSTATUS(
            os.system(
                f"""bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} install << EOF
P
P
EOF"""
            )
        )

        if return_code:
            raise Exception("Unable to install bucardo.")
        else:
            print("Configuring logging.")
            self._configure_bucardo()

            print("Storing database connection info.")
            os.system(
                f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
                f"add db primary_db {self.primary_conn_bucardo_format}"
            )

            # Makedelta is a bucardo flag that is one component of setting up cascading replication.
            makedelta = ""
            if self.cfg["databases"]["secondary"].get("cascade"):
                makedelta = "makedelta=1"
            os.system(
                f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} "
                f"add db secondary_db {self.secondary_conn_bucardo_format} {makedelta}"
            )

            print("Adding metadata about the tables and sequences that will be replicated.")
            self._add_table_sequence_metadata()

            print("Bucardo installed.")

    def reload(self):
        """Reload bucardo config."""
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} reload_config")

    def restart(self):
        """Restart bucardo daemon."""
        print("Restarting daemon.")
        if self.cfg["bucardo"].get("asynchronous_kicking"):
            self._async_kick_stop()
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} restart")
        if self.cfg["bucardo"].get("asynchronous_kicking"):
            self._async_kick_start()
        print("Daemon restarted.")

    def start(self):
        """Start bucardo daemon."""
        print("Starting daemon.")
        # The piddir gets deleted after the bucardo server is restarted, and bucardo won't start without it.
        os.system(f"mkdir -p {self.piddir}")
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} start")
        if self.cfg["bucardo"].get("asynchronous_kicking"):
            self._async_kick_start()
        print("Daemon started.")

    def status(self):
        """Report status of bucardo daemon."""
        print("Checking status of bucardo.")
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} status")

    def stop(self):
        """Stop bucardo daemon."""
        print("Stopping daemon.")
        os.system(f"bucardo {self.bucardo_opts} {self.bucardo_conn_bucardo_format} stop")
        if self.cfg["bucardo"].get("asynchronous_kicking"):
            self._async_kick_stop()
        print("Daemon stopped.")

    def uninstall(self):
        """Uninstall bucardo metadata.

        If no bucardo database or schema is present, the uninstall method will
        gracefully perform no-op commands.
        """
        print("Uninstalling bucardo.")

        print("Dropping the bucardo database.")
        drop_db_cmd = sql.SQL("DROP DATABASE IF EXISTS {bucardo_db}").format(
            bucardo_db=sql.Identifier(self.bucardo["dbname"])
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

        print("Dropping the bucardo schema inside the primary database.")
        drop_schema_cmd = "DROP SCHEMA IF EXISTS bucardo CASCADE"

        conn = psycopg2.connect(self.primary_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(drop_schema_cmd)
                conn.commit()
        finally:
            conn.close()

        if self.cfg["databases"]["primary"].get("cascade"):
            print("\n\033[1mWarning:\033[0m You may have just broken replication from A to B in your A->B->C setup.")
            print("To get it working again, see docs/bucardo.md, Cascading Replication, Changing Topology.")
            time.sleep(3)

        print("Uninstalled bucardo.")

    def wait_for_copy(self):
        """Wait for the initial data copy of bucardo to complete.

        This method polls the database every 5 seconds checking on the status of the copy.
        """
        print("Checking to see if the data copy is complete.", end="", flush=True)
        if self.cfg["bucardo"]["copy_data"] in ["always", "empty"]:
            while True:
                conn = psycopg2.connect(self.bucardo_conn_pg_format)
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT onetimecopy FROM bucardo.sync")
                        onetimecopy_status = cur.fetchone()
                finally:
                    conn.close()
                if onetimecopy_status[0] == 0:
                    break
                else:
                    print(".", end="", flush=True)
                    time.sleep(5)
        else:
            print("\nNot configured to do a data copy.")
        return True

    def _validate_install(self):
        print("Check: bucardo database connection info stored...", end="")
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bucardo.db")
                dbrows = cur.fetchone()
        finally:
            conn.close()
        if dbrows[0] >= 2:
            print("Pass.")
        else:
            print("Fail.")
            print("ERROR: Expecting at least two database entries in the bucardo.db table.")
            raise Exception()

        print("Check: replication objects exist...", end="")
        conn = psycopg2.connect(self.bucardo_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM bucardo.goat")
                dbrows = cur.fetchone()
        finally:
            conn.close()
        if dbrows[0] >= 1:
            print("Pass.")
        else:
            print("Fail.")
            print("ERROR: No objects to replicate were successfully recorded in the bucardo.goat table.")
            raise Exception()

    def _validate_uninstall(self):
        print("Check: bucardo metadata database dropped...", end="")
        conn = psycopg2.connect(self.bucardo_fallback_conn_pg_format)
        try:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s")
                cur.execute(query, [self.bucardo["dbname"]])
                rowcount = cur.fetchone()
        finally:
            conn.close()
        if not rowcount[0]:
            print("Pass.")
        else:
            print("Fail.")
            print("ERROR: Bucardo metadata database still exists.")
            raise Exception()

        print("Check: bucardo schema dropped in primary database...", end="")
        conn = psycopg2.connect(self.primary_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_namespace WHERE nspname = 'bucardo'")
                rowcount = cur.fetchone()
        finally:
            conn.close()
        if not rowcount[0]:
            print("Pass.")
        else:
            print("Fail.")
            print("ERROR: bucardo schema still present on primary database.")
            raise Exception()

    def _validate_start(self):
        print("Check: bucardo pidfile contains a pid...", end="")
        try:
            with open(f"/var/run/bucardo/{self.repl_name}_piddir/bucardo.mcp.pid", "r") as file:
                pid = file.readline().rstrip()
        except FileNotFoundError:
            print("Fail.")
            print(f"ERROR: The bucardo pid file is missing from /var/run/bucardo/{self.repl_name}_piddir.")
            raise Exception()
        else:
            print("Pass.")

        print("Check: pid points to a running bucardo process...", end="")
        try:
            with open(f"/proc/{pid}/cmdline") as file:
                if file.read().find("Bucardo") != -1:
                    print("Pass.")
                else:
                    print("Fail.")
                    print(f"ERROR: The pid {pid} in the bucardo pid file is for a process that isn't Bucardo.")
                    raise Exception()
        except FileNotFoundError:
            print("Fail.")
            print(f"ERROR: The bucardo pid file points to a process {pid} that isn't running.")
            raise Exception()

    def _validate_restart(self):
        self._validate_start()

    def _validate_stop(self):
        print("Check: bucardo pidfile does not exist...", end="")
        # Not ideal, but bucardo.stop runs async and can take a second or two to finish.
        time.sleep(5)
        try:
            with open(f"/var/run/bucardo/{self.repl_name}_piddir/bucardo.mcp.pid", "r") as file:
                pid = file.readline().rstrip()
        except FileNotFoundError:
            print("Pass.")
        else:
            print("Fail.")
            print(f"ERROR: There is a bucardo pid file in /var/run/bucardo/{self.repl_name}_piddir with pid {pid}.")
            raise Exception()

    def _validate_trigger_count(self, expected_count):
        # Check that the triggers are at least present/absent on the tables.
        # Checking enabled/disabled status would basically just duplicate the logic and risk introducting new bugs.
        print(f"Check: all tables have {expected_count} bucardo triggers...", end="")

        # Get the list of tables.
        # 'r' is for relation in pg_class.relkind.
        tables = self._find_objects("r", self.cfg["bucardo"]["replication_objects"])

        # Check that there are the right number of triggers beginning with 'bucardo' on each expected table.
        query = sql.SQL(
            """SELECT pn.nspname || '.' || pc.relname
               FROM pg_class pc
                   JOIN pg_namespace pn ON pn.OID = pc.relnamespace
                   LEFT JOIN pg_trigger pt ON pt.tgrelid = pc.OID AND tgname LIKE 'bucardo%'
               WHERE pn.nspname || '.' || pc.relname IN ({table_names})
               GROUP BY pn.nspname || '.' || pc.relname
               HAVING COUNT(pt.OID) <> {variable}
            """
        ).format(
            table_names=sql.SQL(",").join(map(sql.Literal, (".".join(i) for i in tables))),
            variable=sql.Literal(expected_count),
        )
        conn = psycopg2.connect(self.primary_schema_owner_conn_pg_format)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                unexpected_triggers = ", ".join(t[0] for t in cur.fetchall())
                if cur.rowcount:
                    # There are tables with the wrong number of bucardo triggers.
                    print("Fail.")
                    print(f"ERROR: Unexpected number of triggers on {unexpected_triggers}.")
                    raise Exception()
                else:
                    # All tables have the expected number of bucardo triggers.
                    print("Pass.")
        except Exception as e:
            # If we can't validate, abort.
            raise Exception(e)
        finally:
            conn.close()

    def _validate_drop_triggers(self):
        # Check that there are 0 bucardo triggers on each table.
        self._validate_trigger_count(0)

    def _validate_add_triggers(self):
        # Check that there are 3 bucardo triggers on each table.
        self._validate_trigger_count(3)
