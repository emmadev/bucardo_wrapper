"""This module contains the logic for running bucardo jobs concurrently.

Bucardo is a third-party, open-source Postgres replication tool written by
Greg Mullane.  You can find the source code at
https://github.com/bucardo/bucardo.

It assumes the bucardo database is named bucardo.  If you try to name the db
something else, stuff will break.

I've thus created a fork of two of the open source bucardo files, `bucardo`
(written in Perl) and `bucardo_schema.sql` (written in SQL).  Obviously, the
ideal approach would be to make this open source quality and submit a PR and
try to get it accepted, but for now, we have a fork.

The `install` method of the Concurrency class writes the local forked files to
the places in the OS where the bucardo command line tool will look for them.
This requires having root access in one case.

Classes exported:
Concurrency: Dependency installation for running bucardo jobs simultaneously on the same server.
"""

import filecmp
import subprocess

from plugins import Plugin


class Concurrency(Plugin):
    """Allow the user to run multiple bucardo jobs concurrently on the same server.

    The one dependency that the Bucardo plugin doesn't have that keeps it from
    allowing the user to run multiple bucardo jobs simultaneously is the ability to
    customize the bucardo database.  A customized bucardo database means you can
    have multiple concurrent databases and run each job against a different
    database, thus keeping them from conflicting with each other.  The Concurrency
    class provides the dependencies that allow you to have a custom bucardo name.

    To use this plugin, add 'concurrency' to the list of plugins in the config
    file, and in the `concurrency` section of the config file, populate
    `bucardo_dbname`.  Keep this name unique, along with the `replication_name` in
    the bucardo section, and you can run multiple instances of bucardo.

    Methods exported:
    install: install the dependencies that allow custom bucardo database names
    uninstall: uninstall the dependencies that allow custom bucardo database names
    """

    def _update_usr_bin_bucardo(self, filename):
        """Make sure the version of the bucardo code at /usr/bin/bucardo matches the expected code.

        This function will try to copy the code at the designated filename to /usr/bin/bucardo if the
        contents of the files aren't already the same.

        Keyword arguments:
        filename -- filename whose code will become the bucardo executable
        """

        # The code only needs to be updated once per host, so check that the file contents don't already match.
        if not filecmp.cmp(filename, '/usr/bin/bucardo'):
            try:
                subprocess.run(
                    [
                        'sudo', '/bin/cp',
                        f'{filename}', '/usr/bin/bucardo'
                    ],
                    check=True
                )
            except CalledProcessError:
                print(
                    f'Could not copy {filename} to `/usr/bin/bucardo` due to a permission error. '
                    'Make sure that has been done at least once on this host.'
                )
                return
            print('/usr/bin/bucardo successfully updated.')
        else:
            print('/usr/bin/bucardo already correct. No action needed.')

    def install(self):
        """Install the dependencies that allow custom bucardo database names.

        There are two files provided by the open source bucardo project that
        hard-code the name of the bucardo database as `bucardo`: `bucardo_schema.sql`
        and `bucardo`.  Each of them has a copy in this folder that is slightly forked.
        The `install()` method writes those files to the location where they will be
        used when invoking the bucardo command line tool.

        The database name used is that provided in `bucardo_dbname` in the
        `concurrency` section of the config file.
        """
        # This file is a slightly modified variant on the bucardo schema as provided by
        # the Bucardo project.  Instead of a hard-coded name, the revised version has a
        # variable string.
        with open('plugins/concurrency/forked_bucardo_schema.sql', 'r') as file:
            # Replace all instances of the variable with the database name provided in the wrapper config.
            data = file.read().replace(':bucardo_database', f'{self.bucardo["dbname"]}')
        # Write the file to the location that bucardo loads its schema from.
        with open('/usr/local/share/bucardo/bucardo.schema', 'w') as file:
            file.write(data)

        print('/usr/local/share/bucardo/bucardo.schema up to date.')

        # This file is the bucardo executable, modified with a one-word change
        # to turn a hard-coded 'bucardo' into a variable for the dbname.
        self._update_usr_bin_bucardo('plugins/concurrency/forked_bucardo')

    def uninstall(self):
        """Remove the dependencies that allow custom bucardo database names.

        There are two files provided by the open source Bucardo project that hard-code
        the name of the bucardo database as `bucardo`: `bucardo_schema.sql` and
        `bucardo`.  Each of them has a copy in this folder that is provided by the
        Bucardo project, and a copy that has been forked and slightly modified.  The
        `uninstall()` method writes the Bucardo project copy of the files to the location
        where they will be used when invoking the bucardo command line tool, thus
        undoing any custom writes that may have been done.

        The database name used is that provided in `bucardo_dbname` in the
        `concurrency` section of the config file.
        """
        # This file contains the bucardo schema as provided by the Bucardo project.
        with open('plugins/concurrency/original_bucardo_schema.sql', 'r') as file:
            data = file.read()
        # Write the file to the location that bucardo loads its schema from.
        with open('/usr/local/share/bucardo/bucardo.schema', 'w') as file:
            file.write(data)

        print('/usr/local/share/bucardo/bucardo.schema up to date.')

        # This file is the bucardo executable.
        self._update_usr_bin_bucardo('plugins/concurrency/original_bucardo')
