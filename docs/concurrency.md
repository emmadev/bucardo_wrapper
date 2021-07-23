# Overview

The purpose of this plugin is to allow the user to run multiple instances of
the bucardo daemon simultaneously, to reduce the number of VMs you have to spin
up.

Bucardo does not support this functionality out of the box.

Because Bucardo stores configuration data inside the `bucardo_config` table in
the bucardo config, in order to have multiple configurations, you would either
have to change the schema or use different databases. For isolation purposes,
I've chosen to go with different databases.

In order to do that, I've had to make small tweaks to two files in the bucardo
source code. The `plugins/concurrency` folder contains both the original and
forked code for those two files:

- bucardo (written in Perl)
- bucardo\_schema.sql (written in SQL)

# Usage

Add `concurrency` to the list of plugins in the config file.

Run `python wrapper.py`.

Before running `bucardo.install`, run `concurrency.install`.

In the `concurrency` section of the config file, specify a value for `bucardo_dbname`.

Make sure `bucardo_dbname` is unique across simultaneous bucardo runs.

Make sure the `replication_name` in the bucardo section of the config file is
unique across simultaneous bucardo runs.

Install your bucardo instance and proceed.

# Functions

## Install

The install function of the concurrency plugin writes the two forked files to
the respective locations where they will be used when the bucardo command line
tool is run.

- `forked_bucardo` is written to `/usr/bin/bucardo`
- `forked_bucardo_schema.sql` is written to `/usr/local/share/bucardo/bucardo.schema`

## Uninstall

The uninstall function of the concurrency plugin writes the two original files
to the respective locations where they will be used when the bucardo command
line tool is run.

- `original_bucardo` is written to `/usr/bin/bucardo`
- `original_bucardo_schema.sql` is written to `/usr/local/share/bucardo/bucardo.schema`

# Dependencies

- /usr/local/share/bucardo (writable by the user running this script)
