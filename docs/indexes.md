# Overview

The purpose of this plugin is to make it possible to drop large indexes on a
database before bulk loading data in. The intended use case is when setting up
bucardo replication. Removing large indexes can make the initial data copy
orders of magnitude faster.

The plugin automatically detects sufficiently large indexes on the primary,
backs up the DDL, drops the indexes on the secondary database, and restores the
indexes on the standby from the backed up DDL.

There is a way to disable indexes during copy in bucardo, but it requires
permissions that aren't available on RDS. This plugin was developed for RDS but
will work on community Postgres installations as well.

# Usage

Add `indexes` to the list of plugins in the config file.

Run `python wrapper.py`.

Run `bucardo.install` to create the bucardo database.

Run `indexes.install` to populate the bucardo database with index-related
objects.

To set a threshold for which indexes to drop, update `larger_than` in the
`indexes` section of the config file. See the `drop_indexes` section below for
more details and warnings.

When you're ready to drop indexes, run `index.drop`.

`indexes.recreate` will give you the option of recreating the indexes
immediately or waiting on a bucardo data copy to finish. If you want to wait,
select 'y' at the prompt. The script will monitor the data copy and start
recreating indexes when it detects the data copy has inded. See the
documentation below on the `recreate_indexes` function for more details and
warnings.

The advantage is that you don't know exactly when a long-running data copy is
going to finish, and it might be in the middle of the night. Auto-detecting
data copy completion prevents a human from having to monitor it and manually
kick off index recreation, and also avoids potentially having several hours
spent idle that could have been better spent recreating indexes.

## Cascading Replication

If you're replicating from database A to B to C, the B -> C job will check the
size of indexes on B. A is not specified in the B -> C config, so the job has
no way of knowing about A. If the data copy and index recreation aren't
finished on B, then running indexes.install on B -> C will not detect large
indexes on B. This means it is necessary to wait until A -> B is finished
before starting B -> C.

# Functions

## install

The install function in the indexes plugin loads the DDL in
`plugins/indexes/custom_manage_indexes.sql` to the bucardo index. This creates
a schema named `manage_indexes` and a table in the bucardo database.

## drop\_indexes

The `drop_indexes` function performs these steps:

1. Detects indexes to be dropped, with reference to `larger_than` in the config file.

Note that this is the size of the table, not the index. All indexes on tables
larger than this will be dropped, unless they are primary keys. Primary keys
can't be dropped because Bucardo (like most logical replication tools) requires
them to be present for replication purposes.

The `larger_than` parameter accepts formats of '10 GB', '10GB', and '10
gigabyte', but not '10 gigabytes'. This is a limitation of the `quantities`
Python library.

2. Stores the DDL for these indexes in the bucardo databases.

3. Performs a simple sanity check. If the number of indexes stored per table
doesn't match the number detected per table, the script will abort without
dropping.

4. Drop the indexes on the standby.

## recreate\_indexes

The `recreate_indexes` function has two options: recreate immediately or
monitor the bucardo data copy and recreate when the data copy is finished. The
latter option allows you to start a migration, run `indexes.recreate_indexes`,
select 'y', and walk away from your computer, knowing that the data copy and
index recreation will finish without further input from you.

The downside is that the logic for detecting when a data copy is finished is
prone to race conditions. It will grep the bucardo log at the beginning of the
run, search for a specific string ('Total target rows copied:'), count the
number of occurrences of lines matching that string in the file, and grep every
60 seconds to see if that number has increased. The advantage is that you can
run multiple bucardo data copies without having to clean up your log file. The
disadvantage is that there's a small window in which you could start the
migration, and the data copy could finish before the first grep happens. In
this case, the `recreate_functions` will wait on the data copy forever.

Fortunately, you should only need to drop indexes on very long-running data
copies, so this shouldn't arise often in practice.

If you notice that it's happened, you can just ctrl-c out and run
`indexes.recreate` again, then choose `n` so as not to wait on the data copy.

The other disadvantage of this method is that if Bucardo changes the verbiage
in their logs, this plugin has to be updated.
