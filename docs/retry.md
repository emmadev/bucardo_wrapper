# Overview

The purpose of the retry plugin is to reduce customer-facing outages when
adding and dropping bucardo triggers on production tables.

Because bucardo uses triggers, and triggers take an exclusive lock on the
table, setting up bucardo can require downtime. By default, bucardo will
attempt to add or drop a trigger, hang onto its place in the queue for the lock
it's trying to acquire, and throw an error if you cancel the trigger attempt,
leaving replication in an intermediate state that it's difficult to recover
from.

The retry plugin uses `try` logic combined with the Postgres `lock_timeout`
parameter to reduce downtime while making it easier to coax triggers onto a
production database. The user can configure a threshold after which bucardo
will time out and release the attempt to get the lock on a table, thus allowing
other queries on that table to run. The user can rerun the function as many
times as needed, until all triggers have been added. This allows incremental
adding of triggers with minimal disruption to users.

Because of the dependency on `lock_timeout`, this plugin requires Postgres 9.3
or later.

# Usage

Add `retry` to the list of plugins in the config file.

Set your timeout threshold in milliseconds in the `timeout` parameter in the
`retry` section of the config file. This is a per-trigger timeout. The default is
10000 milliseconds, i.e. 10 seconds.

Run `python wrapper.py`.

Run `bucardo.install` to create the bucardo database.

Run `retry.install` to load a forked stored procedure.

Run `retry.add_triggers` to add as many triggers as possible.

If necessary, rerun `retry.add_triggers` until no triggers are reported as
having been impossible to add before timeout.

When done with the migration, run `retry.drop_triggers` to drop as many
triggers as possible.

If necessary, rerun `retry.drop_triggers` until no triggers are reported as
having been impossible to drop before timeout.


# Functions

## add\_triggers

The `add_triggers` function in the retry plugin adds bucardo triggers to the
tables designated for replication in `bucardo.replication_objects` in the
config. The function times out gracefully on a per-trigger basis to allow user
traffic to proceed and can be rerun indefinitely. It will add triggers
incrementally if it's unable to add them all, and it will report failures. If
no failures are reported, all triggers have been added. Once all triggers have
been added, running this function is harmless and does nothing at all.

## drop\_triggers

The `drop_triggers` function in the retry plugin drops bucardo triggers to the
tables designated for replication in `bucardo.replication_objects` in the
config. The function times out gracefully on a per-trigger basis to allow user
traffic to proceed and can be rerun indefinitely. It will drop triggers
incrementally if it's unable to drop them all, and it will report failures. If
no failures are reported, all triggers have been droped. Once all triggers have
been droped, running this function is harmless and does nothing at all.

## install

The `install` function in the retry plugin loads a stored procedure into the
bucardo database. This stored procedure was provided by the open source Bucardo
project and slightly modified by me to have try-catch logic around trigger
adds.

The code for the modified stored procedure lives in `plugins/retry/custom_validate_sync.sql`

The modified stored procedure has a dependency on the Perl Try::Tiny module.
This module is not bundled with Bucardo nor is it installed by the `install`
plugin. You must install this yourself, using cpanm or a package manager.

## uninstall

The `uninstall` function in the retry plugin loads a stored procedure into the
bucardo database. This stored procedure was written by Greg Mullane.

The code for the stored procedure lives in `plugins/retry/original_validate_sync.sql`

This function does not remove the Perl Try::Tiny module that you may have
installed on your system.

# Dependencies

- Perl module Try::Tiny (on the host where the bucardo database lives)
