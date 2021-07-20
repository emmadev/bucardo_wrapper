# Frequently Asked Questions

Q: **What version of Python does the Bucardo Wrapper tool require?**

A: The wrapper was developed on Python 3.9.1. It is not guaranteed to work with
lower versions, but some plugins may.

Q: **Is all bucardo functionality supported by Bucardo Wrapper?**

A: No. Only a limited amount of functionality, that required by CM Group, has been
incorporated. To take advantage of functionality not supported by this wrapper,
such as individually managed bucardo syncs, you must either set up bucardo
yourself without using this script, or create a PR to add that functionality to
this wrapper.

Q: **Where does Bucardo Wrapper log?**

A: The wrapper itself prints directly to the console. If you have lot of
plugins enabled, you may have to scroll up to see the output that printed
before the script redisplayed the menu and returned you to the prompt.

The bucardo package used by the bucardo plugin logs to `/var/log/bucardo`.
See `bucardo.md` for more details.

Q: **How do I set up replication from database A -> B -> C?**

A: This topology is sometimes known as cascading replication, because changes
"cascade" from A to B to C. To find out how to do this, see `docs/bucardo.md`,
the "Cascading Replication" section.

Q: **How do I minimize outages caused by trigger-adding and trigger-dropping?**

A: Use the `retry` plugin.

Q: **How do I update my materialized views on the secondary database?**

A: Note that bucardo won't handle this. The materialized views on the secondary will
be empty until you refresh them using the `mat_views` plugin.

Q: **How do I speed up the initial data copy?**

A: Use the `indexes` plugin to drop indexes on the secondary while the data
copy is in progress, then recreate them after it's done.

Q: **What if I need to execute a schema change on a table being replicated in bucardo?**

A: Execute the schema change on all nodes, then execute `bucardo.restart` to
inform bucardo of the change. Replication will be stalled until the restart, so
remember to do it shortly after the schema change.

Q: **How do I run multiple bucardo jobs on the same server at the same time?**

A: This functionality is not supported by the bucardo package itself, so you must
use the `concurrency` plugin.

Q: **When do I need to invoke the concurrency plugin?**

A: The `concurrency` plugin, as its name indicates, was designed to allow you
to run multiple instances of bucardo simultaneously on the same server, in
complete isolation (separate logs, separate daemon invocations, etc.). A side
effect of this is that it allows you to change the name of the local database
used to store bucardo metadata. By default, that database is named `bucardo`,
but you can change that.

You need to invoke the `concurrency` plugin before `bucardo.install` or
`bucardo.uninstall` in the following scenarios:

1. You want to create a database with a custom name.

Normally, you would want to do this for running simultaneous bucardo jobs, but
you can also do it just because you feel like it.

In that case, run `concurrency.install` before `bucardo.install`, so that
`bucardo.install` creates a database of the right name.

2. You want to uninstall your bucardo setup, and you have a custom database
name that you want to drop.

In that case, you need to have the concurrency plugin uncommented, but you
don't need to run any of the `concurrency` functions before running
`bucardo.install`.

Having the plugin uncommented just tells the wrapper to look in the concurrency
section of the config file for the bucardo database name. If the plugin name is
commented out, only the database named `bucardo` will be dropped, if there is
one.

3. You previously created a database with a custom name, and you want to go
back to a vanilla bucardo setup with a database named `bucardo`.

In that case, you must run `concurrency.uninstall` before running
`bucardo.install`, so that it creates a database by the name of `bucardo` and
not whatever you most recently installed.

The underlying principle here is that one of the things `concurrency` does is
update the file that contains the DDL that creates the bucardo database. If
you're about to create a bucardo database (i.e. by running `bucardo.install`),
the contents of `/usr/local/share/bucardo/bucardo.schema` must contain the
correct database name.

If you're about to drop a bucardo database (i.e. by running
`bucardo.uninstall`), only the config file must be correct: the `concurrency`
plugin has to be uncommented and the correct database name has to be specified
in the concurrency section of the config file.
