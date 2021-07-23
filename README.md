# Overview

The Bucardo Wrapper is a script for executing a database migration using Bucardo.

Bucardo is an open-source replication tool created by Jon Jensen and Greg
Sabino Mullane of End Point Corporation. Bucardo is written in Perl.

The Bucardo Wrapper provides both core Bucardo functionality (not all
functionality is supported) and the option to perform other migration-related
tasks using plugins. The Bucardo Wrapper is written in Python.

The wrapper is meant to be a DBA-friendly command line interface. As its name
implies, it's a wrapper around the bucardo command line tool that allows you to
set up replication without knowing bucardo syntax, and with minimal concern for
implementation details.

This is useful when you have to run bucardo several times in a short period
when the stakes are high, and then years pass before you have to run it again,
thus causing you to forget all the details. A wrapper script saves time on
relearning and reduces the chances of mistakes.

See the Menu section below for more on the interface.

It's also a wrapper around other database migration tasks, including any the
user might choose to develop. See the Plugins section for enabling and
disabling plugins, and `docs/plugins.md` for developing plugins.

# Config

The configuration is done in a YAML config file. This is the file the user
should edit before running the wrapper. The default config file is
`config.yml`, but by passing in a value to the `-c` or `--config_file`
parameter when invoking the wrapper, the user can specify a custom config file.

Each plugin has its own section. If you develop a plugin that requires user
configuration, add a subsection.

The documentation in the config is pretty thorough and not repeated here.

# Setup

1. Install pyenv.

2. Run

```bash
source ./env_activate.sh
```

The first time it runs, it will install the python dependencies the script needs.

3. Any time you enter the `bucardo_wrapper` directory and don't see your prompt
prefixed with `(.venv)`, execute step 2. It will execute much more quickly
after the first time.

# Usage

python wrapper.py [-c config\_file]

# Menu

The core aspect of the wrapper script is that it provides the user with a menu
of functions with human-friendly names. The user is prompted for input.The user
only has to enter the name of the function they want to execute in order for
that function to be executed. All configuration is done in the config file. See
the Config section for more detail.

The wrapper script runs on infinite loop. As soon as a command finishes
executing, the user is returned to the prompt and can execute another command.
Either ctrl-c at the prompt or typing 'exit' will exit the infinite loop and
return you to the shell.

If you hit ctrl-c while a command is running, you will be returned to the
prompt. No guarantees about rollback are made; you may need to do some cleanup.

By using the up and down arrows at the prompt, you can navigate through your
command history. That history is lost once you exit the script.

You can tab-complete your command input at the prompt. Two tabs with no input
will display the complete list of commands.

Each plugin displays its user-exposed functions as a sub-menu. Each function is in bold.

Below is a sample menu, assuming that the bucardo, indexes, and mat\_views
plugins have been enabled, and the user is entering the `mat_views.refresh`
command.

Commands for bucardo: Replication functionality built into the open source Bucardo tool.\
&nbsp;&nbsp;**bucardo.add\_triggers:** Add triggers to tables on primary database.\
&nbsp;&nbsp;**bucardo.drop\_triggers:** Drop triggers from primary database.\
&nbsp;&nbsp;**bucardo.install:** Install bucardo metadata.\
&nbsp;&nbsp;**bucardo.restart:** Restart bucardo daemon.\
&nbsp;&nbsp;**bucardo.start:** Start bucardo daemon.\
&nbsp;&nbsp;**bucardo.status:** Report status of bucardo daemon.\
&nbsp;&nbsp;**bucardo.stop:** Stop bucardo daemon.\
&nbsp;&nbsp;**bucardo.uninstall:** Uninstall bucardo metadata.

Commands for indexes: Identify large indexes and temporarily drop them on the secondary while initial copy is in sync.\
&nbsp;&nbsp;**indexes.drop:** Drop large indexes on the secondary database.\
&nbsp;&nbsp;**indexes.install:** Install the dependencies for index management on the bucardo database.\
&nbsp;&nbsp;**indexes.recreate:** Recreate dropped indexes on the secondary database.

Commands for mat\_views: Identify materialized views and refresh them on the secondary database.\
&nbsp;&nbsp;**mat\_views.refresh:** Refresh materialized views.

Enter the command you wish to run here, or type "exit": mat\_views.refresh

# Plugins

Plugins are meant to be easy to develop. See `plugins.md` in the `docs`
directory.

## Enable Plugins

To enable a plugin, add it to the `list_plugins` in `config.yml`. This causes a
subsection to appear in the menu when the user runs the wrapper. Any
user-exposed functions will be presented to the user, and the user can execute
them by entering their name at the prompt and hitting enter.

## Disable Plugins

To disable a plugin, comment it out in `list_plugins` or delete it. This only
prevents it from appearing in the menu. It does not remove any dependencies or
undo any actions that were taken using it. You can enable and disable plugins
at will, merely to reduce clutter in the menu.

All plugins are optional. You can even delete them from the plugin directory if
you wish.

## Develop Plugins

When you create a plugin, you only have to follow the right naming conventions,
and the plugin will be automatically discovered when you run the wrapper, and
the menu will be populated with the functions you've chosen to expose to the
user. The user will even have tab-completion on those functions.

See `docs/plugins.md` for details on how to write a plugin so that the wrapper
can discover and interact with it seamlessly.
