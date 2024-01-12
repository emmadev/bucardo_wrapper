"""This is an administrative script for migrating a database using bucardo.
The user will pass in a config file with hosts, db names, etc.  The script will
present the user with a list of available commands and prompt for input.  The
script will execute the command selected and return to the prompt.
"""

import argparse
import importlib
import readline
import sys

import config


def completer(text, state):
    # This enables tab completion of commands for the user.
    # Copied and pasted from stackoverflow.
    options = [i for i in available_cmds if i.startswith(text)]
    if state < len(options):
        return options[state]
    else:
        return None


def construct_menu(plugins_added, available_cmds):
    menu = ""
    # Bucardo, as core functionality, goes at the top of the user menu.  All others are alphabetized.
    optional_plugins = [p for p in plugins_added if p != "bucardo"]
    optional_plugins.sort()
    if "bucardo" in plugins_added:
        sorted_plugins = ["bucardo"] + optional_plugins
    else:
        sorted_plugins = optional_plugins

    for plugin_name in sorted_plugins:
        # The header of each submenu is the plugin name and summary line of the class's docstring.
        doc_summary = plugins_added[plugin_name]["instance"].__doc__.split("\n")[0]
        menu = f"{menu}\nCommands for {plugin_name}: {doc_summary}"

        # The contents of each submenu are the public method names and summary line of each method's docstring.
        for cmd in plugins_added[plugin_name]["menu"]:
            fully_qualified_cmd = f"{plugin_name}.{cmd}"
            cmd_info = available_cmds[fully_qualified_cmd]
            # Print the names of commands in bold.
            menu = f'{menu}\n  \033[1m{fully_qualified_cmd}:\033[0m {cmd_info["doc"]}'
        menu = f"{menu}\n"
    return menu.rstrip()


def define_commands(plugins_added):
    # Each plugin returns a set of methods invokable by the end user.
    for plugin_name in plugins_added:
        plugin_class = plugins_added[plugin_name]["class"]

        plugins_added[plugin_name]["menu"] = plugin_class._menu_options(plugin_class)
        # Populate a dictionary mapping the names of the user-invokable methods to the actual class attribute.
        for cmd in plugins_added[plugin_name]["menu"]:
            fully_qualified_cmd = f"{plugin_name}.{cmd}"
            available_cmds[fully_qualified_cmd] = {}
            available_cmds[fully_qualified_cmd]["func"] = getattr(plugins_added[plugin_name]["instance"], cmd)
            # Add the summary line of the method's docstring, for display in the menu.
            summary_docstring = available_cmds[fully_qualified_cmd]["func"].__doc__.split("\n")[0]
            available_cmds[fully_qualified_cmd]["doc"] = summary_docstring

        available_cmds["exit"] = {}

    return available_cmds


def import_plugin_modules(plugin_name):
    # Dynamically import each plugin.
    sys.path.append(plugin_name)
    plugins_added[plugin_name] = {}
    plugins_added[plugin_name]["module"] = importlib.import_module(plugin_name)

    # The class is the same name as the plugin, but CamelCased.
    plugin_name_caps = "".join(x.capitalize() or "_" for x in plugin_name.split("_"))
    plugin_class = getattr(plugins_added[plugin_name]["module"], plugin_name_caps)
    plugins_added[plugin_name]["class"] = plugin_class
    plugins_added[plugin_name]["instance"] = plugin_class(cfg)

    return plugins_added


parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config_file",
    help="path to the file with the configuration settings, default config.yml",
    default="config.yml",
)

args = parser.parse_args()

cfg = config.load_config(args.config_file)

readline.parse_and_bind("tab: complete")
readline.set_completer(completer)

plugin_dir = "plugins"

sys.path.append(plugin_dir)

# Allow the user's config to specify which plugins they want to be prompted
# with options for.  The reason this is valuable is that many plugins are
# irrelevant to many databases.
list_plugins = cfg["plugins"]

# This dictionary will hold the objects for the imported modules.
plugins_added = {}

# This dictionary will map the names of functions that the user can execute to
# the actual functions.  This allows the wrapper script to fetch the functions
# from the plugins dynamically, without executing random user input.
available_cmds = {}

if list_plugins:
    for plugin_name in list_plugins:
        plugins_added = import_plugin_modules(plugin_name)
        available_cmds = define_commands(plugins_added)

    menu = construct_menu(plugins_added, available_cmds)

    # Display the menu, prompt the user to enter a command, execute the command, and reprompt the user.
    try:
        while True:
            print(menu)

            # input() with readline imported above, allows the user the scroll through their
            # history using the up and down arrows.
            user_cmd = input('\nEnter the command you wish to run here, or type "exit": ')
            print()
            if user_cmd == "exit":
                print("Exiting.")
                exit()
            elif user_cmd in available_cmds:
                try:
                    available_cmds[user_cmd]["func"]()
                except KeyboardInterrupt:
                    # Return the user to the prompt.
                    pass

            else:
                print("Not a recognized command.")
            print()
    except KeyboardInterrupt:
        print("Exiting")
else:
    print("No plugins found.")
