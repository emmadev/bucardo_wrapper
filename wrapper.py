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
    suboptions = [i for i in available_cmds[menu_cfg]]
    options = [f"{p}{i}" for i in suboptions for p in ["", "c1.", "c2."] if f"{p}{i}".startswith(text)]
    if state < len(options):
        return options[state]
    else:
        return None


def construct_menu(plugins_added, available_cmds):
    menu = ""
    # Bucardo, as core functionality, goes at the top of the user menu.  All others are alphabetized.
    optional_plugins = [p for p in plugins_added[menu_cfg] if p != "bucardo"]
    optional_plugins.sort()
    if "bucardo" in plugins_added[menu_cfg]:
        sorted_plugins = ["bucardo"] + optional_plugins
    else:
        sorted_plugins = optional_plugins
    for plugin_name in sorted_plugins:
        # The header of each submenu is the plugin name and summary line of the class's docstring.
        doc_summary = plugins_added[menu_cfg][plugin_name]["instance"].__doc__.split("\n")[0]
        menu = f"{menu}\nCommands for {plugin_name}: {doc_summary}"
        # The contents of each submenu are the public method names and summary line of each method's docstring.
        for cmd in plugins_added[menu_cfg][plugin_name]["menu"]:
            fully_qualified_cmd = f"{plugin_name}.{cmd}"
            cmd_info = available_cmds[menu_cfg][fully_qualified_cmd]
            # Print the names of commands in bold.
            menu = f'{menu}\n  \033[1m{fully_qualified_cmd}:\033[0m {cmd_info["doc"]}'
        menu = f"{menu}\n"
    return menu.rstrip()


def define_commands(plugins_added, config_file):
    # Each plugin returns a set of methods invokable by the end user.
    for plugin_name in plugins_added[config_file]:
        plugin_class = plugins_added[config_file][plugin_name]["class"]

        plugins_added[config_file][plugin_name]["menu"] = plugin_class._menu_options(plugin_class)
        # Populate a dictionary mapping the names of the user-invokable methods to the actual class attribute.
        for cmd in plugins_added[config_file][plugin_name]["menu"]:
            fully_qualified_cmd = f"{plugin_name}.{cmd}"
            available_cmds[config_file][fully_qualified_cmd] = {}
            available_cmds[config_file][fully_qualified_cmd]["func"] = getattr(
                plugins_added[config_file][plugin_name]["instance"], cmd
            )
            # Add the summary line of the method's docstring, for display in the menu.
            summary_docstring = available_cmds[config_file][fully_qualified_cmd]["func"].__doc__.split("\n")[0]
            available_cmds[config_file][fully_qualified_cmd]["doc"] = summary_docstring
            available_cmds[config_file][fully_qualified_cmd]["valid"] = getattr(
                plugins_added[config_file][plugin_name]["instance"], f"_validate_{cmd}", lambda: None
            )

        available_cmds[config_file]["exit"] = {}

    return available_cmds


def import_plugin_modules(config_file, plugin_name):
    # Dynamically import each plugin.
    sys.path.append(plugin_name)
    plugins_added[config_file][plugin_name] = {}
    plugins_added[config_file][plugin_name]["module"] = importlib.import_module(plugin_name)
    # The class is the same name as the plugin, but CamelCased.
    plugin_name_caps = "".join(x.capitalize() or "_" for x in plugin_name.split("_"))
    plugin_class = getattr(plugins_added[config_file][plugin_name]["module"], plugin_name_caps)
    plugins_added[config_file][plugin_name]["class"] = plugin_class
    cfg = config.load_config(config_file)
    plugins_added[config_file][plugin_name]["instance"] = plugin_class(cfg)

    return plugins_added


parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config_file",
    help="path to the file with the configuration settings",
)

parser.add_argument(
    "-c1",
    "--config_file1",
    help="path to the file with A->B configuration settings",
)

parser.add_argument(
    "-c2",
    "--config_file2",
    help="path to the file with A->B configuration settings",
)

parser.add_argument(
    "-i", "--interactive", help="Enter interactive mode for issuing bucardo commands", action="store_true"
)

parser.add_argument(
    "-l",
    "--list_commands",
    help='Space-separated list of commands, e.g. "bucardo.install bucardo.add_triggers"',
)

args = parser.parse_args()

menu_cfg = args.config_file or args.config_file1
plugin_cfg = config.load_config(menu_cfg)

readline.parse_and_bind("tab: complete")
readline.set_completer(completer)

plugin_dir = "plugins"

sys.path.append(plugin_dir)

# Allow the user's config to specify which plugins they want to be prompted
# with options for.  The reason this is valuable is that many plugins are
# irrelevant to many databases.
list_plugins = plugin_cfg["plugins"]

# This dictionary will hold the objects for the imported modules.
plugins_added = {}

# This dictionary will map the names of functions that the user can execute to
# the actual functions.  This allows the wrapper script to fetch the functions
# from the plugins dynamically, without executing random user input.
available_cmds = {}

if not list_plugins:
    print("No plugins found.")
    exit()

configs = [args.config_file, args.config_file1, args.config_file2]
valid_configs = [config for config in configs if config is not None]
for config_file in valid_configs:
    plugins_added[config_file] = {}
    available_cmds[config_file] = {}
    for plugin_name in list_plugins:
        plugins_added = import_plugin_modules(config_file, plugin_name)
        available_cmds = define_commands(plugins_added, config_file)


def execute(user_cmd, config_file):
    if user_cmd == "exit":
        print("Exiting.")
        exit()
    if user_cmd in available_cmds[config_file]:
        print(f"Executing \033[1m{user_cmd}\033[0m")
        try:
            available_cmds[config_file][user_cmd]["func"]()
        except KeyboardInterrupt:
            # Return the user to the prompt.
            print("Exiting")
            pass
    else:
        print("Not a recognized command.")


def validate(user_cmd, config_file):
    try:
        print(f"\nValidating \033[1m{user_cmd}\033[0m")
        available_cmds[config_file][user_cmd]["valid"]()
    except Exception as e:
        print(e)
        print(f"{user_cmd} step failed. Aborting.")
        raise Exception()
    else:
        print(f"{user_cmd} step passed checks. Continuing.\n\n")


def parse_cmd(command):
    user_cmd = command
    config_file = menu_cfg
    components = command.split(".", -1)
    if len(components) == 3:
        if components[0] == "c1":
            user_cmd = f"{components[1]}.{components[2]}"
            config_file = args.config_file1
        elif components[0] == "c2":
            user_cmd = f"{components[1]}.{components[2]}"
            config_file = args.config_file2
    elif len(components) == 2:
        user_cmd = f"{components[0]}.{components[1]}"
        config_file = menu_cfg
    return (user_cmd, config_file)


if args.interactive:
    # Let the user supply commands interactively from a menu.
    menu = construct_menu(plugins_added, available_cmds)

    # Display the menu, prompt the user to enter a command, execute the command, and reprompt the user.
    while True:
        print(menu)
        # input() with readline imported above, allows the user the scroll through their
        # history using the up and down arrows.
        user_cmd = input('\nEnter the command you wish to run here, or type "exit": ')
        print()
        user_cmd, config_file = parse_cmd(user_cmd)
        execute(user_cmd, config_file)
        validate(user_cmd, config_file)
        print()
else:
    # Use the commands passed in using the --list_commands flag.
    for user_cmd in args.list_commands.split():
        user_cmd, config_file = parse_cmd(user_cmd)
        execute(user_cmd, config_file)
        validate(user_cmd, config_file)
