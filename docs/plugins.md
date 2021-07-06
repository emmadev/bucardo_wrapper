# Plugin Creation
Instructions for creating a plugin named `my_plugin`:

1. Create a directory named `my_plugin` in the plugins directory.
2. Create `my_plugin/__init__.py`.
3. Add the following lines to `my_plugin/__init__.py`:
```python
from plugins import Plugin


class MyPlugin(Plugin):
```

Note that the name `MyPlugin` must match the directory name `my_plugin`, but
with CamelCase instead of underscores.

4. Create a docstring with a summary line for the class. This summary line will
appear in the menu. A nonexistent docstring will cause an error to be thrown.
5. Create public (user-exposed) methods for the class by not prefixing the
method name with an underscore. These methods will be automatically discovered
and populated in the menu.
6. Create a docstring with a summary line for each method. This summary line
will appear in the menu. A nonexistent docstring will cause an error to be thrown.
7. Prefix the names of any methods that you don't want to appear in the menu
with an underscore.
8. Add `my_plugin` to `list_plugins` in the config file.
9. Add documentation for your plugin to the `docs` directory.
10. Keep files that are dependencies for your plugin in the `my_plugin`
directory, when it makes sense to do so.

You're done! Discovery and menu population will happen automatically, as long
as the right naming conventions are followed and the docstrings are present.

# Inheritance

By creating your plugin as a subclass of Plugin, you will inherit the following
variables and functions.

## Variables

### Shorthand for the `databases` objects in the confg file
- `self.bucardo`
- `self.primary`
- `self.secondary`

### Some psycopg2 connection strings
- `self.bucardo_conn_pg_format`
- `self.bucardo_fallback_conn_pg_format `
- `self.primary_conn_pg_format`
- `self.secondary_db_owner_conn_pg_format`
- `self.secondary_schema_owner_conn_pg_format`

## Functions
See the docstrings for these functions.

- `self._set_inheritable_params(self, plugin_class)`
- `self._find_objects`

Despite the leading underscore, those "semi-private" Python methods are meant
to be exposed to developers. The leading underscore signals that they should
not be exposed to the end user.

# Configs

If you need to access settings in the config file, add the following to your
plugin's subclass. `MyPlugin` is the sample class name; replace with the
appropriate value for your plugin.

```python
def __init__(self, cfg):
    self.cfg = cfg
```
