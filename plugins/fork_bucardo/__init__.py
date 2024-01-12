"""This module allows the user to adjust the open-source Bucardo code locally.

Bucardo is a third-party, open-source Postgres replication tool written by
Greg Mullane.  You can find the source code at
https://github.com/bucardo/bucardo.

Sometimes, their choices may not suit your use case. You can use this plugin to
run sed on a file or files and make the changes you need. This may require root
access.

Technically, this could be used to modify any file anywhere on the system, but
its anticipated use is Bucardo.

Classes exported:
ForkBucardo: Allow the user to make arbitrary changes to Bucardo via regexes.
"""

import subprocess

from plugins import Plugin


class ForkBucardo(Plugin):
    """Allow the user to make arbitrary changes to Bucardo via regexes.

    Methods exported:
    apply_changes: Loop over the changes in the config and apply them via sed.
    """

    def __init__(self, cfg):
        """Create aliases for configuration settings.

        Keyword arguments:
        cfg: contents of the config file as a dictionary
        """
        super(ForkBucardo, self).__init__(cfg)
        self.fork_cfg = cfg["fork_bucardo"]
        self.cfg = cfg

    def _update_file(self, filename, pattern, replacement):
        """Change a line of code in a file using sed."""

        try:
            subprocess.run(["sudo", "/bin/sed", "-i", f"s/{pattern}/{replacement}/", f"{filename}"], check=True)
        except subprocess.CalledProcessError:
            print(f"Could not modify {filename} due to a permission error. ")
            return

    def apply_changes(self):
        """Loop over the changes in the config and apply them as regular expressions."""
        for key in self.fork_cfg:
            filename = self.fork_cfg[key]["file"]
            pattern = self.fork_cfg[key]["pattern"]
            replacement = self.fork_cfg[key]["replacement"]
            self._update_file(filename, pattern, replacement)
            print(f"Change {key} applied. If bucardo is running, it must be restarted for the change to take effect.")
