# Overview

The purpose of this plugin is to refresh materialized views on a secondary
database after a data sync. Bucardo will copy tables but not materialized
views. When the tables are populated onto the secondary database, the
materialized views will need to be specifically refreshed.

# Usage

Add `mat_views` to the list of plugins in the config file.

If you want the script to search for the materialized views in the namespaces
listed in the `replication_objects` list in the `bucardo` section of
the config file, you can leave that object unpopulated in the `mat_views` section.
If you want to search in different namespaces, you can override that list by
populating `replication_objects` in the `mat_views` section.

Run `python wrapper.py`.

Run `mat_views.refresh`.

# Functions

# refresh

The `refresh` function of the mat\_views plugin discovers the materialized
views in the namespaces specified either in the `bucardo` section (by default)
or in the `mat_views` section (for overriding) of the config file, then refreshes
them on the secondary.
