MergeTool Class Documentation
==============================

The `MergeTool` class migrates custom-code notebooks between two Sisense environments. It follows the same initialization pattern as `Migration` and supports skip, overwrite, and duplicate conflict strategies.

Initialization
--------------

Provide either two YAML config files or two pre-built `SisenseClient` instances.

### `__init__(self, source_yaml=None, target_yaml=None, debug=False, *, source_client=None, target_client=None)`

#### Parameters:

-   `source_yaml` (str, optional): Path to the YAML config file for the source environment.

-   `target_yaml` (str, optional): Path to the YAML config file for the target environment.

-   `debug` (bool, optional): Enable debug logging on newly created clients. Default is `False`.

-   `source_client` (SisenseClient, optional): Pre-built source client. Takes precedence over `source_yaml`.

-   `target_client` (SisenseClient, optional): Pre-built target client. Takes precedence over `target_yaml`.

* * * * *

Notebook Migration
------------------

### `migrate_notebooks(self, notebook_ids=None, notebook_names=None, action="skip", emit=None)`

Migrates specific custom-code notebooks from the source to the target environment. Each notebook is exported from the source and created (or replaced) on the target. Conflict detection is based on `displayName`.

#### Parameters:

-   `notebook_ids` (list, optional): Notebook IDs to migrate.

-   `notebook_names` (list, optional): Notebook display names to migrate.

-   `action` (str, optional): Conflict strategy for notebooks that already exist on the target:

    -   `"skip"` — leave the existing notebook unchanged (default).

    -   `"overwrite"` — delete the existing notebook on the target, then recreate from source.

    -   `"duplicate"` — always create, regardless of existing notebooks.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with `ok`, `status`, `succeeded`, `skipped`, `failed`, and counts.

* * * * *

### `migrate_all_notebooks(self, action="skip", emit=None)`

Migrates all custom-code notebooks from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every notebook (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_notebooks`.

* * * * *

Folder Migration
----------------

### `migrate_folders(self, folder_ids=None, folder_names=None, action="skip", emit=None)`

Migrates specific folders and their full subtrees from the source to the target environment. Resolves the requested root folders by OID or display name, expands each to its complete descendant tree, then recreates the hierarchy on the target in depth-first order (parents before children). Conflict detection is path-based (`parent/child` full path), so identically-named folders in different branches are handled independently.

Folders whose parent is not part of the migration are created at the root level on the target.

#### Parameters:

-   `folder_ids` (list, optional): Folder OIDs to migrate. Provide either this or `folder_names`.

-   `folder_names` (list, optional): Folder display names to migrate. Provide either this or `folder_ids`.

-   `action` (str, optional): Conflict strategy for folders that already exist on the target at the same path:

    -   `"skip"` — leave the existing folder unchanged and map its OID so child folders are still placed under it correctly (default).

    -   `"overwrite"` — delete the existing folder on the target, then recreate from source. **Warning:** deleting a folder on Sisense also removes all dashboards inside it.

    -   `"duplicate"` — always create, regardless of existing folders.

-   `emit` (callable, optional): Optional callback invoked with structured progress events. Each event is a `dict` with at least `type`, `step`, and `message` keys. `type` is one of `"started"`, `"progress"`, `"error"`, or `"completed"`.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{name, path, source_oid}`)
    -   `skipped` (list of `{name, path, source_oid, reason}`)
    -   `failed` (list of `{name, path, source_oid, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)

#### Raises:

-   `ValueError`: If both `folder_ids` and `folder_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_folders(self, action="skip", emit=None)`

Migrates all folders from the source to the target environment, preserving the full hierarchy.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every folder (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_folders`.
