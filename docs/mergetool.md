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

* * * * *

Blox Action Migration
----------------------

### `migrate_blox_actions(self, action_types=None, action="skip", emit=None)`

Migrates specific Blox actions from the source to the target environment. Each action is fetched from the source, transformed into a save-ready payload, and created (or replaced) on the target. Conflict detection is based on the action's `type` field. Saving and deleting Blox actions is Linux-only, so the target environment must be a Linux deployment.

#### Parameters:

-   `action_types` (list, optional): The `type` identifiers of the Blox actions to migrate. If omitted, every Blox action on the source is migrated.

-   `action` (str, optional): Conflict strategy for actions that already exist on the target:

    -   `"skip"` — leave the existing action unchanged (default).

    -   `"overwrite"` — delete the existing action on the target, then recreate from source.

    -   `"duplicate"` — always create, regardless of existing actions.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{type}`)
    -   `skipped` (list of `{type, reason}`)
    -   `failed` (list of `{type, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)

* * * * *

### `migrate_all_blox_actions(self, action="skip", emit=None)`

Migrates all Blox actions from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every action (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_blox_actions`.

* * * * *

Group Migration
----------------

### `migrate_groups(self, group_names=None, action="skip", emit=None)`

Migrates specific groups from the source to the target environment via the bulk group endpoint. Conflict detection is based on the group's `name` field.

#### Parameters:

-   `group_names` (list, optional): Group names to migrate. If omitted, every group on the source is migrated.

-   `action` (str, optional): Conflict strategy for groups that already exist on the target:

    -   `"skip"` — leave the existing group unchanged (default).

    -   `"overwrite"` — delete the existing group on the target, then recreate from source. **Warning:** this can disrupt user/group associations still referencing the deleted group on the target.

    -   `"duplicate"` — always create, regardless of existing groups.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{name}`)
    -   `skipped` (list of `{name, reason}`)
    -   `failed` (list of `{name, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)

* * * * *

### `migrate_all_groups(self, action="skip", emit=None)`

Migrates all eligible groups from the source to the target environment. Excludes the built-in `Admins`, `All users in system`, and `Everyone` groups, and — when the source environment exposes tenant information — restricts migration to groups belonging to the system tenant. If `/api/v1/tenants` is unavailable (single-tenant on-premises deployments), tenant-based filtering is skipped.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every group (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_groups`.

* * * * *

User Migration
---------------

### `migrate_users(self, user_emails=None, action="skip", ignore_custom_roles=False, emit=None)`

Migrates specific users from the source to the target environment via the bulk user endpoint. Resolves each user's role and group assignments to target environment IDs before creating them. Conflict detection is based on the user's `email` field.

Role resolution mirrors the legacy Win2Linux merge tool: when the target environment is multi-tenant (its role list includes `tenantAdmin`), source `super`/`admin` roles are mapped to `tenantAdmin`; when the target is a Windows deployment, source `tenantAdmin` is mapped back to `admin`.

#### Parameters:

-   `user_emails` (list, optional): Email addresses of the users to migrate. If omitted, every user on the source is migrated.

-   `action` (str, optional): Conflict strategy for users that already exist on the target:

    -   `"skip"` — leave the existing user unchanged (default).

    -   `"overwrite"` — delete the existing user on the target, then recreate from source.

    -   `"duplicate"` — always create, regardless of existing users.

-   `ignore_custom_roles` (bool, optional): When `True`, strips a `custom_` prefix from source role names before matching them against target roles (and matches target roles with the same prefix stripped). Default is `False`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{email}`)
    -   `skipped` (list of `{email, reason}`)
    -   `failed` (list of `{email, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)

* * * * *

### `migrate_all_users(self, action="skip", ignore_custom_roles=False, emit=None)`

Migrates all eligible users from the source to the target environment. Excludes users with the built-in `super` role (the source and target super admin accounts are expected to already exist independently on each environment), and — when the source environment exposes tenant information — restricts migration to users belonging to the system tenant. If tenant information is unavailable (single-tenant on-premises deployments), tenant-based filtering is skipped.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every user (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `ignore_custom_roles` (bool, optional): Same as in `migrate_users`. Default is `False`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_users`.

**Note:** Migrate groups before users — user payloads reference target group IDs, and groups not yet present on the target will be silently omitted from the user's group list.
