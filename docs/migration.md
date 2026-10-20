Migration Class Documentation
=============================

The `Migration` class manages the migration of users, groups, dashboards, data models, custom-code notebooks, folders, Blox actions, data security rules, saved formulas, and saved filters between Sisense environments. This includes full support for selective and bulk migrations, share and ownership handling, and dependency resolution for data models.

Initialization
--------------

## Example Configuration Files

To use the `Migration` class, provide a config for each of your source and target Sisense environments. A config is anything `SisenseClient` accepts: a YAML file, a JSON file, or a Python dict with the same keys.

- [`examples/source.yaml`](../examples/source.yaml): Configuration for the **source** Sisense environment.
- [`examples/target.yaml`](../examples/target.yaml): Configuration for the **target** Sisense environment.
- [`examples/config.json`](../examples/config.json): The same structure as a JSON file.

These files should follow the same structure as [`examples/config.yaml`](../examples/config.yaml), including:

```yaml
domain: "your-domain.sisense.com"
is_ssl: true
token: "<your_api_token>"
```

```python
migration = Migration(source_config="source.yaml", target_config="target.json")
migration = Migration(
    source_config={"domain": "src.example.com", "token": "<src_token>"},
    target_config={"domain": "tgt.example.com", "token": "<tgt_token>"},
)
```

### `__init__(self, source_yaml=None, target_yaml=None, debug=False, *, source_client=None, target_client=None, source_config=None, target_config=None)`

Initializes the Migration class with API clients for both source and target environments. Provide either two configs or two pre-built `SisenseClient` instances.

#### Parameters:

-   `source_config` (str | os.PathLike | dict, optional): Config for the source environment: a `.yaml`/`.yml` or `.json` file path, or a dict with the same keys.

-   `target_config` (str | os.PathLike | dict, optional): Config for the target environment, in the same forms.

-   `source_yaml`, `target_yaml` (optional): Aliases for `source_config` / `target_config`, kept for backward compatibility. They accept the same forms.

-   `source_client`, `target_client` (SisenseClient, optional): Pre-built clients. Take precedence over the configs.

-   `debug` (bool, optional): Enables debug logging on newly created clients. Default is `False`.

* * * * *

Group and User Migration
------------------------

### `migrate_groups(self, group_name_list)`

**Deprecated:** use `migrate_groups_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates specific groups from the source to the target environment.

#### Parameters:

-   `group_name_list` (list): List of group names to migrate.

#### Returns:

-   `list`: Group migration results, including any errors.

* * * * *

### `migrate_all_groups(self, emit=None)`

**Deprecated:** use `migrate_all_groups_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates all groups from the source to the target environment.

#### Parameters:

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `list`: Group migration results, including any errors.

* * * * *

### `migrate_users(self, user_name_list)`

**Deprecated:** use `migrate_users_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates specific users from the source to the target environment.

#### Parameters:

-   `user_name_list` (list): List of user names to migrate.

#### Returns:

-   `list`: User migration results, including any errors.

* * * * *

### `migrate_all_users(self, emit=None)`

**Deprecated:** use `migrate_all_users_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates all users from the source to the target environment.

#### Parameters:

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `list`: User migration results, including any errors.

* * * * *

Dashboard Migration
-------------------

### `migrate_dashboard_shares(self, source_dashboard_ids, target_dashboard_ids, change_ownership=False)`

Migrates dashboard shares from the source to the target environment.

#### Parameters:

-   `source_dashboard_ids` (list): Dashboard IDs to fetch shares from.

-   `target_dashboard_ids` (list): Dashboard IDs to apply shares to.

-   `change_ownership` (bool, optional): Whether to transfer dashboard ownership. Default is `False`.

#### Returns:

-   `dict`: Summary of the share migration, including success and failure counts.

* * * * *

### `migrate_dashboards(self, dashboard_ids=None, dashboard_names=None, action=None, republish=False, migrate_share=False, change_ownership=False, emit=None)`

**Deprecated:** use `migrate_dashboards_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates specific dashboards with optional republishing, ownership transfer, and share migration.

#### Parameters:

-   `dashboard_ids` (list, optional): Dashboard IDs to migrate.

-   `dashboard_names` (list, optional): Dashboard names to migrate.

-   `action` (str, optional): Behavior on existing dashboards. Options:

    -   `skip`

    -   `overwrite`

    -   `duplicate`

-   `republish` (bool, optional): Whether to republish dashboards after migration. Default is `False`.

-   `migrate_share` (bool, optional): Whether to migrate shares. Default is `False`.

-   `change_ownership` (bool, optional): Whether to transfer ownership. Only relevant if `migrate_share` is `True`. Default is `False`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with succeeded, skipped, and failed dashboard lists.

* * * * *

### `migrate_all_dashboards(self, action=None, republish=False, migrate_share=False, change_ownership=False, batch_size=10, sleep_time=10, emit=None)`

**Deprecated:** use `migrate_all_dashboards_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates all dashboards from the source to the target environment in batches.

#### Parameters:

-   `action` (str, optional): Behavior on existing dashboards (`skip`, `overwrite`, `duplicate`).

-   `republish` (bool, optional): Whether to republish dashboards. Default is `False`.

-   `migrate_share` (bool, optional): Whether to migrate shares. Default is `False`.

-   `change_ownership` (bool, optional): Whether to change ownership. Relevant only if shares are migrated.

-   `batch_size` (int, optional): Dashboards per batch. Default is `10`.

-   `sleep_time` (int, optional): Pause time (seconds) between batches. Default is `10`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Batch summary with lists of succeeded, skipped, and failed dashboards.

* * * * *

Data Model Migration
--------------------

### `migrate_datamodels(self, datamodel_ids=None, datamodel_names=None, provider_connection_map=None, dependencies=None, shares=False, action=None, new_title=None, emit=None)`

**Deprecated:** use `migrate_datamodels_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates specific data models with support for dependencies and shares.

#### Parameters:

-   `datamodel_ids` (list, optional): DataModel IDs to migrate.

-   `datamodel_names` (list, optional): DataModel names to migrate.

-   `provider_connection_map` (dict, optional): Mapping of provider names to connection IDs.

-   `dependencies` (list, optional): Data model components to migrate. Options:

    -   `dataSecurity`

    -   `formulas`

    -   `hierarchies`

    -   `perspectives`

    -   or `all`

-   `shares` (bool, optional): Whether to migrate shares. Default is `False`.

-   `action` (str, optional): Strategy to handle existing data models in the target environment.

    -   `overwrite`: Attempts to overwrite an existing model using its original ID via the datamodelId parameter. If the model is not found in the target environment, it will automatically fall back and create the model.

    -    `duplicate`: Creates a new model by passing a new_title to the newTitle parameter of the import API endpoint. If new_title is not provided, appends " (Duplicate)" to the original name.

-   `new_title` (str, optional): New name for the duplicated data model. Used only when action='duplicate'.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary of succeeded, skipped, failed data model migrations, and failure reasons if any.

* * * * *

### `migrate_all_datamodels(self, dependencies=None, shares=False, batch_size=10, sleep_time=5, emit=None)`

**Deprecated:** use `migrate_all_datamodels_v2`, which takes different parameters and returns the standard result. Removed in the next major release.

Migrates all data models from the source to the target environment in batches.

#### Parameters:

-   `dependencies` (list, optional): Data model components to migrate. Options same as above.

-   `shares` (bool, optional): Whether to migrate shares. Default is `False`.

-   `batch_size` (int, optional): Models per batch. Default is `10`.

-   `sleep_time` (int, optional): Pause time (seconds) between batches. Default is `5`.

-   `action` (str, optional): Strategy to handle existing data models. Same behavior as in `migrate_datamodels`. When set to duplicate, appends " (Duplicate)" to each model title automatically.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary of succeeded, skipped, failed data model migrations with batch-level details.

* * * * *

Notebook Migration
------------------

### `migrate_notebooks(self, notebook_ids=None, notebook_names=None, action="skip", concurrency=1, emit=None)`

Migrates specific custom-code notebooks from the source to the target environment. Each notebook is exported from the source and created (or replaced) on the target. Conflict detection is based on `displayName`.

#### Parameters:

-   `notebook_ids` (list, optional): Notebook IDs to migrate.

-   `notebook_names` (list, optional): Notebook display names to migrate.

-   `action` (str, optional): Conflict strategy for notebooks that already exist on the target:

    -   `"skip"` — leave the existing notebook unchanged (default).

    -   `"overwrite"` — delete the existing notebook on the target, then recreate from source.

    -   `"duplicate"` — always create, regardless of existing notebooks.

-   `concurrency` (int, optional): Maximum number of notebooks to migrate concurrently, run via a background thread pool (`asyncio.to_thread`) since the underlying HTTP client is synchronous. Notebooks are independent of each other, so any value is safe. Default is `1` (sequential). If called from code that's already running an asyncio event loop, values `> 1` fall back to sequential processing.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with `ok`, `status`, `succeeded`, `skipped`, `failed`, and counts.

* * * * *

### `migrate_all_notebooks(self, action="skip", concurrency=1, emit=None)`

Migrates all custom-code notebooks from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every notebook (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `concurrency` (int, optional): Same as in `migrate_notebooks`. Default is `1` (sequential).

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_notebooks`.

* * * * *

Folder Migration
----------------

### `migrate_folders(self, folder_ids=None, folder_names=None, action="skip", concurrency=1, emit=None)`

Migrates specific folders and their full subtrees from the source to the target environment. Resolves the requested root folders by OID or display name, expands each to its complete descendant tree, then recreates the hierarchy on the target in depth-first order (parents before children). Conflict detection is path-based (`parent/child` full path), so identically-named folders in different branches are handled independently.

Folders whose parent is not part of the migration are created at the root level on the target.

#### Parameters:

-   `folder_ids` (list, optional): Folder OIDs to migrate. Provide either this or `folder_names`.

-   `folder_names` (list, optional): Folder display names to migrate. Provide either this or `folder_ids`.

-   `action` (str, optional): Conflict strategy for folders that already exist on the target at the same path:

    -   `"skip"` — leave the existing folder unchanged and map its OID so child folders are still placed under it correctly (default).

    -   `"overwrite"` — delete the existing folder on the target, then recreate from source. **Warning:** deleting a folder on Sisense also removes all dashboards inside it.

    -   `"duplicate"` — always create, regardless of existing folders.

-   `concurrency` (int, optional): Maximum number of folders to create/delete concurrently within each hierarchy depth level, run via a background thread pool (`asyncio.to_thread`) since the underlying HTTP client is synchronous. Folders at the same depth are independent of each other — their parent was already migrated in an earlier, fully-completed depth level — so they're safe to run in parallel; different depths are still processed strictly in order. Values `<= 1` (default) process folders one at a time. If called from code that's already running an asyncio event loop, values `> 1` fall back to sequential processing for the affected depth level.

-   `emit` (callable, optional): Optional callback invoked with structured progress events. Each event is a `dict` with at least `type`, `step`, and `message` keys. `type` is one of `"started"`, `"progress"`, `"error"`, or `"completed"`. When `concurrency` is greater than 1, this callback may be invoked from multiple worker threads concurrently for folders in the same depth level.

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

### `migrate_all_folders(self, action="skip", concurrency=1, emit=None)`

Migrates all folders from the source to the target environment, preserving the full hierarchy.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every folder (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `concurrency` (int, optional): Same as in `migrate_folders`. Default is `1` (sequential).

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_folders`.

* * * * *

Blox Action Migration
----------------------

### `migrate_blox_actions(self, action_types=None, action="skip", concurrency=1, emit=None)`

Migrates specific Blox actions from the source to the target environment. Each action is fetched from the source, transformed into a save-ready payload, and created (or replaced) on the target. Conflict detection is based on the action's `type` field. Saving and deleting Blox actions is Linux-only, so the target environment must be a Linux deployment.

#### Parameters:

-   `action_types` (list, optional): The `type` identifiers of the Blox actions to migrate. If omitted, every Blox action on the source is migrated.

-   `action` (str, optional): Conflict strategy for actions that already exist on the target:

    -   `"skip"` — leave the existing action unchanged (default).

    -   `"overwrite"` — delete the existing action on the target, then recreate from source.

    -   `"duplicate"` — always create, regardless of existing actions.

-   `concurrency` (int, optional): Maximum number of Blox actions to migrate concurrently, run via a background thread pool (`asyncio.to_thread`) since the underlying HTTP client is synchronous. Blox actions are independent of each other, so any value is safe. Default is `1` (sequential). If called from code that's already running an asyncio event loop, values `> 1` fall back to sequential processing.

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

### `migrate_all_blox_actions(self, action="skip", concurrency=1, emit=None)`

Migrates all Blox actions from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every action (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `concurrency` (int, optional): Same as in `migrate_blox_actions`. Default is `1` (sequential).

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_blox_actions`.

* * * * *

Data Security Migration
-------------------------

### `migrate_datasecurity(self, datamodel_ids=None, datamodel_names=None, emit=None)`

Migrates row-level datasecurity rules for specific data models from the source to the target environment. Requires the target data model to already exist — run `migrate_datamodels_v2`/`migrate_all_datamodels_v2` first. Each rule's raw datasecurity payload is fetched from the source, its `shares` are remapped to target user/group ids (matched by email for users and by name for groups), and the resolved rules are written onto the target data model via `update_datasecurity` (EXTRACT) or `set_live_datasecurity_add_many` (LIVE).

#### Parameters:

-   `datamodel_ids` (list, optional): Data model OIDs to migrate. Provide either this or `datamodel_names`.

-   `datamodel_names` (list, optional): Data model titles to migrate. Provide either this or `datamodel_ids`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{title, source_oid, rule_count}`)
    -   `skipped` (list of `{title, source_oid, reason}`)
    -   `failed` (list of `{title, source_oid, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)

#### Raises:

-   `ValueError`: If both `datamodel_ids` and `datamodel_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_datasecurity(self, emit=None)`

Migrates datasecurity rules for all data models from the source to the target environment. Data models not yet present on the target are skipped.

#### Parameters:

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_datasecurity`.

**Note:** Migrate data models before data security — datasecurity rules can only be written onto a data model that already exists on the target.

* * * * *

Saved Formula Migration
-------------------------

### `migrate_saved_formulas(self, datamodel_ids=None, datamodel_names=None, action="skip", emit=None)`

Migrates saved formula measures for specific data models from the source to the target environment. Requires the target data model to already exist — run `migrate_datamodels_v2`/`migrate_all_datamodels_v2` first. Each formula is fetched from the source via the metadata measures endpoint and created on the target. Conflict detection is based on the formula's `title`.

#### Parameters:

-   `datamodel_ids` (list, optional): Data model OIDs to migrate. Provide either this or `datamodel_names`.

-   `datamodel_names` (list, optional): Data model titles to migrate. Provide either this or `datamodel_ids`.

-   `action` (str, optional): Conflict strategy for formulas whose `title` already exists on the target datasource:

    -   `"skip"` — leave the existing formula unchanged (default).

    -   `"overwrite"` or `"duplicate"` — always create, regardless of conflicts. The Sisense metadata API exposes no update or delete endpoint for saved formulas, so `"overwrite"` cannot replace the existing formula in place and behaves identically to `"duplicate"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{datamodel, formula}`)
    -   `skipped` (list of `{datamodel, formula, reason}`)
    -   `failed` (list of `{datamodel, formula, reason}`)
    -   `source_count` (total saved formulas found across the resolved data models), `succeeded_count`, `skipped_count`, `failed_count` (int)

#### Raises:

-   `ValueError`: If both `datamodel_ids` and `datamodel_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_saved_formulas(self, action="skip", emit=None)`

Migrates saved formula measures for all data models from the source to the target environment. Data models not yet present on the target are skipped.

#### Parameters:

-   `action` (str, optional): Same as in `migrate_saved_formulas`. Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_saved_formulas`.

* * * * *

Saved Filter Migration
------------------------

### `migrate_saved_filters(self, datamodel_ids=None, datamodel_names=None, action="skip", emit=None)`

Migrates saved filter dimensions for specific data models from the source to the target environment. Requires the target data model to already exist — run `migrate_datamodels_v2`/`migrate_all_datamodels_v2` first. Each data model's dimensions are fetched from the source, filtered down to the ones carrying a saved filter definition (a truthy `filter` key), and recreated on the target via a raw metadata query. Conflict detection is based on the filter's `title`.

#### Parameters:

-   `datamodel_ids` (list, optional): Data model OIDs to migrate. Provide either this or `datamodel_names`.

-   `datamodel_names` (list, optional): Data model titles to migrate. Provide either this or `datamodel_ids`.

-   `action` (str, optional): Conflict strategy for filters whose `title` already exists on the target datasource:

    -   `"skip"` — leave the existing filter unchanged (default).

    -   `"overwrite"` or `"duplicate"` — always create, regardless of conflicts. The Sisense metadata API exposes no update or delete endpoint for saved filters, so `"overwrite"` cannot replace the existing filter in place and behaves identically to `"duplicate"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{datamodel, filter}`)
    -   `skipped` (list of `{datamodel, filter, reason}`)
    -   `failed` (list of `{datamodel, filter, reason}`)
    -   `source_count` (total saved filters found across the resolved data models), `succeeded_count`, `skipped_count`, `failed_count` (int)

#### Raises:

-   `ValueError`: If both `datamodel_ids` and `datamodel_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_saved_filters(self, action="skip", emit=None)`

Migrates saved filter dimensions for all data models from the source to the target environment. Data models not yet present on the target are skipped.

#### Parameters:

-   `action` (str, optional): Same as in `migrate_saved_filters`. Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_saved_filters`.

* * * * *

Migration methods (v2)
======================

The `*_v2` methods replace `migrate_groups`, `migrate_users`, `migrate_dashboards`, `migrate_datamodels` and their `migrate_all_*` versions. They check the target before writing (`action="skip"` by default), handle each item on its own, and return the standard migration result: `ok`, `status`, `succeeded`, `skipped`, `failed` and the counts. In the next major release the old methods are removed and the `*_v2` methods take the plain names.

Migrate in this order: groups, users, folders, data models, then dashboards.

Group Migration (v2)
--------------------

### `migrate_groups_v2(self, group_names=None, action="skip", emit=None)`

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
    -   `error` (str): only when the run could not start

* * * * *

### `migrate_all_groups_v2(self, action="skip", emit=None)`

Migrates all eligible groups from the source to the target environment. Excludes the built-in `Admins`, `All users in system`, and `Everyone` groups, and — when the source environment exposes tenant information — restricts migration to groups belonging to the system tenant. If `/api/v1/tenants` is unavailable (single-tenant on-premises deployments), tenant-based filtering is skipped.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every group (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_groups_v2`.

* * * * *

User Migration (v2)
-------------------

### `migrate_users_v2(self, user_emails=None, action="skip", ignore_custom_roles=False, emit=None)`

Migrates specific users from the source to the target environment via the bulk user endpoint. Resolves each user's role and group assignments to target environment IDs before creating them. Conflict detection is based on the user's `email` field.

Role names are adapted to the target before matching: when the target environment is multi-tenant (its role list includes `tenantAdmin`), source `super`/`admin` roles are mapped to `tenantAdmin`; when the target is a Windows deployment, source `tenantAdmin` is mapped back to `admin`.

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
    -   `error` (str): only when the run could not start

* * * * *

### `migrate_all_users_v2(self, action="skip", ignore_custom_roles=False, emit=None)`

Migrates all eligible users from the source to the target environment. Excludes users with the built-in `super` role (the source and target super admin accounts are expected to already exist independently on each environment), and — when the source environment exposes tenant information — restricts migration to users belonging to the system tenant. If tenant information is unavailable (single-tenant on-premises deployments), tenant-based filtering is skipped.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every user (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `ignore_custom_roles` (bool, optional): Same as in `migrate_users_v2`. Default is `False`.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_users_v2`.

**Note:** Migrate groups before users — user payloads reference target group IDs, and groups not yet present on the target will be silently omitted from the user's group list.

* * * * *

Data Model Migration (v2)
-------------------------

### `migrate_datamodels_v2(self, datamodel_ids=None, datamodel_names=None, action="skip", dependencies=None, provider_connection_map=None, shares=False, concurrency=1, emit=None, new_title=None)`

Migrates specific data models from the source to the target environment. Each data model's schema is exported from the source and imported into the target via the schema import endpoint. Conflict detection is based on the data model's `title`. Embedded connection credentials are repointed via `provider_connection_map` when a matching provider entry is supplied, and stripped otherwise — they must be re-entered (or reconnected) on the target after migration.

#### Parameters:

-   `datamodel_ids` (list, optional): Data model OIDs to migrate. Provide either this or `datamodel_names`.

-   `datamodel_names` (list, optional): Data model titles to migrate. Provide either this or `datamodel_ids`.

-   `action` (str, optional): Conflict strategy for data models whose `title` already exists on the target:

    -   `"skip"` — leave the existing data model unchanged (default).

    -   `"overwrite"` — replace the existing data model's schema with the source version, matched by the target model's own OID.

    -   `"duplicate"` — always create a new data model titled `"<title> (Duplicate)"`, regardless of existing data models.

-   `dependencies` (list or str, optional): One or more of `"dataSecurity"`, `"formulas"`, `"hierarchies"`, `"perspectives"` to include in the export. Defaults to all of them when omitted or `"all"`. Windows: has no effect when the source is a Windows deployment, its export endpoint accepts no dependencies parameter.

-   `provider_connection_map` (dict, optional): Maps a connection provider name (for example `"Athena"`) to a target-environment connection OID.

-   `shares` (bool, optional): Whether to also migrate each data model's shares after a successful import, remapping users and groups by email/name. Default is `False`.

-   `concurrency` (int, optional): Maximum number of data models to migrate concurrently, run via a background thread pool (`asyncio.to_thread`) since the underlying HTTP client is synchronous. Data models are independent of each other, so any value is safe. Default is `1` (sequential). If called from code that's already running an asyncio event loop, values `> 1` fall back to sequential processing.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

-   `new_title` (str, optional): Title for the new data model when `action="duplicate"`, instead of `"<title> (Duplicate)"`. Ignored for other actions. Every selected model gets this title, so use it with one model.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{title, source_oid, target_id}`)
    -   `skipped` (list of `{title, source_oid, reason}`)
    -   `failed` (list of `{title, source_oid, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)
    -   `error` (str): only when the run could not start

#### Raises:

-   `ValueError`: If both `datamodel_ids` and `datamodel_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_datamodels_v2(self, action="skip", dependencies=None, provider_connection_map=None, shares=False, concurrency=1, emit=None)`

Migrates all data models from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every data model (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `dependencies` (list or str, optional): Same as in `migrate_datamodels_v2`.

-   `provider_connection_map` (dict, optional): Same as in `migrate_datamodels_v2`.

-   `shares` (bool, optional): Same as in `migrate_datamodels_v2`.

-   `concurrency` (int, optional): Same as in `migrate_datamodels_v2`. Default is `1` (sequential).

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `dict`: Same structure as `migrate_datamodels_v2`.

**Note:** Migrate data models before dashboards — dashboards reference the underlying data model's local Elasticube.

* * * * *

Dashboard Migration (v2)
------------------------

### `migrate_dashboards_v2(self, dashboard_ids=None, dashboard_names=None, action="skip", concurrency=1, emit=None, republish=False, migrate_share=True, change_ownership=True)`

Migrates specific dashboards from the source to the target environment. Each dashboard is exported from the source, has its embedded datasource references repointed to the target's local Elasticube, and is imported into the target via the bulk import endpoint, which matches dashboards by `oid` and applies `action` natively. After a successful import, the dashboard's owner and shares are remapped to target users/groups (matched by email/name), and the dashboard is moved into the target folder whose path matches its source parent folder — if that folder has already been migrated with `migrate_folders`.

#### Parameters:

-   `dashboard_ids` (list, optional): Dashboard OIDs to migrate. Provide either this or `dashboard_names`.

-   `dashboard_names` (list, optional): Dashboard titles to migrate. Provide either this or `dashboard_ids`.

-   `action` (str, optional): Conflict strategy for dashboards whose `oid` already exists on the target:

    -   `"skip"` — leave the existing dashboard unchanged (default).

    -   `"overwrite"` — replace the existing dashboard with the source version.

    -   `"duplicate"` — always create, regardless of existing dashboards.

-   `concurrency` (int, optional): Maximum number of dashboards to migrate concurrently, run via a background thread pool (`asyncio.to_thread`) since the underlying HTTP client is synchronous. Dashboards are independent of each other, so any value is safe. Default is `1` (sequential). If called from code that's already running an asyncio event loop, values `> 1` fall back to sequential processing.

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

-   `republish` (bool, optional): Republish each imported dashboard to the users it is shared with (`republish=true` on the bulk import request). Default is `False`.

-   `migrate_share` (bool, optional): Recreate the dashboard's shares on the target, matching users by email and groups by name. `False` leaves the imported dashboard unshared. Default is `True`.

-   `change_ownership` (bool, optional): Give the target dashboard the same owner as on the source, matched by email. `False` leaves the token user as owner. Default is `True`.

#### Returns:

-   `dict`: Summary with:
    -   `ok` (bool)
    -   `status` (`"success"` | `"failed"` | `"noop"`)
    -   `succeeded` (list of `{title, oid, source_oid}`)
    -   `skipped` (list of `{title, source_oid, reason}`)
    -   `failed` (list of `{title, source_oid, reason}`)
    -   `source_count`, `succeeded_count`, `skipped_count`, `failed_count` (int)
    -   `error` (str): only when the run could not start

#### Raises:

-   `ValueError`: If both `dashboard_ids` and `dashboard_names` are provided, or if neither is provided.

* * * * *

### `migrate_all_dashboards_v2(self, action="skip", concurrency=1, emit=None, republish=False, migrate_share=True, change_ownership=True)`

Migrates all dashboards from the source to the target environment.

#### Parameters:

-   `action` (str, optional): Conflict strategy applied to every dashboard (`"skip"`, `"overwrite"`, `"duplicate"`). Default is `"skip"`.

-   `concurrency` (int, optional): Same as in `migrate_dashboards_v2`. Default is `1` (sequential).

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

-   `republish`, `migrate_share`, `change_ownership` (bool, optional): Same as in `migrate_dashboards_v2`.

#### Returns:

-   `dict`: Same structure as `migrate_dashboards_v2`.

**Note:** Migrate groups, users, and folders before dashboards — dashboard owner/share remapping and folder placement depend on all three having already been migrated.
