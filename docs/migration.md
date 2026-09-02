Migration Class Documentation
=============================

The `Migration` class manages the migration of users, groups, dashboards, and data models between Sisense environments. This includes full support for selective and bulk migrations, share and ownership handling, and dependency resolution for data models.

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

Migrates specific groups from the source to the target environment.

#### Parameters:

-   `group_name_list` (list): List of group names to migrate.

#### Returns:

-   `list`: Group migration results, including any errors.

* * * * *

### `migrate_all_groups(self, emit=None)`

Migrates all groups from the source to the target environment.

#### Parameters:

-   `emit` (callable, optional): Optional callback invoked with structured progress events.

#### Returns:

-   `list`: Group migration results, including any errors.

* * * * *

### `migrate_users(self, user_name_list)`

Migrates specific users from the source to the target environment.

#### Parameters:

-   `user_name_list` (list): List of user names to migrate.

#### Returns:

-   `list`: User migration results, including any errors.

* * * * *

### `migrate_all_users(self, emit=None)`

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

