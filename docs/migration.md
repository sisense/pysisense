Migration Class Documentation
=============================

The `Migration` class manages the migration of users, groups, dashboards, and data models between Sisense environments. This includes full support for selective and bulk migrations, share and ownership handling, and dependency resolution for data models.

Initialization
--------------

## Example Configuration Files

To use the `Migration` class, you must provide two YAML files representing your source and target Sisense environments:

- [`examples/source.yaml`](../examples/source.yaml): Configuration for the **source** Sisense environment.
- [`examples/target.yaml`](../examples/target.yaml): Configuration for the **target** Sisense environment.

These files should follow the same structure as [`examples/config.yaml`](../examples/config.yaml), including:

```yaml
domain: "your-domain.sisense.com"
is_ssl: true
token: "<your_api_token>"
```

### `__init__(self, source_yaml, target_yaml, debug=False)`

Initializes the Migration class with API clients and Access Management for both source and target environments.

#### Parameters:

-   `source_yaml` (str): Path to the YAML file for source environment configuration.

-   `target_yaml` (str): Path to the YAML file for target environment configuration.

-   `debug` (bool, optional): Enables debug logging if `True`. Default is `False`.

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

### `migrate_all_groups(self)`

Migrates all groups from the source to the target environment.

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

### `migrate_all_users(self)`

Migrates all users from the source to the target environment.

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

### `migrate_dashboards(self, dashboard_ids=None, dashboard_names=None, action=None, republish=False, migrate_share=False, change_ownership=False)`

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

#### Returns:

-   `dict`: Summary with succeeded, skipped, and failed dashboard lists.

* * * * *

### `migrate_all_dashboards(self, action=None, republish=False, migrate_share=False, change_ownership=False, batch_size=10, sleep_time=10)`

Migrates all dashboards from the source to the target environment in batches.

#### Parameters:

-   `action` (str, optional): Behavior on existing dashboards (`skip`, `overwrite`, `duplicate`).

-   `republish` (bool, optional): Whether to republish dashboards. Default is `False`.

-   `migrate_share` (bool, optional): Whether to migrate shares. Default is `False`.

-   `change_ownership` (bool, optional): Whether to change ownership. Relevant only if shares are migrated.

-   `batch_size` (int, optional): Dashboards per batch. Default is `10`.

-   `sleep_time` (int, optional): Pause time (seconds) between batches. Default is `10`.

#### Returns:

-   `dict`: Batch summary with lists of succeeded, skipped, and failed dashboards.

* * * * *

Data Model Migration
--------------------

### `migrate_datamodels(self, datamodel_ids=None, datamodel_names=None, provider_connection_map=None, dependencies=None, shares=False, action=None, new_title=None)`

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

#### Returns:

-   `dict`: Summary of succeeded, skipped, failed data model migrations, and failure reasons if any.

* * * * *

### `migrate_all_datamodels(self, dependencies=None, shares=False, batch_size=10, sleep_time=5)`

Migrates all data models from the source to the target environment in batches.

#### Parameters:

-   `dependencies` (list, optional): Data model components to migrate. Options same as above.

-   `shares` (bool, optional): Whether to migrate shares. Default is `False`.

-   `batch_size` (int, optional): Models per batch. Default is `10`.

-   `sleep_time` (int, optional): Pause time (seconds) between batches. Default is `5`.

-   `action` (str, optional): Strategy to handle existing data models. Same behavior as in `migrate_datamodels`. When set to duplicate, appends " (Duplicate)" to each model title automatically.

#### Returns:

-   `dict`: Summary of succeeded, skipped, failed data model migrations with batch-level details.
