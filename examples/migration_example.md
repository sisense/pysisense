# Migration Example Usage

This guide demonstrates how to use the `Migration` class from the `pysisense` package. Each example below includes a description and a code snippet for easy reference and copying.

---

## Prerequisites

- Ensure `source.yaml` and `target.yaml` are in the same folder as your script.
- Import required modules and initialize the Migration class.

```python
import sys
import os
import json

# For local development only — not needed after pip install
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pysisense import Migration

source_yaml_path = os.path.join(os.path.dirname(__file__), "source.yaml")
target_yaml_path = os.path.join(os.path.dirname(__file__), "target.yaml")

migration = Migration(source_yaml=source_yaml_path, target_yaml=target_yaml_path, debug=False)
```

---

## Example 1: Migrate Specific Groups

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_groups_v2` (Examples 26–37).

Migrate specific groups from source to target.

```python
group_name_list = ["mig_test", "mig_test_2"]
migration_results = migration.migrate_groups(group_name_list)
print(json.dumps(migration_results, indent=4))
```

---

## Example 2: Migrate All Groups

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_all_groups_v2` (Examples 26–37).

Migrate all groups from source to target.

```python
migration_results = migration.migrate_all_groups()
print(json.dumps(migration_results, indent=4))
```

---

## Example 3: Migrate Specific Users

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_users_v2` (Examples 26–37).

Migrate specific users from source to target.

```python
user_name = ["john.doe@sisense.com"]
migration_results = migration.migrate_users(user_name)
print(json.dumps(migration_results, indent=4))
```

---

## Example 4: Migrate All Users

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_all_users_v2` (Examples 26–37).

Migrate all users from source to target.

```python
migration_results = migration.migrate_all_users()
print(json.dumps(migration_results, indent=4))
```

---

## Example 5: Migrate Dashboard Shares

Migrate shares for specific dashboards from source to target.

```python
source_dashboard_ids = ["659583469a933c002adc8574", "6261702faae68e0036b1572b"]
target_dashboard_ids = ["659583469a933c002adc8574", "6261702faae68e0036b1572b"]
share_migration_results = migration.migrate_dashboard_shares(
    source_dashboard_ids=source_dashboard_ids,
    target_dashboard_ids=target_dashboard_ids,
    change_ownership=True,
)
print(json.dumps(share_migration_results, indent=4))
```

---

## Example 6: Migrate Specific Dashboards

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_dashboards_v2` (Examples 26–37).

Migrate dashboards by name or ID.

**By Name:**
```python
dashboard_name_list = ["Export to PDF text widget", "samp_lead_gen_2"]
migration_results_by_name = migration.migrate_dashboards(
    dashboard_names=dashboard_name_list,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    republish=True,  # Republishes dashboards after migration
    migrate_share=True,  # Migrate shares for the dashboards
    change_ownership=True,  # Change ownership of dashboards (requires migrate_share=True)
)
print("Migration results using dashboard names:")
print(json.dumps(migration_results_by_name, indent=4))
```

**By ID:**
```python
dashboard_id_list = ["659583469a933c002adc8574", "659abcde9a933c002adc1234", "6261702faae68e0036b1572b"]
migration_results_by_id = migration.migrate_dashboards(
    dashboard_ids=dashboard_id_list,
    action="overwrite",  # Options: "skip", "overwrite", "duplicate"
    republish=False,  # Don't republish in this case
    migrate_share=True,
    change_ownership=False,  # Only migrate shares, don't change ownership
)
print("\nMigration results using dashboard IDs:")
print(json.dumps(migration_results_by_id, indent=4))
```

---

## Example 7: Migrate All Dashboards in Batches

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_all_dashboards_v2` (Examples 26–37).

Migrate all dashboards from source to target environment in batches.

```python
migration_results = migration.migrate_all_dashboards(
    # action="overwrite",                                                        # Options: "skip", "overwrite", "duplicate".
    republish=True,  # Republishes dashboards after migration
    migrate_share=True,  # Migrate shares for the dashboards
    change_ownership=True,  # Change ownership of dashboards (requires migrate_share=True)
    batch_size=10,  # Process 10 dashboards at a time
    sleep_time=10,  # Wait for 10 seconds between processing each batch
)
print(json.dumps(migration_results, indent=4))
```

---

## Example 8: Migrate DataModels

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_datamodels_v2` (Examples 26–37).

Migrate one or more datamodels from source to target.

**By IDs:**
```python
datamodel_ids = ["dddfac9f-86e6-4a9a-af28-f52106dcc55c", "97a41a88-3d8e-491d-8fe6-9eb111d86c57"]
migration_results = migration.migrate_datamodels(
    datamodel_ids=datamodel_ids,
    dependencies=["dataSecurity", "formulas"],  # Optional. Choose from: dataSecurity, formulas, hierarchies, perspectives
    shares=True,  # Optional. If True, also migrates the model's shares
)
print(json.dumps(migration_results, indent=4))
```

**By Names:**
```python
datamodel_names = ["pysense_databricks", "pysense_databricks_ec"]
migration_results = migration.migrate_datamodels(
    datamodel_names=datamodel_names,
    dependencies=["hierarchies", "perspectives"],  # Optional. Select only needed dependencies
    shares=False,  # Skip share migration in this case
)
print(json.dumps(migration_results, indent=4))
```

**Migrate data models and update the data source connections in the target environment using Connection Mapping:**
```python
datamodel_names = ["pysense_databricks", "pysense_databricks_ec"]

# Map original provider names to new connection IDs in the target (e.g., when migrating from dev to prod)
provider_connection_map = {"Databricks": "53874a46-1360-45d8-a005-1cca41ef3e1c", "GoogleBigQuery": "5550c482-4e52-4b0f-afff-865e964c0df5"}
migration_results = migration.migrate_datamodels(datamodel_names=datamodel_names, provider_connection_map=provider_connection_map, dependencies="all", shares=False)
print(json.dumps(migration_results, indent=4))
```

**Overwrite Existing Models:**
```python
datamodel_ids = ["a42107d7-b4e6-4679-b11f-da580978d1ee", "dddfac9f-86e6-4a9a-af28-f52106dcc55c"]
migration_results = migration.migrate_datamodels(
    datamodel_ids=datamodel_ids,
    action="overwrite",  # Overwrites existing model in target using its ID
    dependencies="all",
    shares=True,
)
print(json.dumps(migration_results, indent=4))
```

**Duplicate Model with New Name:**
```python
datamodel_names = ["pysense_databricks"]
migration_results = migration.migrate_datamodels(
    datamodel_names=datamodel_names,
    action="duplicate",  # Create a new model in target
    new_title="",  # Optional: Custom title; if not set, adds " (Duplicate)" to original name
    dependencies=["formulas", "perspectives"],
    shares=False,
)
print(json.dumps(migration_results, indent=4))
```

---

## Example 9: Migrate All DataModels in Batches

> **Deprecated:** this method warns and is removed in the next major release. Use `migrate_all_datamodels_v2` (Examples 26–37).

Migrate all datamodels from source to target environment in batches.

```python
dependencies = ["dataSecurity", "formulas"]  # Only migrate selected dependencies
shares = True  # Migrate shares along with the data models
batch_size = 10  # Process 5 data models per batch
sleep_time = 10  # Wait 10 seconds between batches

migration_summary = migration.migrate_all_datamodels(
    dependencies=dependencies,
    shares=shares,
    batch_size=batch_size,
    sleep_time=sleep_time,
    action="overwrite",  # Options: "overwrite", "duplicate". For "duplicate", a new model is created in the target with the same name as the source model, but with " (Duplicate)" appended to it.
)
print(json.dumps(migration_summary, indent=4))
```

---

## Example 10: Migrate Specific Notebooks by ID

```python
notebook_ids = [
    "notebook-id-1",
    "notebook-id-2",
]
results = migration.migrate_notebooks(
    notebook_ids=notebook_ids,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 11: Migrate Specific Notebooks by Name

```python
notebook_names = [
    "My Transformation Notebook",
    "ETL Pipeline",
]
results = migration.migrate_notebooks(
    notebook_names=notebook_names,
    action="overwrite",  # Deletes existing on target then recreates from source
)
print(json.dumps(results, indent=4))
```

---

## Example 12: Migrate All Notebooks

```python
results = migration.migrate_all_notebooks(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    concurrency=5,  # Notebooks are independent — migrate up to 5 at once
)
print(json.dumps(results, indent=4))
```

---

## Example 13: Migrate Specific Folders by Name (with subtree)

```python
folder_names = [
    "Analytics",
    "Finance Reports",
]
results = migration.migrate_folders(
    folder_names=folder_names,
    action="skip",  # Child folders are included automatically
)
print(json.dumps(results, indent=4))
```

---

## Example 14: Migrate Specific Folders by ID (with subtree)

```python
folder_ids = [
    "folder-oid-1",
    "folder-oid-2",
]
results = migration.migrate_folders(
    folder_ids=folder_ids,
    action="overwrite",  # Deletes existing folder on target then recreates
)
print(json.dumps(results, indent=4))
```

---

## Example 15: Migrate All Folders

```python
results = migration.migrate_all_folders(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 16: Migrate Folders Concurrently

Folders in the same hierarchy depth level are independent of each other (their parent was already migrated in the previous level), so they can be created concurrently. Different depths are still processed strictly in order.

```python
results = migration.migrate_all_folders(
    action="skip",
    concurrency=5,  # Up to 5 folders per depth level created/deleted at once
)
print(json.dumps(results, indent=4))
```

---

## Example 17: Using an emit callback for progress tracking

```python
def on_progress(event: dict) -> None:
    print(f"[{event.get('type', '').upper()}] {event.get('step')} — {event.get('message')}")


results = migration.migrate_all_folders(action="skip", emit=on_progress)
print(json.dumps(results, indent=4))
```

---

## Example 18: Migrate Specific Blox Actions by Type

```python
action_types = [
    "Send Email Notification",
    "Refresh Dashboard",
]
results = migration.migrate_blox_actions(
    action_types=action_types,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 19: Migrate All Blox Actions

```python
results = migration.migrate_all_blox_actions(
    action="overwrite",  # Deletes existing action on target then recreates from source
    concurrency=5,  # Blox actions are independent — migrate up to 5 at once
)
print(json.dumps(results, indent=4))
```

---

## Example 20: Migrate Datasecurity for Specific Data Models

Migrate data models before data security — rules can only be written onto a data model that already exists on the target.

```python
results = migration.migrate_datasecurity(
    datamodel_names=["Sample ECommerce"],
)
print(json.dumps(results, indent=4))
```

---

## Example 21: Migrate Datasecurity for All Data Models

```python
results = migration.migrate_all_datasecurity()
print(json.dumps(results, indent=4))
```

---

## Example 22: Migrate Saved Formulas for Specific Data Models

Migrate data models before saved formulas — formulas can only be created on a datasource that already exists on the target.

```python
results = migration.migrate_saved_formulas(
    datamodel_names=["Sample ECommerce"],
    action="skip",  # Options: "skip", "overwrite", "duplicate" ("overwrite" behaves like "duplicate" — no update/delete endpoint exists)
)
print(json.dumps(results, indent=4))
```

---

## Example 23: Migrate Saved Formulas for All Data Models

```python
results = migration.migrate_all_saved_formulas()
print(json.dumps(results, indent=4))
```

---

## Example 24: Migrate Saved Filters for Specific Data Models

Migrate data models before saved filters — filters can only be created on a datasource that already exists on the target.

```python
results = migration.migrate_saved_filters(
    datamodel_names=["Sample ECommerce"],
    action="skip",  # Options: "skip", "overwrite", "duplicate" ("overwrite" behaves like "duplicate" — no update/delete endpoint exists)
)
print(json.dumps(results, indent=4))
```

---

## Example 25: Migrate Saved Filters for All Data Models

```python
results = migration.migrate_all_saved_filters()
print(json.dumps(results, indent=4))
```

---

## The *_v2 Methods (Examples 26–37)

`migrate_groups_v2`, `migrate_users_v2`, `migrate_datamodels_v2` and `migrate_dashboards_v2` (and their `migrate_all_*_v2` versions) replace Examples 1–4 and 6–9. They check the target before writing (`action="skip"` by default) and return the standard result: `ok`, `status`, `succeeded`, `skipped`, `failed` and the counts.

---

## Example 26: Migrate Specific Groups by Name

```python
group_names = [
    "Sales Team",
    "Finance Team",
]
results = migration.migrate_groups_v2(
    group_names=group_names,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 27: Migrate All Groups

```python
results = migration.migrate_all_groups_v2(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 28: Migrate Specific Users by Email

Migrate groups before users — user payloads reference target group IDs.

```python
user_emails = [
    "alice@example.com",
    "bob@example.com",
]
results = migration.migrate_users_v2(
    user_emails=user_emails,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 29: Migrate All Users

```python
results = migration.migrate_all_users_v2(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    ignore_custom_roles=False,  # Set True to strip a "custom_" prefix when matching roles
)
print(json.dumps(results, indent=4))
```

---

## Example 30: Migrate Specific Data Models by Name

```python
datamodel_names = [
    "Sample ECommerce",
    "Marketing Live Model",
]
results = migration.migrate_datamodels_v2(
    datamodel_names=datamodel_names,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    dependencies="all",  # Or a list like ["dataSecurity", "formulas"]
    provider_connection_map={"Athena": "target-connection-oid"},
    shares=False,
)
print(json.dumps(results, indent=4))
```

---

## Example 31: Migrate Specific Data Models by ID

```python
datamodel_ids = [
    "datamodel-oid-1",
    "datamodel-oid-2",
]
results = migration.migrate_datamodels_v2(
    datamodel_ids=datamodel_ids,
    action="overwrite",  # Replaces the existing model on target with the source schema
    shares=True,  # Also remap and migrate shares (users/groups matched by email/name)
)
print(json.dumps(results, indent=4))
```

---

## Example 32: Migrate All Data Models

```python
results = migration.migrate_all_datamodels_v2(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    concurrency=5,  # Data models are independent — migrate up to 5 at once
)
print(json.dumps(results, indent=4))
```

---

## Example 33: Migrate Specific Dashboards by ID

Migrate groups, users, folders, and data models before dashboards — dashboard owner/share remapping and folder placement depend on the first three, and dashboard widgets reference the data models by title.

```python
dashboard_ids = [
    "dashboard-oid-1",
    "dashboard-oid-2",
]
results = migration.migrate_dashboards_v2(
    dashboard_ids=dashboard_ids,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 34: Migrate Specific Dashboards by Name

```python
dashboard_names = [
    "Sales Overview",
    "Marketing KPIs",
]
results = migration.migrate_dashboards_v2(
    dashboard_names=dashboard_names,
    action="overwrite",  # Replaces the existing dashboard (matched by oid) with the source version
)
print(json.dumps(results, indent=4))
```

---

## Example 35: Migrate All Dashboards

```python
results = migration.migrate_all_dashboards_v2(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    concurrency=5,  # Dashboards are independent — migrate up to 5 at once
)
print(json.dumps(results, indent=4))
```

---

## Example 36: Dashboards without Shares or Owner, Republished

`migrate_share=False` leaves the imported dashboards unshared, `change_ownership=False` keeps the token user as owner, and `republish=True` republishes them to their users.

```python
results = migration.migrate_dashboards_v2(
    dashboard_names=["Sample ECommerce Overview"],
    action="overwrite",
    republish=True,
    migrate_share=False,
    change_ownership=False,
)
print(results["succeeded"], results["failed"])
```

---

## Example 37: Duplicate a Data Model Under a New Title

```python
results = migration.migrate_datamodels_v2(
    datamodel_names=["Sample ECommerce"],
    action="duplicate",
    new_title="Sample ECommerce Copy",
)
print(results["succeeded"])
```

---

## Notes

- Adjust parameters as needed for your environment.
- Prefer the `*_v2` methods for groups, users, data models and dashboards: the older methods warn and are removed in the next major release. `migrate_dashboard_shares` is not deprecated.
- Notebook, folder, and Blox action migrations take an `action` parameter (`"skip"`, `"overwrite"`, `"duplicate"`); saved formula and filter migrations support `"skip"` and `"duplicate"` only. All six return `ok`, `status`, `succeeded`, `skipped`, `failed`, and counts.
- Notebook, folder, and Blox action migrations accept `concurrency` to process several items in parallel. Folders are processed one hierarchy level at a time, so a parent always exists before its children.
- `"overwrite"` on a folder deletes it and all its dashboards on the target before recreating: use with caution.
- Blox action migration requires the target to be a Linux deployment.
- Data security, saved formulas, and saved filters need the data model to already exist on the target: migrate data models first.
- For more details, refer to the documentation in the `docs/` folder.

---
