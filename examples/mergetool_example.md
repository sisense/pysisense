# MergeTool Example Usage

This guide demonstrates how to use the `MergeTool` class from the `pysisense` package to migrate custom-code notebooks between Sisense environments.

---

## Prerequisites

- Ensure `source.yaml` and `target.yaml` are in the same folder as your script.
- Import required modules and initialize the MergeTool class.

```python
import sys
import os
import json

# For local development only — not needed after pip install
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pysisense import MergeTool

source_yaml_path = os.path.join(os.path.dirname(__file__), "source.yaml")
target_yaml_path = os.path.join(os.path.dirname(__file__), "target.yaml")

merge = MergeTool(source_yaml=source_yaml_path, target_yaml=target_yaml_path, debug=False)
```

---

## Example 1: Migrate Specific Notebooks by ID

```python
notebook_ids = [
    "notebook-id-1",
    "notebook-id-2",
]
results = merge.migrate_notebooks(
    notebook_ids=notebook_ids,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 2: Migrate Specific Notebooks by Name

```python
notebook_names = [
    "My Transformation Notebook",
    "ETL Pipeline",
]
results = merge.migrate_notebooks(
    notebook_names=notebook_names,
    action="overwrite",  # Deletes existing on target then recreates from source
)
print(json.dumps(results, indent=4))
```

---

## Example 3: Migrate All Notebooks

```python
results = merge.migrate_all_notebooks(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 4: Migrate Specific Folders by Name (with subtree)

```python
folder_names = [
    "Analytics",
    "Finance Reports",
]
results = merge.migrate_folders(
    folder_names=folder_names,
    action="skip",  # Child folders are included automatically
)
print(json.dumps(results, indent=4))
```

---

## Example 5: Migrate Specific Folders by ID (with subtree)

```python
folder_ids = [
    "folder-oid-1",
    "folder-oid-2",
]
results = merge.migrate_folders(
    folder_ids=folder_ids,
    action="overwrite",  # Deletes existing folder on target then recreates
)
print(json.dumps(results, indent=4))
```

---

## Example 6: Migrate All Folders

```python
results = merge.migrate_all_folders(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 7: Using an emit callback for progress tracking

```python
def on_progress(event: dict) -> None:
    print(f"[{event.get('type', '').upper()}] {event.get('step')} — {event.get('message')}")


results = merge.migrate_all_folders(action="skip", emit=on_progress)
print(json.dumps(results, indent=4))
```

---

## Example 8: Migrate Specific Blox Actions by Type

```python
action_types = [
    "Send Email Notification",
    "Refresh Dashboard",
]
results = merge.migrate_blox_actions(
    action_types=action_types,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 9: Migrate All Blox Actions

```python
results = merge.migrate_all_blox_actions(
    action="overwrite",  # Deletes existing action on target then recreates from source
)
print(json.dumps(results, indent=4))
```

---

## Example 10: Migrate Specific Groups by Name

```python
group_names = [
    "Sales Team",
    "Finance Team",
]
results = merge.migrate_groups(
    group_names=group_names,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 11: Migrate All Groups

```python
results = merge.migrate_all_groups(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 12: Migrate Specific Users by Email

Migrate groups before users — user payloads reference target group IDs.

```python
user_emails = [
    "alice@example.com",
    "bob@example.com",
]
results = merge.migrate_users(
    user_emails=user_emails,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 13: Migrate All Users

```python
results = merge.migrate_all_users(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    ignore_custom_roles=False,  # Set True to strip a "custom_" prefix when matching roles
)
print(json.dumps(results, indent=4))
```

---

## Example 14: Migrate Specific Data Models by Name

```python
datamodel_names = [
    "Sales Elasticube",
    "Marketing Live Model",
]
results = merge.migrate_datamodels(
    datamodel_names=datamodel_names,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
    dependencies="all",  # Or a list like ["dataSecurity", "formulas"]
    provider_connection_map={"Athena": "target-connection-oid"},
    shares=False,
)
print(json.dumps(results, indent=4))
```

---

## Example 15: Migrate Specific Data Models by ID

```python
datamodel_ids = [
    "datamodel-oid-1",
    "datamodel-oid-2",
]
results = merge.migrate_datamodels(
    datamodel_ids=datamodel_ids,
    action="overwrite",  # Replaces the existing model on target with the source schema
    shares=True,  # Also remap and migrate shares (users/groups matched by email/name)
)
print(json.dumps(results, indent=4))
```

---

## Example 16: Migrate All Data Models

```python
results = merge.migrate_all_datamodels(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 17: Migrate Datasecurity for Specific Data Models

Migrate data models before data security — rules can only be written onto a data model that already exists on the target.

```python
results = merge.migrate_datasecurity(
    datamodel_names=["Sales Elasticube"],
)
print(json.dumps(results, indent=4))
```

---

## Example 18: Migrate Datasecurity for All Data Models

```python
results = merge.migrate_all_datasecurity()
print(json.dumps(results, indent=4))
```

---

## Example 19: Migrate Specific Dashboards by ID

Migrate groups, users, folders, and data models before dashboards — dashboard owner/share remapping and folder placement depend on the first three, and dashboard widgets reference the data models by title.

```python
dashboard_ids = [
    "dashboard-oid-1",
    "dashboard-oid-2",
]
results = merge.migrate_dashboards(
    dashboard_ids=dashboard_ids,
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Example 20: Migrate Specific Dashboards by Name

```python
dashboard_names = [
    "Sales Overview",
    "Marketing KPIs",
]
results = merge.migrate_dashboards(
    dashboard_names=dashboard_names,
    action="overwrite",  # Replaces the existing dashboard (matched by oid) with the source version
)
print(json.dumps(results, indent=4))
```

---

## Example 21: Migrate All Dashboards

```python
results = merge.migrate_all_dashboards(
    action="skip",  # Options: "skip", "overwrite", "duplicate"
)
print(json.dumps(results, indent=4))
```

---

## Notes

- Adjust parameters as needed for your environment.
- Folder migration preserves the full hierarchy — child folders are always created under their parent.
- Folders whose parent is not in the migration list are created at the root level on the target.
- `"overwrite"` on a folder deletes it and all its dashboards on the target before recreating — use with caution.
- Blox action migration requires the target environment to be a Linux deployment — saving and deleting Blox actions is not supported on Windows.
- Group migration excludes the built-in `Admins`, `All users in system`, and `Everyone` groups when using `migrate_all_groups`.
- User migration excludes users with the built-in `super` role when using `migrate_all_users`, and resolves roles across environments (including multi-tenant `tenantAdmin` and Windows `admin` remapping).
- Migrate groups before users, and users before dashboards — user payloads reference target group IDs, and dashboard shares reference target user/group IDs.
- Migrate folders before dashboards so each dashboard can be placed into its matching target folder; dashboards whose parent folder path isn't found on the target are left at the root.
- Data model migration matches conflicts by `title` (not OID). Connection credentials are never copied as-is — map providers to a target connection via `provider_connection_map`, or reconnect manually on the target after migration.
- Migrate data models before dashboards — dashboard widgets reference their data model's local Elasticube by title.
- Datasecurity migration requires the target data model to already exist — migrate data models before data security, and shares are matched by email (users) or name (groups), same as data model shares.
- Dashboard migration matches conflicts by `oid` (not title) — re-running with `"skip"` after a partial migration will correctly skip already-migrated dashboards.
- For more details, refer to `docs/mergetool.md`.

---
