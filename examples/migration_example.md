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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pysisense import Migration

source_yaml_path = os.path.join(os.path.dirname(__file__), "source.yaml")
target_yaml_path = os.path.join(os.path.dirname(__file__), "target.yaml")

migration = Migration(source_yaml=source_yaml_path, target_yaml=target_yaml_path, debug=False)
```

---

## Example 1: Migrate Specific Groups

Migrate specific groups from source to target.

```python
group_name_list = ["mig_test", "mig_test_2"]
migration_results = migration.migrate_groups(group_name_list)
print(json.dumps(migration_results, indent=4))
```

---

## Example 2: Migrate All Groups

Migrate all groups from source to target.

```python
migration_results = migration.migrate_all_groups()
print(json.dumps(migration_results, indent=4))
```

---

## Example 3: Migrate Specific Users

Migrate specific users from source to target.

```python
user_name =  ["john.doe@sisense.com"]
migration_results = migration.migrate_users(user_name)
print(json.dumps(migration_results, indent=4))
```

---

## Example 4: Migrate All Users

Migrate all users from source to target.

```python
migration_results = migration.migrate_all_users()
print(json.dumps(migration_results, indent=4))
```

---

## Example 5: Migrate Dashboard Shares

Migrate shares for specific dashboards from source to target.

```python
source_dashboard_ids = ["659583469a933c002adc8574","6261702faae68e0036b1572b"]
target_dashboard_ids = ["659583469a933c002adc8574","6261702faae68e0036b1572b"]
share_migration_results = migration.migrate_dashboard_shares(
    source_dashboard_ids=source_dashboard_ids,
    target_dashboard_ids=target_dashboard_ids,
    change_ownership=True,
)
print(json.dumps(share_migration_results, indent=4))
```

---

## Example 6: Migrate Specific Dashboards

Migrate dashboards by name or ID.

**By Name:**
```python
dashboard_name_list = [
    "Export to PDF text widget",
    "samp_lead_gen_2"
]
migration_results_by_name = migration.migrate_dashboards(
    dashboard_names=dashboard_name_list,
    action="skip",                                                              # Options: "skip", "overwrite", "duplicate"
    republish=True,                                                             # Republishes dashboards after migration
    migrate_share=True,                                                         # Migrate shares for the dashboards
    change_ownership=True                                                       # Change ownership of dashboards (requires migrate_share=True)
)
print("Migration results using dashboard names:")
print(json.dumps(migration_results_by_name, indent=4))
```

**By ID:**
```python
dashboard_id_list = [
    "659583469a933c002adc8574",
    "659abcde9a933c002adc1234",
    "6261702faae68e0036b1572b"
]
migration_results_by_id = migration.migrate_dashboards(
    dashboard_ids=dashboard_id_list,
    action="overwrite",                                                         # Options: "skip", "overwrite", "duplicate"
    republish=False,                                                            # Don't republish in this case
    migrate_share=True,
    change_ownership=False                                                      # Only migrate shares, don't change ownership
)
print("\nMigration results using dashboard IDs:")
print(json.dumps(migration_results_by_id, indent=4))
```

---

## Example 7: Migrate All Dashboards in Batches

Migrate all dashboards from source to target environment in batches.

```python
migration_results = migration.migrate_all_dashboards(
    #action="overwrite",                                                        # Options: "skip", "overwrite", "duplicate".
    republish=True,                                                             # Republishes dashboards after migration
    migrate_share=True,                                                         # Migrate shares for the dashboards
    change_ownership=True,                                                      # Change ownership of dashboards (requires migrate_share=True)
    batch_size=10,                                                              # Process 10 dashboards at a time
    sleep_time=10                                                               # Wait for 10 seconds between processing each batch
)
print(json.dumps(migration_results, indent=4))
```

---

## Example 8: Migrate DataModels

Migrate one or more datamodels from source to target.

**By IDs:**
```python
datamodel_ids = [
    "dddfac9f-86e6-4a9a-af28-f52106dcc55c",
    "97a41a88-3d8e-491d-8fe6-9eb111d86c57"
]
migration_results = migration.migrate_datamodels(
    datamodel_ids=datamodel_ids,
    dependencies=["dataSecurity", "formulas"],                                  # Optional. Choose from: dataSecurity, formulas, hierarchies, perspectives
    shares=True                                                                 # Optional. If True, also migrates the model's shares
)
print(json.dumps(migration_results, indent=4))
```

**By Names:**
```python
datamodel_names = [
    "pysense_databricks",
    "pysense_databricks_ec"
]
migration_results = migration.migrate_datamodels(
    datamodel_names=datamodel_names,
    dependencies=["hierarchies", "perspectives"],                               # Optional. Select only needed dependencies
    shares=False                                                                # Skip share migration in this case
)
print(json.dumps(migration_results, indent=4))
```

**Migrate data models and update the data source connections in the target environment using Connection Mapping:**
```python
datamodel_names = [
    "pysense_databricks",
    "pysense_databricks_ec"
]

# Map original provider names to new connection IDs in the target (e.g., when migrating from dev to prod)
provider_connection_map = {
    "Databricks": "53874a46-1360-45d8-a005-1cca41ef3e1c",
    "GoogleBigQuery": "5550c482-4e52-4b0f-afff-865e964c0df5"
}
migration_results = migration.migrate_datamodels(
    datamodel_names=datamodel_names,
    provider_connection_map=provider_connection_map,
    dependencies="all",
    shares=False
)
print(json.dumps(migration_results, indent=4))
```

**Overwrite Existing Models:**
```python
datamodel_ids = [
    "a42107d7-b4e6-4679-b11f-da580978d1ee",
    "dddfac9f-86e6-4a9a-af28-f52106dcc55c"
]
migration_results = migration.migrate_datamodels(
    datamodel_ids=datamodel_ids,
    action="overwrite",                                                         # Overwrites existing model in target using its ID
    dependencies="all",
    shares=True
)
print(json.dumps(migration_results, indent=4))
```

**Duplicate Model with New Name:**
```python
datamodel_names = [
    "pysense_databricks"
]
migration_results = migration.migrate_datamodels(
    datamodel_names=datamodel_names,
    action="duplicate",                                                         # Create a new model in target
    new_title="",                                                               # Optional: Custom title; if not set, adds " (Duplicate)" to original name
    dependencies=["formulas", "perspectives"],
    shares=False
)
print(json.dumps(migration_results, indent=4))
```

---

## Example 9: Migrate All DataModels in Batches

Migrate all datamodels from source to target environment in batches.

```python
dependencies = ["dataSecurity", "formulas"]                                     # Only migrate selected dependencies
shares = True                                                                   # Migrate shares along with the data models
batch_size = 10                                                                 # Process 5 data models per batch
sleep_time = 10                                                                 # Wait 10 seconds between batches

migration_summary = migration.migrate_all_datamodels(
    dependencies=dependencies,
    shares=shares,
    batch_size=batch_size,
    sleep_time=sleep_time,
    action="overwrite",                                                         # Options: "overwrite", "duplicate". For "duplicate", a new model is created in the target with the same name as the source model, but with " (Duplicate)" appended to it.
)
print(json.dumps(migration_summary, indent=4))
```

---

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to the documentation in the `docs/` folder.

---
