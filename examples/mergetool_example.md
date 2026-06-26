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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
    action="skip",               # Options: "skip", "overwrite", "duplicate"
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
    action="overwrite",          # Deletes existing on target then recreates from source
)
print(json.dumps(results, indent=4))
```

---

## Example 3: Migrate All Notebooks

```python
results = merge.migrate_all_notebooks(
    action="skip",               # Options: "skip", "overwrite", "duplicate"
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
    action="skip",               # Child folders are included automatically
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
    action="overwrite",          # Deletes existing folder on target then recreates
)
print(json.dumps(results, indent=4))
```

---

## Example 6: Migrate All Folders

```python
results = merge.migrate_all_folders(
    action="skip",               # Options: "skip", "overwrite", "duplicate"
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

## Notes

- Adjust parameters as needed for your environment.
- Folder migration preserves the full hierarchy — child folders are always created under their parent.
- Folders whose parent is not in the migration list are created at the root level on the target.
- `"overwrite"` on a folder deletes it and all its dashboards on the target before recreating — use with caution.
- For more details, refer to `docs/mergetool.md`.

---
