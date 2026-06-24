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

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to `docs/mergetool.md`.

---
