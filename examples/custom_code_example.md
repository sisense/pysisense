# Custom Code Example Usage

Manage Sisense custom-code notebooks for migration and automation.

---

## Prerequisites

```python
import os
import json
from pysisense import SisenseClient, CustomCode

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

custom_code = CustomCode(api_client=api_client)
```

---

## Example 1: List Notebooks

```python
notebooks = custom_code.get_notebooks(notebook_type="CustomCodeTransformation")
print(json.dumps(notebooks, indent=4))
```

---

## Example 2: Export a Notebook

```python
export = custom_code.export_notebook("notebook-uuid-here")
print(json.dumps(export, indent=4))
```

---

## Example 3: Create a Notebook

Uses the `Internal` header by default (required for programmatic create).

```python
payload = {
    "notebookType": "CustomCodeTransformation",
    "displayName": "My Custom Notebook",
}
result = custom_code.create_notebook(payload)
print(json.dumps(result, indent=4))
```

---

## Example 4: Update a Notebook

```python
result = custom_code.update_notebook("notebook-uuid-here", {"displayName": "Renamed"})
print(json.dumps(result, indent=4))
```

---

## Example 5: Delete a Notebook

```python
result = custom_code.delete_notebook("notebook-uuid-here")
print(json.dumps(result, indent=4))
```

---

## Example 6: List Folder Contents

```python
contents = custom_code.list_notebook_folder_contents("folder-id")
print(json.dumps(contents, indent=4))
```

---

## Example 7: Rename File or Folder

```python
custom_code.rename_notebook_file(
    "notebooks/custom_code_notebooks/my_folder/file.ipynb",
    {"name": "renamed.ipynb"},
)
custom_code.rename_notebook_folder("old-folder-id", {"name": "new-folder-name"})
```
