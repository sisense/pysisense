# Folder Example Usage

This guide demonstrates how to use the `Folder` class from the `pysisense` package to manage Sisense dashboard folders.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Use a Sisense **admin** API token.

```python
import os
import json
from pysisense import SisenseClient, Folder

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

folder = Folder(api_client=api_client)
```

---

## Example 1: Get All Folders (Tree)

Retrieve the nested folder hierarchy.

```python
response = folder.get_all_folders()
print(json.dumps(response, indent=4))

folder_df = api_client.to_dataframe(response)
print(folder_df)
api_client.export_to_csv(response, "all_folders.csv")
```

---

## Example 2: Get Folder by ID

Fetch a single folder by OID.

```python
folder_id = "65d62c9wregfhg0e33bc64e8"
response = folder.get_folder_id(folder_id)
print(json.dumps(response, indent=4))
```

---

## Example 3: Create a Folder

Create a root-level folder or a subfolder under a parent.

```python
# Root folder
response = folder.create_folder("Analytics")

# Subfolder under an existing parent
parent_id = "65d62c9wregfhg0e33bc64e8"
response = folder.create_folder("Q1 Reports", parent_id=parent_id)
print(json.dumps(response, indent=4))
```

---

## Example 4: Update a Folder

Change name, parent, or owner. Only pass the fields you want to change.

```python
folder_id = "65d62c9wregfhg0e33bc64e8"
response = folder.update_folder(folder_id, name="Analytics (Archive)")
print(json.dumps(response, indent=4))
```

---

## Example 5: Delete a Folder

Delete an empty folder by OID.

```python
folder_id = "65d62c9wregfhg0e33bc64e8"
response = folder.delete_folder(folder_id)
print(json.dumps(response, indent=4))
```

---

## Notes

- Folder operations require appropriate admin permissions.
- Deleting a folder may fail if it still contains dashboards or subfolders.
- For ownership transfers across a folder tree, see `AccessManagement.change_folder_and_dashboard_ownership` in [access_management_example.md](./access_management_example.md).
