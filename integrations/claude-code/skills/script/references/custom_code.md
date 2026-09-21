# Custom-code notebook operations reference

`from pysisense import CustomCode` — `custom_code = CustomCode(api_client=api_client)`

Manages Sisense custom-code notebooks (Jupyter notebooks used for transformations/custom code tables) and their file-system layout under `custom_code_notebooks`. This module has a single mixin (`core.py`) — no groups/columns/ownership/admin/tenant methods live here (those are on `AccessManagement`).

## Listing and exporting

| Method | Use when |
|---|---|
| `get_notebooks(notebook_type=None, params=None)` | List notebooks. Pass `notebook_type="CustomCodeTransformation"` to filter; extra `params` are merged into the query string alongside it. |
| `export_notebook(notebook_id)` | Full notebook export payload for one notebook — the source object for re-creating/migrating a notebook elsewhere. |

```python
notebooks = custom_code.get_notebooks(notebook_type="CustomCodeTransformation")
export = custom_code.export_notebook("notebook-uuid-here")
```

## Notebook CRUD lifecycle

```python
payload = {
    "notebookType": "CustomCodeTransformation",
    "displayName": "My Custom Notebook",
}
created = custom_code.create_notebook(payload)  # POST /api/v1/notebooks
custom_code.update_notebook(created["oid"], {"displayName": "Renamed"})  # PATCH, partial
custom_code.delete_notebook(created["oid"])  # DELETE
```

- `create_notebook` / `update_notebook` both send the `Internal: true` header by default (`use_internal_header=True`) — this is required for programmatic notebook create/update (win2linux migration support). Pass `use_internal_header=False` only if a target environment rejects that header.
- `update_notebook` is a partial update — only the fields present in `notebook_data` are sent. Calling it with an empty dict returns `{"ok": False, "error": "notebook_data must contain at least one field to update."}` without hitting the API.
- `create_notebook` returns `{"ok": False, "error": "notebook_data must be a dictionary."}` if `notebook_data` isn't a dict.
- `delete_notebook` returns `{"success": True}` on an HTTP 204 (the common case), otherwise a parsed JSON body if the API returns one on success.

## Folder/file listing and renaming

```python
contents = custom_code.list_notebook_folder_contents("folder-id")  # GET .../notebooks/{folder_id}/

custom_code.rename_notebook_file(
    "notebooks/custom_code_notebooks/my_folder/file.ipynb",
    {"name": "renamed.ipynb"},
)
custom_code.rename_notebook_folder("old-folder-id", {"name": "new-folder-name"})
```

- `rename_notebook_file` takes a `resource_path` **relative to `/api/resources/`** — a leading `/` is stripped automatically if included. It PATCHes that arbitrary resource path, so it isn't limited to notebook files (whatever path is passed is what gets hit).
- `rename_notebook_folder` is scoped specifically to `notebooks/custom_code_notebooks/notebooks/{old_id}/` — pass just the folder ID, not a full path.
- Both renaming methods return `{"ok": False, "error": "payload must contain at least one field to update."}` if `payload` is empty, before making a request.

## Return-shape conventions

All methods follow the SDK-wide convention: a successful call returns the parsed JSON body (or `{"success": True}` when the API responds with no body / 204), and a failed call returns `{"ok": False, "error": "..."}` with the response detail appended when available. There's no separate exception path — check for the `"error"` key rather than wrapping calls in `try/except`.

```python
result = custom_code.create_notebook(payload)
if "error" in result:
    print(result["error"])
```

For cross-environment notebook migration (not just single-environment CRUD), use `MergeTool.migrate_notebooks` / `migrate_all_notebooks` instead — see `examples/mergetool_example.md`.
