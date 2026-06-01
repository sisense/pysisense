Custom Code Class Documentation
================================

The `CustomCode` class manages Sisense custom-code notebooks (Jupyter notebooks used for transformations and custom code tables).

* * * * *

Class: `CustomCode`
-------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `CustomCode` class.

* * * * *

### `get_notebooks(notebook_type=None, params=None)`

Lists notebooks via `GET /api/v1/notebooks`.

* * * * *

### `export_notebook(notebook_id)`

Exports a notebook via `GET /api/v1/notebooks/{id}/export`.

* * * * *

### `create_notebook(notebook_data, use_internal_header=True)`

Creates a notebook via `POST /api/v1/notebooks`. Sends `Internal: true` header by default.

* * * * *

### `update_notebook(notebook_id, notebook_data, use_internal_header=True)`

Updates a notebook via `PATCH /api/v1/notebooks/{id}`. Sends `Internal: true` header by default.

* * * * *

### `delete_notebook(notebook_id)`

Deletes a notebook via `DELETE /api/v1/notebooks/{id}`.

* * * * *

### `list_notebook_folder_contents(folder_id)`

Lists folder contents via `GET /api/resources/notebooks/custom_code_notebooks/notebooks/{id}/`.

* * * * *

### `rename_notebook_file(resource_path, payload)`

Renames a resource file via `PATCH /api/resources/{path}`.

* * * * *

### `rename_notebook_folder(old_id, payload)`

Renames a notebook folder via `PATCH /api/resources/notebooks/custom_code_notebooks/notebooks/{oldId}/`.
