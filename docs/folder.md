Folder Class Documentation
==========================

The `Folder` class provides methods to create, read, update, and delete Sisense dashboard folders via the REST API (`/api/v1/folders`).

* * * * *

Class: `Folder`
---------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Folder` class.

**Parameters:**

-   `api_client` (SisenseClient, optional): An existing authenticated client. When omitted, a new `SisenseClient` is created.

-   `debug` (bool): Enable debug logging on a newly created client. Default is `False`.

* * * * *

### `create_folder(name, parent_id=None)`

Creates a new folder.

**Parameters:**

-   `name` (str): Display name for the folder.

-   `parent_id` (str, optional): Parent folder OID. Omit for a root-level folder.

**Returns:**

-   `dict`: Created folder object on success (HTTP 201), or a dict with an `"error"` key on failure.

* * * * *

### `update_folder(folder_id, name=None, parent_id=None, owner=None)`

Updates folder properties. Only provided arguments are sent in the PATCH body.

**Parameters:**

-   `folder_id` (str): Folder OID to update.

-   `name` (str, optional): New folder name.

-   `parent_id` (str, optional): New parent folder OID (moves the folder).

-   `owner` (str, optional): New owner user OID.

**Returns:**

-   `dict`: Updated folder object on success (HTTP 200), or a dict with an `"error"` key on failure.

* * * * *

### `get_folder_id(folder_id)`

Retrieves a single folder by OID.

**Parameters:**

-   `folder_id` (str): Folder OID.

**Returns:**

-   `dict`: Folder metadata on success, or a dict with an `"error"` key on failure.

* * * * *

### `get_folders(structure="flat")`

Retrieves folders with a configurable ``structure`` query parameter (``GET /api/v1/folders?structure={structure}``). Defaults to ``"flat"``.

**Parameters:**

-   `structure` (str, optional): Sisense folder structure type. Default is `"flat"`.

**Returns:**

-   `list`: Folder data from the API, or a dict with an `"error"` key on failure.

* * * * *

### `get_folder_ancestors(structure)`

Retrieves folder data for a given ``structure`` query parameter (``GET /api/v1/folders?structure={structure}``).

**Parameters:**

-   `structure` (str): Sisense folder structure type for the request.

**Returns:**

-   `list`: Folder data from the API, or a dict with an `"error"` key on failure.

* * * * *

### `get_navver()`

Retrieves the Sisense navigation payload (``GET /api/v1/navver``), including the ``folders`` hierarchy.

**Returns:**

-   `dict`: Navver response on success, or a dict with an `"error"` key on failure.

* * * * *

### `get_all_folders()`

Retrieves the full folder tree (`structure=tree`).

**Returns:**

-   `list`: Root-level folder nodes (nested `folders` and `dashboards` may be present), or a dict with an `"error"` key on failure.

* * * * *

### `delete_folder(folder_id)`

Deletes a folder by OID.

**Parameters:**

-   `folder_id` (str): Folder OID to delete.

**Returns:**

-   `dict`: `{"message": "..."}` on success (HTTP 204), or a dict with an `"error"` key on failure.

* * * * *

### `get_folders(structure="flat")`

Retrieves folders using a configurable `structure` query parameter. Sends `GET /api/v1/folders?structure={structure}`. The default `"flat"` structure returns every folder as a top-level entry — useful for bulk lookups and ID mapping before migrations.

**Parameters:**

-   `structure` (str, optional): Sisense folder structure type. `"flat"` (default) returns a flat list. `"tree"` returns the nested hierarchy.

**Returns:**

-   `list[dict]`: Folder list on success, or `{"error": "..."}` on failure.

* * * * *

### `get_navver()`

Retrieves the Sisense navigation tree payload. Sends `GET /api/v1/navver`. The response includes a `folders` key with the folder hierarchy as rendered in the Sisense UI.

**Returns:**

-   `dict`: The navver response object on success, or `{"error": "..."}` on failure.
