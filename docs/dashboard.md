Dashboard Class Documentation
=============================

The `Dashboard` class provides high-level methods to interact with Sisense dashboards using the API. It allows users to retrieve, update, share, and analyze dashboards.

* * * * *

Class: `Dashboard`
------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Dashboard` class.

**Parameters:**

-   `api_client` (APIClient, optional): Instance of `APIClient`.

-   `debug` (bool): Enable debug logging.

* * * * *

### `get_all_dashboards()`

Fetches all dashboards accessible to the authenticated user.

**Returns:**

-   `list`: List of dashboard metadata.

* * * * *

### `get_dashboard_by_id(dashboard_id)`

Retrieves a specific dashboard by its ID.

**Parameters:**

-   `dashboard_id` (str): The ID of the dashboard.

**Returns:**

-   `dict`: Dashboard metadata or `None`.

* * * * *

### `get_dashboard_by_name(dashboard_name)`

Retrieves a specific dashboard by its name.

**Parameters:**

-   `dashboard_name` (str): The name of the dashboard.

**Returns:**

-   `dict`: Dashboard metadata or `None`.

* * * * *

### `export_dashboard(dashboard_id)`

Exports a full dashboard definition as JSON using the admin export API (`GET /api/v1/dashboards/export` with `dashboardIds` and `adminAccess=true`). Use this when you need the complete serialized dashboard (scripts, widgets map, layout, filters, metadata), not only the admin list row from `get_dashboard_by_id`.

**Parameters:**

-   `dashboard_id` (str): The dashboard `oid` to export.

**Returns:**

-   `dict`: The first element of the export array on success (the dashboard object). On failure, a dict with an `"error"` key and a short message (HTTP failure, invalid JSON, or unexpected response shape).

* * * * *

### `get_dashboard_widgets(dashboard_ref)`

Returns the `widgets` collection from an admin **export** of the dashboard—the same `GET /api/v1/dashboards/export?dashboardIds=...&adminAccess=true` flow as `export_dashboard`, not the lightweight `.../widgets` REST list. Resolves `dashboard_ref` as a 24-character `oid` or a title via `resolve_dashboard_reference`. If the export has no `widgets` key or it is empty, returns an empty list.

**Parameters:**

-   `dashboard_ref` (str): Dashboard `oid` or title.

**Returns:**

-   `list`: Widget objects from the export payload on success (may be empty). On failure, a dict with an `"error"` key (unresolved reference or export failure). If `widgets` is present but not a list or object map of widget dicts, returns an error dict.

* * * * *

### `add_dashboard_script(dashboard_id, script, executing_user=None)`

Adds or overwrites the dashboard-level JavaScript script. Sisense only allows the **dashboard owner** to modify scripts.

**Script input**

-   A JSON string whose parsed object is sent to the API (for example a payload containing a `script` field), **or**
-   A raw JavaScript string (multi-line). If the string does **not** start with `{`, it is wrapped automatically as `{"script": "<your code>"}` before the request.

**`executing_user` (optional)**

-   Sisense **username** (login) of the API token user. When provided, the method temporarily changes dashboard ownership to that user (using admin APIs), reapplies the script, then restores the previous owner and prior share rows.
-   When omitted, the code assumes the token user is already the dashboard owner. If the PUT fails with **404** and no `executing_user` was passed, the returned message explains that the token may not be the owner and suggests passing `executing_user` or making the token user the owner.

**Returns:**

-   `str`: `"Dashboard Script added successfully."` on success, or an `"Error: ..."` string on failure.

* * * * *

### `add_widget_script(dashboard_id, widget_id, script, executing_user=None)`

Adds or overwrites the JavaScript script for one widget. Same **owner** and **`executing_user`** semantics as `add_dashboard_script` (temporary ownership, share restore, owner restore when `executing_user` is set).

**After a successful script update**, the dashboard is **republished** via `POST /api/v1/dashboards/{dashboard_id}/publish?force=true`. A **204** response is treated as success; otherwise an error string is returned (the script may already have been saved).

If the PUT fails with **403** and `executing_user` was not provided, the error text suggests the token user is not the owner and recommends `executing_user` or changing ownership.

**Returns:**

-   `str`: `"Widget Script added successfully."` on success, or an `"Error: ..."` string on failure.

* * * * *

### `get_dashboard_script(dashboard_id)`

Builds a **`SisenseScript`** helper from an admin export of the dashboard (`export_dashboard` / `/api/v1/dashboards/export`). Use it to read the current dashboard script in a cleaned, formatted form.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

**Returns:**

-   `SisenseScript` or `dict`: A `SisenseScript` instance on success, or `{"error": "..."}` if the export fails.

* * * * *

### `get_widget_script(dashboard_id, widget_id)`

Builds a **`SisenseScript`** helper for one widget in the exported dashboard payload.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.
-   `widget_id` (str): Widget key in the exported dashboard’s `widgets` map (typically the widget OID / id used in that structure).

**Returns:**

-   `SisenseScript` or `dict`: A `SisenseScript` instance when the widget exists and has script data, or `{"error": "..."}` on failure (including missing widget in the export).

* * * * *

### `add_dashboard_shares(dashboard_id, shares)`

Adds or updates sharing settings for a dashboard.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

-   `shares` (list): Share definitions with `name`, `type`, and `rule`.

**Returns:**

-   `str`: Status message of the operation.

* * * * *

### `get_dashboard_columns(dashboard_name)`

Extracts distinct columns used in a dashboard (filters and widgets).

**Parameters:**

-   `dashboard_name` (str): Name of the dashboard.

**Returns:**

-   `list`: List of unique table/column combinations with metadata.

* * * * *

### `get_dashboard_share(dashboard_name)`

Retrieves share information (users and groups) for a specific dashboard by its title.

**Parameters:**

- `dashboard_name` (str): Title of the dashboard whose share settings you want to inspect.

**Returns:**

- `list`: A list of dictionaries, each containing the type of share (`user` or `group`) and the corresponding name (email or group name). Returns an empty list if the dashboard is not found or has no shares.

* * * * *

### `get_dashboard_shares_v1(dashboard_id, admin_access=True)`

Retrieves share details using ``GET /api/v1/dashboards/{dashboard_id}/shares``. Returns the raw Sisense shares payload (for example ``sharesTo`` and ``owner``), unlike ``get_dashboard_share`` which resolves names from the admin list.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `admin_access` (bool, optional): Request with ``adminAccess=true``. Default `True`.

**Returns:**

- `dict`: Shares response on success, or `{"error": "..."}` on failure.

* * * * *

### `resolve_dashboard_reference(dashboard_ref)`

Resolves a dashboard reference (ID or name) to a concrete dashboard ID and title.

This helper accepts a single string that may be either:

- A Sisense dashboard ID, or  
- A dashboard title (name).

It first attempts to treat the reference as an ID using `get_dashboard_by_id`.  
If that fails or the reference does not look like an ID, it falls back to `get_dashboard_by_name`.

**Parameters:**

- `dashboard_ref` (str):  
  Dashboard reference to resolve. This can be either an ID or a name.

**Returns:**

- `dict`: Dictionary with the following keys:
  - `success` (bool): `True` if the reference was resolved to a dashboard; otherwise `False`.
  - `status_code` (int):  
    `200` if resolved successfully, `404` if not found, or `500` if an unexpected error occurred.
  - `dashboard_id` (str or None): Resolved dashboard ID (`oid`) if found, otherwise `None`.
  - `dashboard_title` (str or None): Resolved dashboard title if found, otherwise `None`.
  - `error` (str or None): Error message if `success` is `False`, otherwise `None`.

* * * * *

### `move_dashboard_to_folder(dashboard_id, folder_id)`

Moves a dashboard into a folder by PATCHing ``parentFolder`` on ``/api/dashboards/{dashboard_id}``.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `folder_id` (str): Target folder ``oid``.

**Returns:**

- `dict`: Updated dashboard object on success, or `{"error": "..."}` on failure.

* * * * *

### `rename_dashboard(dashboard_id, title)`

Renames a dashboard by PATCHing ``title`` on ``/api/dashboards/{dashboard_id}``.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `title` (str): New dashboard title.

**Returns:**

- `dict`: Updated dashboard object on success, or `{"error": "..."}` on failure.

* * * * *

### `publish_dashboard(dashboard_id, admin_access=True, force=False)`

Publishes (republishes) a dashboard via ``POST /api/v1/dashboards/{dashboard_id}/publish``. Defaults to ``adminAccess=true`` for admin-token preflight republish.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `admin_access` (bool, optional): Append ``adminAccess=true``. Default `True`.
- `force` (bool, optional): Append ``force=true``. Default `False`.

**Returns:**

- `dict`: `{"success": True}` or the JSON body on success; `{"error": "..."}` on failure.

* * * * *

### `can_be_owned(dashboard_id)`

Checks whether the dashboard can be owned by the current user via ``GET /api/v1/dashboards/{dashboard_id}/can_be_owned``.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.

**Returns:**

- `dict`: API response on success, or `{"error": "..."}` on failure.

* * * * *

### `SisenseScript` helper (`scripts.py`)

Instances of **`SisenseScript`** are returned by `get_dashboard_script` and `get_widget_script` on success. They wrap raw script text plus metadata (title, URL path, last opened; widget scripts also carry **widget type**).

**Rendering behavior (`to_text`)**

-   Dashboard scripts: a fixed Sisense welcome comment block is removed from the source, then the body is passed through **jsbeautifier** (4-space indent).
-   Widget scripts: boilerplate matching the standard Sisense “see the online documentation” comment is stripped with a **regex**, then the same beautifier runs.
-   A short **footer** is appended (dashboard title / URL / last opened for dashboard scripts; widget title, type, URL, and last opened for widget scripts).

**Methods**

-   `to_text() -> str`: Full formatted JavaScript string (empty string if nothing remains after stripping).
-   `to_md() -> str`: Markdown with an `#` title and a fenced `js` code block built from `to_text()`.
-   `to_file(path: str) -> None`: Writes `to_text()` to the given path.

* * * * *

### `get_dashboards(fields=None)`

Retrieves dashboards visible to the authenticated user via `GET /api/v1/dashboards`. Returns dashboards the current user owns or has been shared to, as opposed to `get_all_dashboards` which uses the admin endpoint.

**Parameters:**

-   `fields` (list[str], optional): Subset of fields to include in each dashboard object (e.g. `["oid", "title", "owner"]`). When omitted, all fields are returned.

**Returns:**

-   `list[dict]`: List of dashboard objects on success, or `{"error": "..."}` on failure.

* * * * *

### `publish_dashboard(dashboard_id)`

Publishes a dashboard so it becomes visible to shared users. Sends `POST /api/v1/dashboards/{id}/publish?force=false&adminAccess=true`.

**Parameters:**

-   `dashboard_id` (str): The dashboard `oid` to publish.

**Returns:**

-   `dict`: `{"success": True}` on success, or `{"error": "..."}` on failure.

* * * * *

### `rename_dashboard(dashboard_id, title)`

Renames a dashboard by sending `PATCH /api/dashboards/{id}` with only `title` in the body. Other fields are not modified.

**Parameters:**

-   `dashboard_id` (str): The dashboard `oid` to rename.
-   `title` (str): The new display title.

**Returns:**

-   `dict`: The updated dashboard object on success, or `{"error": "..."}` on failure.

* * * * *

### `move_dashboard_to_folder(dashboard_id, folder_id)`

Moves a dashboard into a folder by sending `PATCH /api/dashboards/{id}` with only `parentFolder` in the body. Other fields are not modified.

**Parameters:**

-   `dashboard_id` (str): The dashboard `oid` to move.
-   `folder_id` (str): The target folder `oid`.

**Returns:**

-   `dict`: The updated dashboard object on success, or `{"error": "..."}` on failure.
