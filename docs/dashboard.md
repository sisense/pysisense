Dashboard Class Documentation
=============================

The `Dashboard` class provides high-level methods to interact with Sisense dashboards using the API. It allows users to retrieve, update, share, and analyze dashboards.

All methods report failures as a standard error dict: `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}`. An empty list from a read method always means a genuinely empty result, never a failure.

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

-   `dict`: The first element of the export array on success (the dashboard object). On failure, the standard error dict `{"ok": False, "error": "..."}` (HTTP failure, invalid JSON, or unexpected response shape).

* * * * *

### `get_dashboard_widgets(dashboard_ref)`

Returns the `widgets` collection from an admin **export** of the dashboard—the same `GET /api/v1/dashboards/export?dashboardIds=...&adminAccess=true` flow as `export_dashboard`, not the lightweight `.../widgets` REST list. Resolves `dashboard_ref` as a 24-character `oid` or a title via `resolve_dashboard_reference`. If the export has no `widgets` key or it is empty, returns an empty list.

**Parameters:**

-   `dashboard_ref` (str): Dashboard `oid` or title.

**Returns:**

-   `list`: Widget objects from the export payload on success (may be empty). On failure, the standard error dict `{"ok": False, "error": "..."}` (unresolved reference or export failure). If `widgets` is present but not a list or object map of widget dicts, returns the error dict as well.

* * * * *

### `add_dashboard_script(dashboard_id, script, executing_user=None)`

Adds or overwrites the dashboard-level JavaScript script. Sisense only allows the **dashboard owner** to modify scripts.

**Script input**

-   A JSON string whose parsed object is sent to the API (for example a payload containing a `script` field), **or**
-   A raw JavaScript string (multi-line). If the string does **not** start with `{`, it is wrapped automatically as `{"script": "<your code>"}` before the request.

**`executing_user` (optional)**

-   Sisense **username** (login) of the API token user. When provided, the method temporarily changes dashboard ownership to that user (using admin APIs), reapplies the script, then restores the previous owner and prior share rows.
-   When omitted, the code assumes the token user is already the dashboard owner. If the PUT fails with **404** and no `executing_user` was passed, a hint is appended to the `error` sentence explaining that the token may not be the owner and suggesting passing `executing_user` or making the token user the owner.

**Returns:**

-   `dict`: `{"success": True, "message": "..."}` on success, or the standard error dict `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}` on failure. An invalid JSON script returns `{"ok": False, "error": "Dashboard Script must be a valid JSON string."}`.

* * * * *

### `add_widget_script(dashboard_id, widget_id, script, executing_user=None)`

Adds or overwrites the JavaScript script for one widget. Same **owner** and **`executing_user`** semantics as `add_dashboard_script` (temporary ownership, share restore, owner restore when `executing_user` is set).

**After a successful script update**, the dashboard is **republished** via `POST /api/v1/dashboards/{dashboard_id}/publish?force=true`. A **204** response is treated as success; otherwise the standard error dict is returned, stating that the script was added but republishing the dashboard failed.

If the PUT fails with **403** and `executing_user` was not provided, a hint is appended to the `error` sentence suggesting the token user is not the owner and recommending `executing_user` or changing ownership.

**Returns:**

-   `dict`: `{"success": True, "message": "..."}` on success, or the standard error dict `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}` on failure (including when the script was added but the republish failed). An invalid JSON script returns `{"ok": False, "error": "Widget Script must be a valid JSON string."}`.

* * * * *

### `get_dashboard_script(dashboard_id)`

Builds a **`SisenseScript`** helper from an admin export of the dashboard (`export_dashboard` / `/api/v1/dashboards/export`). Use it to read the current dashboard script in a cleaned, formatted form.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

**Returns:**

-   `SisenseScript` or `dict`: A `SisenseScript` instance on success, or the standard error dict `{"ok": False, "error": "..."}` if the export fails (with `status_code` for HTTP failures) or the dashboard has no script — a normal state, reported as an explicit "has no dashboard script" message rather than an exception.

* * * * *

### `get_widget_script(dashboard_id, widget_id)`

Builds a **`SisenseScript`** helper for one widget in the exported dashboard payload.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.
-   `widget_id` (str): Widget key in the exported dashboard’s `widgets` map (typically the widget OID / id used in that structure).

**Returns:**

-   `SisenseScript` or `dict`: A `SisenseScript` instance when the widget exists and has a script, or the standard error dict `{"ok": False, "error": "..."}` on failure — export failure (with `status_code` for HTTP failures), widget not found in the export, or the widget has no script (a normal state, reported as an explicit "has no widget script" message rather than an exception).

* * * * *

### `add_dashboard_shares(dashboard_id, shares)`

Adds or updates sharing settings for a dashboard.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

-   `shares` (list): Share definitions with `name`, `type`, and `rule`.

**Returns:**

-   `dict`: On success, `{"success": True, "message": "...", "new_shares": <n>, "updated_shares": <n>}` — the counts of shares actually written. When every requested share already exists with the same rule, the result is still a success dict with both counts at `0` ("No new or updated shares added"). On failure, the standard error dict `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}`.

* * * * *

### `get_dashboard_columns(dashboard_name)`

Extracts distinct columns used in a dashboard (filters and widgets).

Reads dashboard and default filters (plain, dependent-level and measured `filter.by`), drill hierarchies, every widget panel item (including formulas nested inside formulas, conditional-formatting expressions and drill chains), widget drill history, widget `query.metadata` and table-widget headers. A field reference found anywhere else in the dashboard is kept as well. Widgets on another datasource are included, since the dashboard references them. Both `[Table.Column]` and `[Table].[Column]` references are understood, and table or column names may contain any character — Sisense enforces no naming restriction. The `" (Calendar)"` suffix Sisense adds to date dimensions is ignored when deduplicating.

**Parameters:**

-   `dashboard_name` (str): Name of the dashboard.

**Returns:**

-   `list` or `dict`: List of unique table/column combinations. Each row carries `dashboard_name`, `source` (`"filter"`, `"hierarchy"` or `"widget"`), `widget_id` (the widget's own `oid`, or `"N/A"` for a dashboard filter), `table`, and `column`. An empty list means the dashboard genuinely references no columns. On failure (dashboard not found, or the export failed or could not be parsed), the standard error dict `{"ok": False, "error": "..."}`.

* * * * *

### `get_dashboard_share(dashboard_name)`

Retrieves share information (users and groups) for a specific dashboard by its title.

**Parameters:**

- `dashboard_name` (str): Title of the dashboard whose share settings you want to inspect.

**Returns:**

- `list` or `dict`: A list of dictionaries, each containing the type of share (`user` or `group`) and the corresponding name (email or group name). An empty list always means the dashboard genuinely has no shares. On failure (dashboard not found, or the users/groups lookup failed), the standard error dict `{"ok": False, "error": "..."}`.

* * * * *

### `get_dashboard_shares_v1(dashboard_id, admin_access=True)`

Retrieves share details using ``GET /api/v1/dashboards/{dashboard_id}/shares``. Returns the raw Sisense shares payload (for example ``sharesTo`` and ``owner``), unlike ``get_dashboard_share`` which resolves names from the admin list.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `admin_access` (bool, optional): Request with ``adminAccess=true``. Default `True`.

**Returns:**

- `dict`: Shares response on success, or the standard error dict `{"ok": False, "error": "..."}` on failure.

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

- `dict`: Updated dashboard object on success, or `{"success": True}` when the API responds 200 with an empty body. The standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `rename_dashboard(dashboard_id, title)`

Renames a dashboard by PATCHing ``title`` on ``/api/dashboards/{dashboard_id}``.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `title` (str): New dashboard title.

**Returns:**

- `dict`: Updated dashboard object on success, or `{"success": True}` when the API responds 200 with an empty body. The standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `publish_dashboard(dashboard_id, admin_access=True, force=False)`

Publishes (republishes) a dashboard via ``POST /api/v1/dashboards/{dashboard_id}/publish``. Defaults to ``adminAccess=true`` for admin-token preflight republish.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.
- `admin_access` (bool, optional): Append ``adminAccess=true``. Default `True`.
- `force` (bool, optional): Append ``force=true``. Default `False`.

**Returns:**

- `dict`: `{"success": True}` or the JSON body on success; the standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `can_be_owned(dashboard_id)`

Checks whether the dashboard can be owned by the current user via ``GET /api/v1/dashboards/{dashboard_id}/can_be_owned``.

**Parameters:**

- `dashboard_id` (str): Dashboard ``oid``.

**Returns:**

- `dict`: API response on success, or the standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `import_dashboards_bulk(dashboards, action="skip")`

Imports one or more dashboards via `POST /api/v1/dashboards/import/bulk`. Dashboards are typically the payloads returned by `export_dashboard`. The server matches dashboards by `oid`: when a dashboard with the same `oid` already exists, `action` controls whether it is left unchanged, replaced, or a new copy is created.

**Parameters:**

- `dashboards` (list): Dashboard objects to import.
- `action` (str, optional): Conflict behavior — `"skip"`, `"overwrite"`, or `"duplicate"`. Default is `"skip"`.

**Returns:**

- `dict`: The API response body, including `succeded` and `failed` lists describing the outcome for each dashboard, or the standard error dict `{"ok": False, "error": "..."}` on failure.

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

-   `list[dict]`: List of dashboard objects on success, or the standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `change_dashboard_owner(dashboard_id, new_owner_id, admin_access=True, original_owner_rule="edit")`

Transfers ownership of a dashboard to a different user via `POST /api/v1/dashboards/{dashboard_id}/change_owner`. The outgoing owner is demoted to a share entry.

Used directly when you know the new owner's user ID. Also called internally by `add_dashboard_script` and `add_widget_script` when `executing_user` is provided.

**Parameters:**

-   `dashboard_id` (str): The `oid` of the dashboard.
-   `new_owner_id` (str): The Sisense user ID (`_id`) of the new owner.
-   `admin_access` (bool, optional): Append `?adminAccess=true`. Default `True`. Pass `False` when restoring ownership back from a temporary holder.
-   `original_owner_rule` (str, optional): Share rule assigned to the outgoing owner. Default `"edit"`.

**Returns:**

-   `dict`: API response body on success, or `{"success": True}` when the API responds 200 with an empty body. The standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_widget_by_id(dashboard_id, widget_id, admin_access=True)`

Retrieves a single widget by its dashboard and widget IDs via `GET /api/v1/dashboards/{dashboard_id}/widgets/{widget_id}`.

**Parameters:**

-   `dashboard_id` (str): The `oid` of the dashboard.
-   `widget_id` (str): The `oid` of the widget.
-   `admin_access` (bool, optional): Append `?adminAccess=true`. Default `True`.

**Returns:**

-   `dict`: The full widget object on success, or the standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `update_widget(dashboard_id, widget_id, widget_data)`

Writes updated widget data back to Sisense via `PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}`. Server-managed fields (`oid`, `_id`, `owner`, `userId`, `created`, `lastUpdated`, `instanceType`, `dashboardid`) are stripped automatically before the request.

Only the dashboard owner can write widgets. Pair with `change_dashboard_owner` if the API token user is not the owner.

**Parameters:**

-   `dashboard_id` (str): The `oid` of the dashboard.
-   `widget_id` (str): The `oid` of the widget.
-   `widget_data` (dict): Full widget payload with the desired changes applied. Obtain the current widget from `get_widget_by_id`, modify the relevant fields, and pass the result here.

**Returns:**

-   `dict`: The API response body on success, or the standard error dict `{"ok": False, "error": "..."}` on failure.

* * * * *

### `find_widgets_by_type(widget_type, dashboards=None, admin_access=True, max_results=None)`

Searches for all widgets matching a given type across one or more dashboards.

**Parameters:**

-   `widget_type` (str): The widget type to match (for example `"BloX"`, `"chart"`, `"pivot"`). Case-sensitive.
-   `dashboards` (list[str] | str | None, optional): Dashboard IDs or titles to search. A bare string is treated as a single-item list. When `None` (default), all dashboards on the instance are searched.
-   `admin_access` (bool, optional): When `True` (default), enumerates all dashboards on the instance via the admin endpoint and fetches widgets using `adminAccess=true`, including dashboards owned by other users. When `False`, only dashboards visible to the API token user are scanned.
-   `max_results` (int | None, optional): Stop after this many matches. Default `None` (no limit).

**Returns:**

-   `list[dict]`: Match records, each containing `dashboard_id`, `dashboard_title`, `widget_id`, `widget_title`, and `widget_type`. Returns an empty list when no matches are found.

* * * * *

### `get_dashboards_by_datasource(datamodel, deep=False)`

Finds every dashboard that uses a data model, including dashboards linked only through a widget. Reads the admin dashboard listing once and matches each dashboard two ways: its own `datasource` (match `"dashboard"`), or any datasource in its `widgetsDatasources` summary (match `"widget"`) — a dashboard built on model A with one widget on model B is found for both models. Titles are compared case-insensitively. With `deep=True`, dashboards whose `widgetsDatasources` summary is empty are exported in batches of 20 and their widgets inspected directly, closing the one gap the listing leaves.

**Parameters:**

- `datamodel` (str): The data model, as a title or an oid.
- `deep` (bool, optional): Also export dashboards with an empty widget-datasource summary and inspect their widgets. Slower. Defaults to `False`.

**Returns:**

- `list` or `dict`: One row per matching dashboard with `dashboard_id`, `title`, `owner` (user id), `owner_email`, `datasource_title` (the dashboard's own datasource), `match` (`"dashboard"` or `"widget"`), `folder_id` and `last_updated`. Dashboard-level matches come first. An empty list means no dashboard uses the model. On failure, the standard error dict `{"ok": False, "error": "..."}`.
