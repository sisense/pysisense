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

### `add_dashboard_script(dashboard_id, script, executing_user=None)`

Adds or updates a script at the dashboard level.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

-   `script` (str or JSON): Script content.

-   `executing_user` (str, optional): Username to assume temporary ownership.

**Returns:**

-   `str`: Success or error message.

* * * * *

### `add_widget_script(dashboard_id, widget_id, script, executing_user=None)`

Adds or updates a script to a specific widget.

**Parameters:**

-   `dashboard_id` (str): Dashboard ID.

-   `widget_id` (str): Widget ID.

-   `script` (str or JSON): Script content.

-   `executing_user` (str, optional): Username to assume temporary ownership.

**Returns:**

-   `str`: Success or error message.

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
