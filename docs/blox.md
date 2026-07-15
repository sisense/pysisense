# Blox Module Documentation

The `Blox` class manages custom Blox actions and BloX widget styles on a Sisense instance.

---

## Class: `Blox`

### `__init__(self, api_client=None, debug=False)`

Initializes the Blox class.

**Parameters:**

- `api_client` (SisenseClient, optional): An existing client. A new one is created if not provided.
- `debug` (bool, optional): Enable debug-level logging. Default `False`.

---

## Custom Blox Actions (`blox/core.py`)

### `get_blox_actions()`

Retrieves all custom Blox actions installed on the instance. The endpoint is selected automatically based on `operating_system`:

- Linux: `GET /api/v1/blox/getCustomActions`
- Windows: `GET /api/v1/getCustomActions/actions`

**Returns:**

- `list | dict`: List of action objects on success, or `{"error": "..."}` on failure.

---

### `save_blox_action(action)`

Creates or overwrites a custom Blox action. Linux only.

**Parameters:**

- `action` (dict): The action object. The `type` field is the unique identifier; saving an existing `type` overwrites it.

**Returns:**

- `dict`: API response on success, or `{"error": "...not supported on Windows..."}` on Windows.

---

### `delete_blox_action(action_type)`

Deletes a custom Blox action by its type identifier. Linux only.

**Parameters:**

- `action_type` (str): The `type` field of the action to delete.

**Returns:**

- `dict`: API response on success, or `{"error": "...not supported on Windows..."}` on Windows.

---

## BloX Widget Styles (`blox/widgets.py`)

### `get_blox_widget_style(dashboard_id, widget_id, admin_access=True)`

Retrieves a BloX widget's `style.currentCard` and `style.currentConfig` objects. `currentCard` holds the BloX card definition (body, actions, the `style` CSS string, and so on); `currentConfig` holds the widget configuration (`fontFamily`, `fontSizes`, and so on). Returns `{"error": "..."}` if the widget is not of type `"BloX"`.

**Parameters:**

- `dashboard_id` (str): The `oid` of the dashboard.
- `widget_id` (str): The `oid` of the BloX widget.
- `admin_access` (bool, optional): Append `?adminAccess=true`. Default `True`.

**Returns:**

- `dict`: On success: `{"currentCard": dict, "currentConfig": dict}`. On failure: `{"error": "..."}`.

---

### `update_blox_widget_style(dashboard_id, widget_id, current_card=None, current_config=None, executing_user_id=None)`

Updates a BloX widget's `style.currentCard` and/or `style.currentConfig` objects. Reads the current widget, replaces the provided objects wholesale, and writes back via `PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}`. Server-managed fields are stripped before the write.

The typical flow is read-modify-write: fetch the objects with `get_blox_widget_style`, change the fields you need, and pass the modified objects back. Omitted objects are left unchanged. When neither object is provided, returns the current style immediately without writing.

When `executing_user_id` is provided, ownership of the dashboard is temporarily transferred to that user before the write, then restored in a `finally` block regardless of write success or failure. Pass the Sisense user ID (not email); use `AccessManagement.get_my_user()` to look up the ID of the API token user.

**Parameters:**

- `dashboard_id` (str): The `oid` of the dashboard.
- `widget_id` (str): The `oid` of the BloX widget.
- `current_card` (dict | None, optional): Replacement for the `style.currentCard` object. Omit to leave unchanged.
- `current_config` (dict | None, optional): Replacement for the `style.currentConfig` object. Omit to leave unchanged.
- `executing_user_id` (str | None, optional): Sisense user ID to use for the temporary ownership swap. Required when the API token user is not the dashboard owner.

**Returns:**

- `dict`: On success: `{"currentCard": dict, "currentConfig": dict}` reflecting the values after the update. On failure: `{"error": "..."}`.
