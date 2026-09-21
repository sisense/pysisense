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

### `update_blox_widget_style(dashboard_id, widget_id, current_card=None, current_config=None, executing_user_id=None, act_as_owner=False)`

Updates a BloX widget's `style.currentCard` and/or `style.currentConfig` objects. Reads the current widget, replaces the provided objects wholesale, and writes back via `PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}`. Server-managed fields are stripped before the write.

The typical flow is read-modify-write: fetch the objects with `get_blox_widget_style`, change the fields you need, and pass the modified objects back. Omitted objects are left unchanged. When neither object is provided, returns the current style immediately without writing.

**Copies.** With Dashboard Co-Authoring on (system setting `dashboardCoAuthoring`), a published dashboard has a shared copy — what viewers see — and a private copy per owner; a write without `sharedMode=true` reaches only the private copy. The widget is written on both copies, shared first with `sharedMode=true`, and the dashboard is republished (never with `force=true`, which empties the owner's private copy under co-authoring); the result carries `published` (and `publish_error`). A never-published dashboard, or an instance with the feature off, has a single copy and is written once.

**Ownership.** Only the owner may write. A non-owner is refused before anything is written, with `owner` and `co_owners` named — unless `act_as_owner=True` and the token belongs to an administrator, in which case ownership is transferred to the token's user for the duration of the change (`POST /api/v1/dashboards/{id}/change_owner`) and ownership and the exact share list are restored afterwards, even when the write fails; the result then carries `ownership_transferred_temporarily: True` and `original_owner`. `executing_user_id` (a Sisense user ID) is the older form of the same thing and is deprecated in favour of `act_as_owner`: when given, ownership is borrowed for that user instead of the token's user.

**Parameters:**

- `dashboard_id` (str): The `oid` of the dashboard.
- `widget_id` (str): The `oid` of the BloX widget.
- `current_card` (dict | None, optional): Replacement for the `style.currentCard` object. Omit to leave unchanged.
- `current_config` (dict | None, optional): Replacement for the `style.currentConfig` object. Omit to leave unchanged.
- `executing_user_id` (str | None, optional): Deprecated in favour of `act_as_owner`; the Sisense user ID to borrow ownership for.
- `act_as_owner` (bool, optional): Take ownership temporarily when the token's user is an administrator but not the owner. Defaults to `False`: refuse instead.

**Returns:**

- `dict`: On success: `{"currentCard": dict, "currentConfig": dict}` reflecting the values after the update, plus `published` (and `publish_error`) when a shared copy was written and `ownership_transferred_temporarily` / `original_owner` when ownership was borrowed. On failure the standard error dict `{"ok": False, "error": "..."}`, with `owner` and `co_owners` when the token's user is not the owner.
