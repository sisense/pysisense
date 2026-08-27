# Blox operations reference

`from pysisense import Blox` — `blox = Blox(api_client=api_client)`

`Blox` manages custom Blox actions (OS-routed reads, Linux-only writes) and BloX widget styles.

## Custom Blox actions

| Method | OS support | Use when |
|---|---|---|
| `get_blox_actions()` | Linux + Windows (OS-routed) | List every custom Blox action installed on the instance. |
| `save_blox_action(action)` | Linux only | Create a new action, or overwrite an existing one by `type`. |
| `delete_blox_action(action_type)` | Linux only | Remove an action by its `type` identifier. |

`get_blox_actions()` picks the endpoint from `api_client.operating_system`:
- Linux: `GET /api/v1/blox/getCustomActions`
- Windows: `GET /api/v1/getCustomActions/actions`

Returns a `list[dict]` of action objects, or `[{"error": "..."}]` on failure (note: the error is wrapped in a one-item **list**, not a bare dict, since the success shape is a list).

```python
actions = blox.get_blox_actions()
```

`save_blox_action` / `delete_blox_action` are Linux-only. On a Windows-configured client they short-circuit before any request and return:

```python
{"error": "save_blox_action is not supported on Windows deployments."}
{"error": "delete_blox_action is not supported on Windows deployments."}
```

On Linux, both return the parsed JSON body on success, or `{"success": True}` if the response has no content:

```python
action = {"type": "MyCustomAction", "body": "console.log(payload);"}
blox.save_blox_action(action)  # overwrites if "type" already exists

blox.delete_blox_action("MyCustomAction")
```

## BloX widget styles

| Method | Use when |
|---|---|
| `get_blox_widget_style(dashboard_id, widget_id, *, admin_access=True)` | Read a widget's `style.currentCard` and `style.currentConfig` objects. |
| `update_blox_widget_style(dashboard_id, widget_id, *, current_card=None, current_config=None, executing_user_id=None)` | Replace one or both style objects and write the widget back. |

Both operate only on widgets of type `"BloX"` — any other widget type returns `{"error": "..."}` naming the actual type found.

- `currentCard` — the BloX card definition: body, actions, and the `style` CSS string.
- `currentConfig` — widget configuration: `fontFamily`, `fontSizes`, etc.

```python
style = blox.get_blox_widget_style(dashboard_id, widget_id)
style["currentCard"]["style"]  # CSS string
style["currentConfig"]["fontFamily"]
```

`get_blox_widget_style` returns `{"currentCard": {...}, "currentConfig": {...}}` on success, `{"error": "..."}` on failure. `admin_access=True` (default) appends `?adminAccess=true`, letting the call reach dashboards the token user doesn't own.

`update_blox_widget_style` is read-modify-write: it fetches the widget itself, then replaces `style.currentCard` and/or `style.currentConfig` **wholesale** with whatever you pass — there's no partial merge of nested fields, so start from the object returned by `get_blox_widget_style`. An object you omit is left untouched.

```python
style["currentCard"]["style"] = "body { font-size: 14px; color: #333; }"
style["currentConfig"]["fontFamily"] = "Roboto"

result = blox.update_blox_widget_style(
    dashboard_id,
    widget_id,
    current_card=style["currentCard"],
    current_config=style["currentConfig"],
)
# {"currentCard": {...}, "currentConfig": {...}}
```

Passing neither `current_card` nor `current_config` makes **no write** — the method just returns the widget's current style objects.

### Ownership hop for widgets you don't own

`update_blox_widget_style` writes via `PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}`, which requires dashboard ownership. Pass `executing_user_id` (a Sisense **user ID**, not email — resolve via `AccessManagement.get_my_user()` or `get_user(email)`) to have the SDK temporarily transfer ownership, perform the write, and restore the original owner and shares in a `finally` block — restoration runs regardless of whether the write succeeds.

```python
from pysisense import AccessManagement

access_mgmt = AccessManagement(api_client=api_client)
my_user_id = access_mgmt.get_my_user()["_id"]

blox.update_blox_widget_style(
    dashboard_id,
    widget_id,
    current_card=style["currentCard"],
    executing_user_id=my_user_id,
)
```

Server-managed fields (`oid`, `_id`, `owner`, `userId`, `created`, `lastUpdated`, `instanceType`, `dashboardid`) are stripped from the payload automatically before the write — no need to remove them yourself.
