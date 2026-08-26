# Dashboard operations reference

`from pysisense import Dashboard` — `dashboard = Dashboard(api_client=api_client)`

## Listing and fetching

| Method | Use when |
|---|---|
| `get_all_dashboards()` | Admin-wide listing — every dashboard on the instance, regardless of owner/shares. Requires elevated access. This is what "find all dashboards owned by X" scripts should use. |
| `get_dashboards(fields=None)` | Dashboards the **current API token user** owns or is shared to. Pass `fields=["oid", "title", "owner"]` to trim the payload. |
| `get_dashboard_by_id(dashboard_id)` | Single dashboard, admin endpoint, by `oid`. |
| `get_dashboard_by_name(dashboard_name)` | Single dashboard, admin endpoint, by title. |
| `resolve_dashboard_reference(ref)` | Accepts either an ID or a title; returns `{"success": bool, "dashboard_id": ..., "dashboard_title": ..., "status_code": ...}`. Use this at the top of a script when the reference could be either. |
| `get_dashboard_widgets(ref)` | Widget payloads from the admin export; accepts ID or title. |
| `export_dashboard(dashboard_id)` | Full dashboard export object (`title`, `oid`, `script`, `widgets`, `layout`, `filters`) — the source payload for `import_dashboards_bulk`. |

Dashboard objects use `"owner"` for the owning user's internal `_id` (not `"ownerId"` — that's the *payload key* used only when calling `change_dashboard_owner`).

**Pagination on large environments.** `get_all_dashboards()` and `get_dashboards()` don't expose pagination — on an environment with hundreds of dashboards this can be a slow, unwieldy single request. Their underlying endpoints (`/api/v1/dashboards/admin?dashboardType=owner` and `/api/v1/dashboards` respectively) accept standard `skip`/`limit` query params, so page through directly via `api_client.get(...)` when that matters:

```python
dashboards, skip, page_size = [], 0, 50
while True:
    response = api_client.get("/api/v1/dashboards/admin", params={"dashboardType": "owner", "skip": skip, "limit": page_size})
    if response is None or not response.ok:
        raise RuntimeError(f"Failed to retrieve dashboards: {response.text[:500] if response else 'no response'}")
    page = response.json()
    if not page:
        break
    dashboards.extend(page)
    skip += page_size
```

This bypasses the mixin's `{"error": ...}` wrapping — check `response.ok`/`response.status_code` yourself, as above.

## Ownership transfer

`change_dashboard_owner(dashboard_id, new_owner_id, *, admin_access=True, original_owner_rule="edit")`

- `new_owner_id` must be a Sisense internal user ID (`_id`), not an email — resolve with `access_mgmt.get_user(email)["USER_ID"]` first.
- `admin_access=True` (default): required whenever the API token user is **not** the dashboard's current owner — the normal case for an admin script transferring between two other users.
- `admin_access=False`: use only when restoring ownership back to a user who is already the effective owner at call time (e.g. the token user itself, after a temporary hop — see `executing_user` below).
- The previous owner is demoted to a share entry with `original_owner_rule` (default `"edit"`) rather than losing access outright.
- Returns the API response, or `{"success": True}` on an empty 200, or `{"error": "..."}`.

For bulk transfers, resolve both users once, filter `get_all_dashboards()` by `owner == from_user_id`, and call `change_dashboard_owner` per dashboard — see SKILL.md Worked Example 1 for the full dry-run-safe pattern.

## Temporary ownership hop (scripts, not permanent transfers)

`add_dashboard_script` / `add_widget_script` accept `executing_user` (the Sisense **username**, i.e. the API token user's own identity) because **only the dashboard owner can modify scripts**. When passed, the SDK:
1. Temporarily changes ownership to `executing_user`,
2. Applies the script,
3. Restores the original owner and shares.

This is narrower than `change_dashboard_owner` — use it only when the goal is "run a script-modifying operation as owner," not "permanently reassign this dashboard." `add_widget_script` also republishes the dashboard afterward so the change takes effect.

```python
dashboard.add_dashboard_script(dashboard_id, script, executing_user="admin@sisense.com")
```

A failed PUT with HTTP 403 on either method usually means the ownership hop didn't happen — check the token user has admin rights.

## Sharing

`add_dashboard_shares(dashboard_id, shares)` — `shares` is a list of `{"name": <email or group name>, "type": "user"|"group", "rule": "view"|"edit"}`. Names are resolved to `shareId` internally; unresolvable entries are skipped (logged, not raised). Only new/changed shares are POSTed — existing untouched shares are preserved. Returns a human-readable summary string, not a dict — check for a leading `"Error"`/`"Exception"` rather than an `"error"` key.

```python
shares = [
    {"name": "john.doe@sisense.com", "type": "user", "rule": "edit"},
    {"name": "viewers-group", "type": "group", "rule": "view"},
]
result = dashboard.add_dashboard_shares(dashboard_id, shares)
```

Reading shares back:
- `get_dashboard_share(dashboard_name)` — resolved to human-readable `{"type", "name"}` entries, by title.
- `get_dashboard_shares_v1(dashboard_id, *, admin_access=True)` — raw API payload (`sharesTo`, `owner`), by ID.

## Folder placement, renaming, publishing

```python
dashboard.move_dashboard_to_folder(dashboard_id, folder_id)
dashboard.rename_dashboard(dashboard_id, "New Title")
dashboard.publish_dashboard(dashboard_id, admin_access=True, force=False)  # republish after ownership/share/script changes
dashboard.can_be_owned(dashboard_id)  # check eligibility before attempting change_dashboard_owner
```

## Bulk import / cross-environment

`import_dashboards_bulk(dashboards, action="skip")` — `action` is `"skip"` (leave existing untouched), `"overwrite"` (replace), or `"duplicate"` (create alongside). Dashboards are matched by `oid`, so this is typically fed the output of `export_dashboard()` from another environment. For full cross-environment dashboard migration (including shares/owner remapping), use `MergeTool.migrate_dashboards` / `migrate_all_dashboards` instead of calling this directly — see `examples/mergetool_example.md`.

## Widgets

```python
widget = dashboard.get_widget_by_id(dashboard_id, widget_id)
widget["title"] = "Updated Title"
dashboard.update_widget(dashboard_id, widget_id, widget)  # server-managed fields stripped automatically

# Search across the instance (or a subset of dashboards) by widget type
dashboard.find_widgets_by_type(
    "BloX",
    dashboards=None,          # None = every dashboard; or a list of IDs/titles
    admin_access=True,
    max_results=None,
)
```

## Dashboard/widget scripts

`get_dashboard_script(dashboard_id)` / `get_widget_script(dashboard_id, widget_id)` return a `SisenseScript` helper (not a plain string):

```python
script_obj = dashboard.get_dashboard_script(dashboard_id)
if isinstance(script_obj, dict) and "error" in script_obj:
    print(script_obj["error"])
else:
    print(script_obj.to_text())      # cleaned JS, boilerplate stripped
    print(script_obj.to_md())        # markdown title + fenced block
    script_obj.to_file("results/dashboard_script.js")
```
