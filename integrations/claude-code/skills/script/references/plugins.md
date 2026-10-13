# Plugin management reference

`from pysisense import Plugins` — `plugins = Plugins(api_client=api_client)`

Manages plugin enable/disable state and snapshots. Does **not** install or remove plugin packages from the filesystem.

## Listing and lookup

| Method | Use when |
|---|---|
| `get_all_plugins()` | Every plugin on the instance. Paginates `GET /api/v1/plugins` internally and returns one flat list of `{"name", "folderName", "isEnabled"}` objects. Returns `[{"ok": False, "error": "..."}]` (a list, not a dict) on failure. |
| `get_plugin(plugin)` | One plugin by `name` or `folderName`. Matching is case-insensitive and the `"plugin-"` prefix is optional on either side — `get_plugin("CustomTodayFilter")` and `get_plugin("plugin-CustomTodayFilter")` both work. Returns `{"ok": False, "error": "..."}` if not found. |

## Single enable/disable

```python
plugins.enable_plugin("AdditionalInfoTooltip")
# {"folderName": "plugin-AdditionalInfoTooltip", "isEnabled": True, "changed": True}

plugins.disable_plugin("plugin-CustomTodayFilter")
# {"folderName": "plugin-CustomTodayFilter", "isEnabled": False, "changed": True}
```

If the plugin is already in the target state, no PATCH is sent — `"changed": False` comes back instead. `{"ok": False, "error": "..."}` on lookup failure or a failed PATCH.

## Bulk enable/disable

`enable_plugins(plugins, bulk=True)` / `disable_plugins(plugins, bulk=True)` — accepts a single name (`str`) or a `list[str]`; same name/prefix matching as `get_plugin`.

```python
result = plugins.enable_plugins(["AdditionalInfoTooltip", "plugin-CustomTodayFilter", "Sync Dashboard Filters"])
# {"changed": [...], "already_enabled": [...], "not_found": [...], "errors": []}
```

- `bulk=True` (default): all changed plugins go out in **one** PATCH request.
- `bulk=False`: one PATCH per plugin — slower, but isolates failures per-plugin instead of failing the whole batch. `errors` (list of `folderName`) is only ever populated in this mode; a bulk-mode PATCH failure short-circuits and returns `{"ok": False, "error": "..."}` for the whole call instead.
- Plugins already in the target state are skipped (no API call) and land in `already_enabled` / `already_disabled` instead of `changed`.
- Unmatched names land in `not_found`, not an error.

## Snapshots — capture and rollback

A snapshot records only which plugins were **enabled** at capture time (a sorted list of `folderName`, plus an ISO 8601 UTC `created` timestamp) — it says nothing about plugins outside that set beyond "not enabled."

```python
snapshot = plugins.save_snapshot()
# {"created": "2026-05-06T14:30:00Z", "plugins": ["plugin-AdditionalInfoTooltip", "plugin-CustomTodayFilter"]}
```

`restore_snapshot(snapshot, bulk=True)` diffs the snapshot against the instance's live state and applies the minimal delta: enables anything in the snapshot that's currently off, disables anything currently on that isn't in the snapshot. Requires the `"plugins"` key — `{"ok": False, "error": "..."}` if missing.

```python
result = plugins.restore_snapshot(snapshot)
# {"enabled": [...], "disabled": [...], "already_set": 25, "not_in_instance": [...], "errors": []}
```

- `not_in_instance` — snapshot entries whose `folderName` no longer exists on the instance (e.g. uninstalled since capture).
- `already_set` is a count, not a list.
- Same `bulk` semantics as `enable_plugins`/`disable_plugins`: `bulk=False` isolates per-plugin errors instead of failing the whole restore.

Typical use: snapshot immediately before a risky bulk change (upgrade, batch enable of a new plugin set), apply the change, and `restore_snapshot` the saved dict if something breaks. Persist the snapshot dict as plain JSON between the two steps — it's just `created` + a string list.

```python
snapshot = plugins.save_snapshot()
plugins.enable_plugins(["plugin-NewFeaturePlugin"])
# ...something goes wrong...
plugins.restore_snapshot(snapshot)
```
