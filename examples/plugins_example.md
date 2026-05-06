# Plugins Example Usage

This guide demonstrates how to use the `Plugins` class from the `pysisense` package to list, enable, disable, and snapshot Sisense plugins. Each example includes a short description and a code snippet you can copy into your own scripts.

---

## Prerequisites

- Import required modules and initialize the API client and `Plugins` class.

```python
import os
import json
from pysisense import SisenseClient, Plugins

# Option A — YAML config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

# Option B — inline credentials (no config.yaml required)
api_client = SisenseClient(
    domain="https://your-domain.sisense.com",
    token="YOUR_API_TOKEN",
)

# Initialize the Plugins class using the shared client
plugins = Plugins(api_client=api_client)
```

---

## Example 1: List All Plugins

Retrieve every plugin installed on the instance, including its current enabled state.

```python
response = plugins.get_all_plugins()
print(json.dumps(response, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(response)
print(df)

# Export to CSV
api_client.export_to_csv(response, "all_plugins.csv")
```

Each plugin object contains:

| Field | Description |
|---|---|
| `name` | API identifier (e.g. `"AdditionalInfoTooltip"`) |
| `folderName` | Filesystem folder name (e.g. `"plugin-AdditionalInfoTooltip"`) |
| `isEnabled` | `true` if the plugin is currently active |

---

## Example 2: Get a Single Plugin

Look up one plugin by its name or folder name. The `"plugin-"` prefix is optional.

```python
# By API name
response = plugins.get_plugin("AdditionalInfoTooltip")
print(json.dumps(response, indent=4))

# By folder name (prefix is stripped automatically)
response = plugins.get_plugin("plugin-CustomTodayFilter")
print(json.dumps(response, indent=4))
```

---

## Example 3: Enable a Single Plugin

Enable one plugin. If the plugin is already enabled, no API call is made.

```python
response = plugins.enable_plugin("AdditionalInfoTooltip")
print(response)
# {"folderName": "plugin-AdditionalInfoTooltip", "isEnabled": True, "changed": True}
```

---

## Example 4: Disable a Single Plugin

Disable one plugin. If the plugin is already disabled, no API call is made.

```python
response = plugins.disable_plugin("plugin-CustomTodayFilter")
print(response)
# {"folderName": "plugin-CustomTodayFilter", "isEnabled": False, "changed": True}
```

---

## Example 5: Enable Multiple Plugins (Bulk)

Enable a list of plugins in a single PATCH request (default behavior).

```python
plugin_names = [
    "AdditionalInfoTooltip",
    "plugin-CustomTodayFilter",
    "Sync Dashboard Filters",
]

response = plugins.enable_plugins(plugin_names)
print(json.dumps(response, indent=4))
# {
#   "changed": ["plugin-AdditionalInfoTooltip", "plugin-CustomTodayFilter", ...],
#   "already_enabled": [],
#   "not_found": [],
#   "errors": []
# }
```

---

## Example 6: Enable Multiple Plugins (One Request Per Plugin)

Use `bulk=False` to send a separate PATCH request for each plugin. This is useful for debugging or when the Sisense instance does not support bulk updates reliably.

```python
response = plugins.enable_plugins(
    ["AdditionalInfoTooltip", "CustomTodayFilter"],
    bulk=False,
)
print(json.dumps(response, indent=4))
```

---

## Example 7: Disable Multiple Plugins (Bulk)

Disable a list of plugins in a single PATCH request.

```python
plugin_names = [
    "plugin-AdditionalInfoTooltip",
    "plugin-CustomTodayFilter",
]

response = plugins.disable_plugins(plugin_names)
print(json.dumps(response, indent=4))
```

---

## Example 8: Save a Snapshot

Capture the current plugin enable/disable state. Store the returned dict however you like — write it to a file, a database, or pass it directly to `restore_snapshot` later.

```python
import json

snapshot = plugins.save_snapshot()
print(f"Snapshot taken at {snapshot['created']}")
print(f"Enabled plugins: {len(snapshot['plugins'])}")

# Persist to disk
with open("my_snapshot.json", "w") as f:
    json.dump(snapshot, f, indent=2)
```

The snapshot dict has this shape:

```json
{
  "created": "2026-05-06T14:30:00Z",
  "plugins": [
    "plugin-AdditionalInfoTooltip",
    "plugin-CustomTodayFilter"
  ]
}
```

---

## Example 9: Restore a Snapshot

Bring the instance back to the exact state captured in a snapshot. Plugins in the snapshot that are currently off will be enabled; plugins not in the snapshot that are currently on will be disabled.

```python
import json

# Load the previously saved snapshot
with open("my_snapshot.json") as f:
    snapshot = json.load(f)

response = plugins.restore_snapshot(snapshot)
print(json.dumps(response, indent=4))
# {
#   "enabled":          ["plugin-AdditionalInfoTooltip"],
#   "disabled":         ["plugin-SomeOtherPlugin"],
#   "already_set":      25,
#   "not_in_instance":  [],
#   "errors":           []
# }
```

Use `bulk=False` to apply one change at a time:

```python
response = plugins.restore_snapshot(snapshot, bulk=False)
```

---

## Example 10: Safe Upgrade Workflow

A common pattern — snapshot before a change, apply changes, restore if something goes wrong.

```python
import json

# 1. Capture current state
snapshot = plugins.save_snapshot()
with open("pre_upgrade_snapshot.json", "w") as f:
    json.dump(snapshot, f, indent=2)

# 2. Apply changes (enable a new set of plugins)
plugins.enable_plugins(["plugin-NewFeaturePlugin"])

# 3. If something goes wrong, roll back
with open("pre_upgrade_snapshot.json") as f:
    snapshot = json.load(f)

plugins.restore_snapshot(snapshot)
print("Rolled back to pre-upgrade state.")
```
