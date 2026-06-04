# Blox Example Usage

This guide demonstrates how to use the `Blox` class from the `pysisense` package to manage custom Blox actions on a Sisense instance.

`get_blox_actions` works on both Linux and Windows deployments (each uses a different API endpoint). `save_blox_action` and `delete_blox_action` are supported on Linux only.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Use a Sisense **admin** API token.
- Set `operating_system: linux` (default) or `operating_system: windows` in your config.

```python
import os
import json
from pysisense import SisenseClient, Blox

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

blox = Blox(api_client=api_client)
```

For a Windows Sisense server, either set `operating_system: windows` in your YAML file, or pass it directly:

```python
api_client = SisenseClient(
    domain="192.168.1.200",
    token="your-api-token",
    is_ssl=False,
    operating_system="windows",
)
blox = Blox(api_client=api_client)
```

---

## Example 1: Get All Blox Actions

Retrieve every custom Blox action installed on the instance.

The correct endpoint is selected automatically based on `operating_system`:
- Linux: `GET /api/v1/blox/getCustomActions`
- Windows: `GET /api/v1/getCustomActions/actions`

```python
response = blox.get_blox_actions()
print(json.dumps(response, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(response)
print(df)

# Export to CSV
api_client.export_to_csv(response, "all_blox_actions.csv")
```

---

## Example 2: Save a Blox Action

Create or overwrite a custom Blox action. Pass the action object directly.

Supported on Linux only.

```python
action = {
    "type": "MyCustomAction",
    "body": "console.log(payload);",
}

response = blox.save_blox_action(action)
print(response)
# {"success": True}
```

---

## Example 3: Delete a Blox Action

Delete a Blox action by its type identifier.

Supported on Linux only.

```python
response = blox.delete_blox_action("MyCustomAction")
print(response)
# {"success": True}
```

---

## Notes

- The `type` field is the unique identifier for a Blox action.
- Saving an action whose `type` already exists will overwrite it.
- Save and delete use Linux-only endpoints. On Windows, only reading actions is supported.
