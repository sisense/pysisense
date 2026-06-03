# Blox Example Usage

This guide demonstrates how to use the `Blox` class from the `pysisense` package to manage custom Blox actions on a Sisense instance. All endpoints are supported on Linux deployments only.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Use a Sisense **admin** API token.

```python
import os
import json
from pysisense import SisenseClient, Blox

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

blox = Blox(api_client=api_client)
```

---

## Example 1: Get All Blox Actions

Retrieve every custom Blox action installed on the instance.

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

```python
response = blox.delete_blox_action("MyCustomAction")
print(response)
# {"success": True}
```

---

## Notes

- All three endpoints require a Linux Sisense deployment.
- The `type` field is the unique identifier for a Blox action.
- Saving an action whose `type` already exists will overwrite it.
