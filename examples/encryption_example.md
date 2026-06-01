# Encryption Example Usage

Use the `Encryption` class when migrating datamodels that contain encrypted connection parameters between Sisense environments.

---

## Prerequisites

```python
import os
import json
from pysisense import SisenseClient, Encryption

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

encryption = Encryption(api_client=api_client)
```

---

## Example 1: Encrypt a Connection Parameter

```python
response = encryption.encrypt({"value": "my-secret-password"})
print(json.dumps(response, indent=4))
```

---

## Example 2: Decrypt a Connection Parameter

```python
encrypted_value = "..."  # From exported .smodel or connection metadata
response = encryption.decrypt({"value": encrypted_value})
print(json.dumps(response, indent=4))
```

---

## Notes

- Do not log or commit encrypted or decrypted secrets.
- Payload field names follow your Sisense version's REST API reference (commonly `value`).
