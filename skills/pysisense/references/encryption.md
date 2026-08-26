# Encryption reference

`from pysisense import Encryption` — `encryption = Encryption(api_client=api_client)`

Wraps the Sisense encryption service used to prepare or recover **connection
parameter values** (passwords, keys, tokens embedded in a data model's
connection) during cross-server data model migration. It does not encrypt
arbitrary data — only values tied to Sisense connection parameters.

## The scenario this exists for

Connection credentials captured in an exported data model schema
(`DataModel.export_datamodel_schema`) are encrypted with the **source**
server's key. Importing that schema as-is on a different server
(`DataModel.import_datamodel_schema`) leaves those values undecryptable there.
`Encryption` lets a script:

1. `decrypt` the connection parameter value(s) pulled from the source export,
2. `encrypt` the resulting plaintext against the **target** server's key
   (call `Encryption` with an `api_client` pointed at the target),
3. splice the re-encrypted value back into the schema/payload before import.

```python
source_encryption = Encryption(api_client=source_client)
target_encryption = Encryption(api_client=target_client)

plaintext = source_encryption.decrypt({"value": encrypted_param_from_export})
if "error" in plaintext:
    ...  # handle failure

reencrypted = target_encryption.encrypt({"value": plaintext["value"]})
```

## When to reach for this vs. when it's already handled

- **`DataModel.generate_connections_payload` / `create_connections`** build
  and POST a **new** connection from plaintext credentials you supply
  directly (`aws_secret_key`, `password`, `token`, etc.) — no encrypted
  value ever passes through them, so `Encryption` is not needed for that
  path. Use it when the target's connection doesn't exist yet and you're
  providing fresh credentials.
- **`MergeTool.migrate_datamodels` / `migrate_all_datamodels`** handle
  connections by repointing each dataset to an existing target connection
  (`provider_connection_map`) or stripping the embedded connection
  parameters entirely when unmapped — it never decrypts/re-encrypts
  credentials itself. `Encryption` is what you reach for only in a custom
  script that needs to carry the *actual* encrypted credential value across
  environments (e.g. working directly with `export_datamodel_schema` /
  `import_datamodel_schema` payloads) rather than remapping to a
  pre-existing connection or supplying new plaintext credentials.

## Methods

| Method | Endpoint | Payload |
|---|---|---|
| `encrypt(payload)` | `POST /api/v1/encryption/encrypt` | dict, typically `{"value": <plaintext>}` |
| `decrypt(payload)` | `POST /api/v1/encryption/decrypt` | dict, typically `{"value": <ciphertext>}` |

Both require `payload` to be a `dict` — passing anything else returns
`{"error": "payload must be a dictionary."}` without a request being made.
Field names beyond `value` follow your Sisense version's REST API reference;
the SDK passes the payload through unchanged.

## Return shape

Both methods return the raw JSON response dict on success, or
`{"error": "..."}` on failure (no response, non-OK status, or an
unparseable body falls back to `{"success": True}` only when the response
was OK but had no JSON body). There is no separate `"success"` key on a
normal successful call — check for the absence of `"error"`.

```python
result = encryption.encrypt({"value": "my-secret-password"})
if "error" in result:
    print(result["error"])
else:
    encrypted_value = result.get("value")
```

## Secrets policy

`encrypt`/`decrypt` inputs and outputs are credential material — never log
the raw `payload` or `response` dicts directly. If a script needs to log
either for debugging, pass it through `redact_secrets()` from
`pysisense.utils` first, same as the rest of the SDK's secrets policy.
`self.logger` calls inside `encrypt`/`decrypt` already avoid logging the
payload itself (only the endpoint and outcome), so the risk is entirely in
caller code that prints or logs `payload`/`result` verbatim.
