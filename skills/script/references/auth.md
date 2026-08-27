# Auth & connection boilerplate

## Config file

Never hardcode a token or domain. Load from YAML via `SisenseClient(config_file=...)`.

```yaml
domain: ""          # IP or domain — no protocol, no port
is_ssl: true         # true for HTTPS, false uses HTTP (default port 30845)
# port: 30845        # optional, HTTP port when is_ssl is false
token: ""            # Sisense Admin API token
# verify_ssl: false  # verify the server's TLS certificate; defaults to true
# ssl_path: ""       # optional CA bundle file/dir for self-signed/internal certs
# retries: false     # auto-retry transient server errors (429/500/502/503/504); defaults to true
```

- **Use a dedicated Sisense admin user's token.** Ownership changes, migrations, and most bulk/admin operations fail or behave inconsistently with a scoped/non-admin user.
- **TLS verification is on by default.** Only set `verify_ssl: false` for trusted internal networks with self-signed certs — it exposes the token to on-path interception, and the SDK logs a warning + emits `UserWarning` when disabled. Prefer `ssl_path` pointing at a CA bundle over disabling verification entirely.
- **Non-SSL default ports**: `30845` (Linux), `8081` (Windows). Override with `port` in the YAML.
- Never commit `config.yaml`, `source.yaml`, or `target.yaml` — they contain real tokens.

## Single-environment init pattern

```python
import os
from pysisense import SisenseClient, AccessManagement, Blox, CustomCode, Dashboard, DataModel, Encryption, Folder, MergeTool, Metadata, Plugins, Queries, ReportManager, WellCheck

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

# Only instantiate the classes the script actually needs:
access_mgmt = AccessManagement(api_client=api_client)
dashboard = Dashboard(api_client=api_client)
folder = Folder(api_client=api_client)
datamodel = DataModel(api_client=api_client)
```

All top-level classes share the same `api_client`, so a single `SisenseClient` instance powers every module in one script — never construct more than one `SisenseClient` unless the script deliberately targets two environments (migrations).

## Cross-environment (migration) init pattern

Two ways to wire up `Migration` / `MergeTool` for source → target scripts:

```python
from pysisense import Migration

# 1. YAML config files
m = Migration(source_yaml="source.yaml", target_yaml="target.yaml", debug=False)

# 2. Pass SisenseClient instances directly (e.g. already built in the script)
from pysisense import SisenseClient
src = SisenseClient(config_file="source.yaml")
tgt = SisenseClient(config_file="target.yaml")
m = Migration(source_client=src, target_client=tgt)
```

`MergeTool` follows the same two-mode pattern for notebooks, folders, BloX actions, groups, users, data models, data security, saved formulas/filters, and dashboards.

Progress can be streamed instead of using the default `print`:

```python
m._emit = my_progress_callback
```

## Logging

- File-only logging to `logs/pysisense.log` in the script's working directory — created automatically, rotated daily, 7 days retained.
- Pass `debug=True` to `SisenseClient(...)` while developing a new script; it's the difference between a silent failure and a readable trail.
- Never `print` secrets, and never manually log a raw request/response payload — if a script genuinely needs to log a full payload, pass it through `redact_secrets()` from `pysisense.utils` first (the SDK already does this at its own request chokepoint).

## Exporting results

Every module shares these two helpers off `api_client`:

```python
df = api_client.to_dataframe(result)
api_client.export_to_csv(result, "output.csv")
```

## Decoding the token identity

Useful when a script needs to confirm which account it's running as (e.g. before a bulk operation):

```python
identity = api_client.decode_bearer_token()
# or, via AccessManagement:
me = access_mgmt.get_my_user()
```
