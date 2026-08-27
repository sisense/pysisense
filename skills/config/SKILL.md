---
name: config
description: Use when the user needs to create, validate, or troubleshoot a pysisense connection config — `config.yaml` for a single-environment script, or `source.yaml`/`target.yaml` for a migration. Triggers on requests like "help me set up the config for this script", "what port should I use", "SSL certificate error connecting to Sisense", "the token isn't working", or "set up source/target config for a migration". Not for the surrounding project shell (folder, `uv init`, README) — that's the `scaffold` skill; not for the Python init code pattern — that's covered in `script`'s `references/auth.md`.
---

# pysisense connection config

Generate, validate, and troubleshoot the YAML connection file(s) a pysisense script needs. This skill owns the *content and correctness* of the config; it doesn't create the surrounding project (use `scaffold` for that) and doesn't cover the Python-side `SisenseClient(config_file=...)` init pattern (see `script`'s [`references/auth.md`](../script/references/auth.md) for that).

## All fields

```yaml
domain: "your-domain-or-IP"      # IP address or hostname — no protocol, no port
is_ssl: true                     # true for HTTPS (port 443), false for HTTP
# port: 30845                    # optional, HTTP port when is_ssl is false — omit to use the OS default
token: "your-api-token-here"     # Sisense Admin API token
# operating_system: linux        # "linux" (default) or "windows" — controls OS-specific API routing AND the default non-SSL port
# verify_ssl: true                # verify the server's TLS certificate; defaults to true
# ssl_path: /path/to/ca-bundle.pem  # CA bundle file/dir for self-signed/internal certs; takes precedence over verify_ssl unless verify_ssl is explicitly false
# retries: true                   # auto-retry transient server errors (429/500/502/503/504) on GET/PUT/DELETE; defaults to true
```

Field notes — ask the user for these if not already known, don't guess:

- **`token`** must belong to a dedicated Sisense **admin** user. Ownership changes, migrations, and most bulk/admin operations fail or behave inconsistently with a scoped/non-admin user's token.
- **`operating_system`** determines the non-SSL port default: `30845` for `linux` (the default if omitted), `8081` for `windows`. Getting this wrong on a Windows deployment with no explicit `port` set is a common cause of connection failures — ask which OS the Sisense server runs on if it's not obvious from context.
- **`verify_ssl: false`** disables TLS certificate verification entirely — only for trusted internal networks with self-signed certs, and the SDK logs a warning + emits `UserWarning` when it's set. Prefer `ssl_path` pointing at a CA bundle instead; it verifies against that bundle without disabling verification.
- **`ssl_path`** takes precedence over `verify_ssl` when both are set, *unless* `verify_ssl` is explicitly `false` — in which case verification stays fully disabled and `ssl_path` is ignored.
- **`retries`** only affects idempotent methods (`GET`, `PUT`, `DELETE`) — `POST`/`PATCH` are never auto-retried, so a script failing without retry on a `create_*`/`update_*` call is expected behavior, not a config bug.

## Generating a config file

Ask (or infer from context) what's needed, then write the file:

1. Single environment → one `config.yaml`.
2. Migration/mergetool script → `source.yaml` + `target.yaml` (same field set, one per environment), unless the user wants to pass `SisenseClient` instances directly in code instead (see `script`'s `references/auth.md` for that alternative — no YAML file needed in that case).

Only include commented-out fields the user is likely to need — don't dump every optional field into every generated file. A minimal, working file is `domain`, `is_ssl`, and `token`; add `operating_system`/`port`/`verify_ssl`/`ssl_path`/`retries` only when the deployment actually needs them.

Never write a real token into a file that will be committed. If scaffolding a new project (see the `scaffold` skill), the real `config.yaml` is `.gitignore`d and a placeholder `config.example.yaml` is what gets committed.

## Validating a config file

There's no standalone `pysisense` config-validation command — the practical check is instantiating the client and making one cheap authenticated call:

```python
import os
from pysisense import SisenseClient, AccessManagement

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)
access_mgmt = AccessManagement(api_client=api_client)

me = access_mgmt.get_my_user()
if isinstance(me, dict) and "error" in me:
    print(f"Config check failed: {me['error']}")
else:
    print(f"OK — authenticated as {me.get('email', me)}")
```

This confirms `domain`, `port`, `is_ssl`, `token`, and SSL settings are all correct in one call, and `debug=True` writes a full trail to `logs/pysisense.log` if it fails.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| SSL certificate verify failed | Self-signed or internal CA cert | Set `ssl_path` to the CA bundle. Only fall back to `verify_ssl: false` if a CA bundle genuinely isn't available. |
| Connection refused / timeout on a non-SSL connection | Wrong `port` for the target OS | Set `operating_system` correctly, or set `port` explicitly — don't assume `30845` on a Windows deployment. |
| `401`/`403` on most calls | Invalid/expired token, or a non-admin token | Regenerate the token from an admin user in Sisense; scoped/non-admin tokens fail on bulk/admin operations even when they succeed on reads. |
| A `create_*`/`update_*` call fails once with no retry, but a `get_*` call retries fine | Not a bug | `POST`/`PATCH` are never auto-retried by design (avoids duplicating a side effect the server may have already applied). |
| Migration script can't find `source.yaml`/`target.yaml` | Two init modes exist | Either provide both YAML files, or pass `SisenseClient` instances directly via `source_client=`/`target_client=` — check which pattern the script actually uses. |

## When something isn't covered here

Check `SisenseClient.__init__`'s docstring in `pysisense/sisenseclient.py` for the exact precedence rules between constructor kwargs and YAML keys (constructor args always win when explicitly passed) before guessing at an edge case.
