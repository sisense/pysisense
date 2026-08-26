---
name: scripting
description: Use whenever the user asks for a script, automation, or one-off task that touches Sisense — dashboards, widgets, users, groups, roles, folders, data models, data security, permissions, ownership transfers, migrations, or admin/governance operations (e.g. "transfer all dashboards from user A to user B", "list dashboards owned by X", "bulk-create these users", "audit unused columns", "share this dashboard with a group"). Also use when the user mentions the `pysisense` package, `SisenseClient`, `config.yaml` for Sisense, or asks to write/fix/debug a Python script against a Sisense instance. Triggers even if the user doesn't say "pysisense" explicitly — any request to script a Sisense admin/BI task qualifies. For setting up a brand-new script's project shell (folder, `uv init`, config template) rather than writing the script logic itself, see the `scaffolding` skill instead.
---

# pysisense automation scripts

Write Python scripts against the **pysisense SDK** — the official wrapper around the Sisense REST API used throughout this repository. Real classes, methods, and conventions only. Never invent endpoints or method names; if a needed capability isn't listed below or in `references/`, say so and grep `pysisense/<module>/` in this repo before guessing.

This skill is for **writing scripts that use pysisense as a library** (the same spirit as `examples/*.md`), not for modifying the SDK's own source. If the user's request is instead about editing pysisense internals, defer to this repo's `CLAUDE.md` conventions.

## Load order

1. Read this file fully first.
2. Auth/config boilerplate is in [`references/auth.md`](references/auth.md) — read it once per session, it rarely changes.
3. Then read whichever of these match the task, before writing the corresponding code:

| Task involves | Read |
|---|---|
| Dashboards, widgets, shares, scripts, ownership transfer | [`references/dashboards.md`](references/dashboards.md) |
| Users, groups, roles, folder+dashboard ownership transfer | [`references/users_groups.md`](references/users_groups.md) |
| Data models, connections, build/deploy, RLS, data-model shares | [`references/datamodel.md`](references/datamodel.md) |
| Folders (structure/CRUD only, not ownership) | [`references/folder.md`](references/folder.md) |
| Custom-code notebooks | [`references/custom_code.md`](references/custom_code.md) |
| BloX actions, BloX widget style | [`references/blox.md`](references/blox.md) |
| Datasource/elasticube metadata (measures, dimensions, schema introspection) | [`references/metadata.md`](references/metadata.md) |
| Running JAQL/SQL queries against a datasource | [`references/queries.md`](references/queries.md) |
| Encrypting/decrypting connection credentials for cross-server migration | [`references/encryption.md`](references/encryption.md) |
| Cross-environment migration of groups/users/dashboards/data models only | [`references/migration.md`](references/migration.md) |
| Cross-environment migration of anything else too (notebooks, folders, BloX, data security, saved formulas/filters) | [`references/mergetool.md`](references/mergetool.md) |
| Plugin enable/disable, plugin state snapshots | [`references/plugins.md`](references/plugins.md) |
| Scheduled reports (CRUD, on-demand run) | [`references/report_manager.md`](references/report_manager.md) |
| Dashboard/data-model health or complexity checks | [`references/wellcheck.md`](references/wellcheck.md) |

If a needed capability still isn't covered by any of these, grep the relevant `pysisense/<module>/*.py` file directly rather than guessing signatures — don't rely on the CLAUDE.md mixin lookup table alone, it has known staleness in at least one spot (some `access_management` methods are misattributed to `custom_code` in that table).

## Non-negotiable conventions

These scripts run against **real, often production, Sisense environments**. Treat every write operation (ownership change, delete, bulk create) as high blast-radius.

**Error handling — check dicts, don't assume success.**
Every pysisense call returns either the payload or `{"error": "..."}`. Never assume success:

```python
response = dashboard.change_dashboard_owner(dashboard_id, new_owner_id)
if isinstance(response, dict) and "error" in response:
    print(f"Failed: {response['error']}")
else:
    print("Success")
```

**No hardcoded credentials.** Tokens/domains always come from a YAML config (`config.yaml`, or `source.yaml`/`target.yaml` for migrations) loaded via `SisenseClient(config_file=...)`. Never inline a token or domain in the script body.

**Use `debug=True` while developing.** It's the only thing standing between "silent failure" and a readable trail in `logs/pysisense.log`. Never log tokens/passwords yourself — the SDK already redacts secrets in its own logging.

**Dry-run first for anything destructive or bulk.** Any script that does ownership transfers, deletions, or bulk creates/updates must default to a `dry_run=True`-style preview: print what *would* happen (counts, names, IDs) before making a single write call. Only flip to live execution on explicit confirmation (a flag, or the user re-running with `dry_run=False`). See Example 1 below for the pattern.

**Resolve names to IDs before mutating.** Ownership/share operations need Sisense internal IDs (`_id` / `USER_ID` / `GROUP_ID`), not emails or display names. Always resolve first via `get_user()` / `get_group()`, and fail fast (print and skip, don't guess) if resolution fails.

**IDs vs titles.** Dashboard- and data-model-facing methods generally accept either a 24-char ID or a title/name — check the method's docstring or `references/` before assuming. `resolve_dashboard_reference()` exists specifically to normalize an ambiguous reference.

**Escape hatch: raw `api_client.get/post/put/delete` when a mixin method can't do it.** Not every capability is wrapped — e.g. `Dashboard.get_all_dashboards()` has no pagination, so it can time out or return an unwieldy payload on an environment with hundreds of dashboards. It's fine to call the underlying endpoint directly:

```python
response = api_client.get("/api/v1/dashboards/admin", params={"dashboardType": "owner", "skip": skip, "limit": 50})
```

Rules for doing this safely:
- Only use an endpoint/param already confirmed in the SDK source (the mixin method's docstring names its endpoint) — never invent one.
- `api_client.get/post/put/delete` return a raw `requests.Response` or `None`, **not** the `{"error": ...}` dict the mixin methods return. Check `response is None`, then `response.ok` / `response.status_code`, yourself — the wrapping the mixins do internally doesn't happen for direct calls.
- Prefer the wrapped method whenever it already does the job; drop to raw calls only for a genuine capability gap (pagination, an uncommon query param), not as a default style.

**PATCH/update payloads carry only what's explicitly provided.** When building an `update_user`/`update_*` payload from user input, don't inject empty-string or empty-list defaults for fields the user didn't ask to change — especially `groups`: an explicit `[]` clears all group membership.

**Admin token required.** Ownership changes, migrations, and most bulk/admin operations require the API token to belong to a dedicated Sisense admin user. If a script fails with permission-shaped errors, that's the first thing to check — not a code bug.

## New script? Scaffold the project first

A pysisense automation script is a deliverable someone else may run, rerun, and hand off — not a scratch snippet. Unless the user explicitly asks for a quick inline snippet, or is clearly just pasting into a REPL or an existing project, the script needs a project shell first: a folder, `uv init`, a `config.example.yaml` template, a README, and a `.gitignore`.

Use the **`scaffolding`** skill (`/pysisense:scaffolding`) to create that shell before writing the script logic below into it. If a project shell already exists (the user is adding to or fixing an existing script), skip straight to writing code.

**Comments in generated script code.** This is a different bar than editing pysisense's own SDK source (which this repo's `CLAUDE.md` deliberately keeps comment-free except for non-obvious WHYs) — a generated automation script is read by whoever runs it, often without this conversation's context. Comment the non-obvious parts: why an operation order matters, what a dry-run flag gates, why a particular endpoint is called directly instead of a wrapped method. Don't narrate the obvious (`# loop over dashboards`).

## Setup boilerplate (always start here)

```python
import os
from pysisense import SisenseClient, AccessManagement, Dashboard  # add more classes as needed

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

access_mgmt = AccessManagement(api_client=api_client)
dashboard = Dashboard(api_client=api_client)
```

Full class list, config.yaml fields, SSL/retry options: [`references/auth.md`](references/auth.md).

---

## Worked example 1 — transfer all dashboards from user A to user B (dry-run safe)

The canonical "admin operation" script. Resolves both users, finds every dashboard owned by A, previews the change, and only executes on confirmation.

```python
import os
from pysisense import SisenseClient, AccessManagement, Dashboard

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)
access_mgmt = AccessManagement(api_client=api_client)
dashboard = Dashboard(api_client=api_client)

FROM_USER_EMAIL = "alice@example.com"
TO_USER_EMAIL = "bob@example.com"
DRY_RUN = True  # flip to False only after reviewing the preview output

# 1. Resolve both users to internal IDs — fail fast if either is missing.
from_user = access_mgmt.get_user(FROM_USER_EMAIL)
to_user = access_mgmt.get_user(TO_USER_EMAIL)
if "error" in from_user or "error" in to_user:
    raise SystemExit(f"Could not resolve users: from={from_user.get('error')} to={to_user.get('error')}")

from_user_id = from_user["USER_ID"]
to_user_id = to_user["USER_ID"]

# 2. Find every dashboard owned by the source user (admin endpoint sees all dashboards).
all_dashboards = dashboard.get_all_dashboards()
if isinstance(all_dashboards, dict) and "error" in all_dashboards:
    raise SystemExit(f"Failed to list dashboards: {all_dashboards['error']}")

owned = [d for d in all_dashboards if d.get("owner") == from_user_id]

print(f"Found {len(owned)} dashboard(s) owned by {FROM_USER_EMAIL}:")
for d in owned:
    print(f"  - {d.get('title')} ({d.get('oid')})")

if DRY_RUN:
    print("\nDry run — no changes made. Set DRY_RUN = False to execute.")
    raise SystemExit(0)

# 3. Execute the transfer, one dashboard at a time, logging each result.
for d in owned:
    dashboard_id = d["oid"]
    result = dashboard.change_dashboard_owner(dashboard_id, to_user_id)
    if isinstance(result, dict) and "error" in result:
        print(f"FAILED: {d.get('title')} ({dashboard_id}): {result['error']}")
    else:
        print(f"OK: {d.get('title')} ({dashboard_id}) -> {TO_USER_EMAIL}")
```

Notes:
- `change_dashboard_owner` defaults to `admin_access=True`, which is correct here since the API token user is neither the old nor new owner.
- If you need to restore ownership back to the token user afterward, pass `admin_access=False` (see `references/dashboards.md`).
- For a **temporary** ownership hop (e.g. to apply a dashboard script as owner, then hand it back), see the `executing_user` pattern in `references/dashboards.md` instead — that's a narrower, self-restoring operation, not a permanent transfer.

## Worked example 2 — audit: list dashboards + owners for governance reporting

Read-only, safe to run anytime. Good starting point when a user's request is exploratory ("how many dashboards does X own") before committing to a write operation.

```python
import os
from pysisense import SisenseClient, AccessManagement, Dashboard

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)
access_mgmt = AccessManagement(api_client=api_client)
dashboard = Dashboard(api_client=api_client)

users = access_mgmt.get_users_all()
users_by_id = {u["_id"]: u.get("email", "unknown") for u in users} if isinstance(users, list) else {}

all_dashboards = dashboard.get_all_dashboards()
rows = []
for d in all_dashboards if isinstance(all_dashboards, list) else []:
    rows.append({
        "title": d.get("title"),
        "oid": d.get("oid"),
        "owner_email": users_by_id.get(d.get("owner"), d.get("owner")),
    })

df = api_client.to_dataframe(rows)
print(df)
api_client.export_to_csv(rows, "dashboard_ownership_audit.csv")
```

## Worked example 3 — bulk-provision groups then users from a CSV

Order matters: groups must exist before users can reference them by name in `create_user`.

```python
import os
import csv
from pysisense import SisenseClient, AccessManagement

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)
access_mgmt = AccessManagement(api_client=api_client)

# 1. Create groups first (idempotency: check existing groups before creating).
existing = access_mgmt.get_groups()
existing_names = {g["name"] for g in existing} if isinstance(existing, list) else set()
new_group_names = {"Sales Team", "Finance Team"} - existing_names
if new_group_names:
    result = access_mgmt.create_groups_bulk([{"name": n} for n in new_group_names])
    print(f"Created groups: {result}")

# 2. Create users, referencing groups by name (create_user resolves names to IDs).
with open("new_users.csv", newline="", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if not row.get("email"):
            print(f"Skipping row, missing email: {row}")
            continue
        user_data = {
            "email": row["email"],
            "firstName": row.get("firstName", ""),
            "lastName": row.get("lastName", ""),
            "role": row.get("role") or None,
            "groups": [g.strip() for g in row.get("groups", "").split(",") if g.strip()],
        }
        user_data = {k: v for k, v in user_data.items() if v not in (None, "")}
        response = access_mgmt.create_user(user_data)
        if "error" in response:
            print(f"FAILED {row['email']}: {response['error']}")
        else:
            print(f"OK {row['email']}")
```

---

## When something isn't covered here

- Check the mixin lookup table in this repo's root `CLAUDE.md` for the method's owning file, then read that file directly — docstrings are accurate and NumPy-style.
- Check `examples/<module>_example.md` for a working snippet in the same style as above.
- Do not guess a method name or endpoint. If unsure, say what you checked and what's still unclear.
