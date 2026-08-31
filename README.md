# 📊 Sisense SDK (`pysisense`)

**pysisense** is a Python SDK designed for seamless and structured interaction with the **Sisense API**.  
It simplifies complex API operations and allows you to automate and manage **users**, **groups**, **dashboards**, **data models**, and more.

> ✅ Built for automation, debugging, and extensibility.

---

## ⚠️ Disclaimer — Community / Field Engineering Project

**`pysisense` is *not* an official Sisense product or SDK.** It is a community project built and maintained by members of the Sisense Field Engineering team on a **best-effort basis**.

Please note:

- **No SLA or official support** — this project is not covered by any Sisense Service Level Agreement or support contract. Do not open Sisense Support tickets for issues with this SDK; use the GitHub issue tracker instead.
- **Not part of Sisense's product processes** — the SDK does not go through Sisense's official product QA, security review, or release lifecycle.
- **No compatibility guarantees** — the REST APIs wrapped here may change between Sisense versions without notice, and SDK methods may break as a result.
- **Use at your own risk** — always validate behavior in a non-production environment before running anything against production, especially write operations (migrations, ownership changes, deletions).
- **Best-effort maintenance** — issues and pull requests are welcome and reviewed as time permits, with no guaranteed response times.

---

## 📦 Installation

You can install `pysisense` from [PyPI](https://pypi.org/project/pysisense/):

```bash
pip install pysisense
```

For local development, install in editable mode:

```bash
pip install -e .
```

### ⬆️ Upgrading from 1.x to 2.0

**2.0 contains breaking changes.** `pysisense` follows semantic versioning, so pinning
`pysisense>=1,<2` keeps you on 1.x until you choose to move.

📖 **[Full migration guide](./docs/migration-2.0.md)** — every change mapped old-to-new, with
a symptom → cause → fix table. Complete detail in the [changelog](./CHANGELOG.md).

The main ones:

1. **`ROLE_NAME` now holds the raw Sisense value** (`super` / `consumer` / `contributor`).
   The UI name moved to the new **`ROLE_DISPLAY_NAME`** (`sysAdmin` / `viewer` /
   `dashboardDesigner`). **This one fails silently** — `ROLE_NAME == "sysAdmin"` matches zero
   users instead of raising.
2. **`GROUPS` now includes `Everyone`**, which `get_users_all()` used to strip out. The key
   still holds group names as before, and the new `GROUP_IDS` sits alongside it.
3. **Detect failures with `result.get("ok") is False`** — every failure dict now carries that
   marker, and methods that used to fail with `[]`, `None` or an `"Error: ..."` string now
   return the standard error dict. An empty list always means a genuinely empty result.
4. **`get_unused_columns_bulk` returns a dict**, not a list — read `result["results"]`.
5. **`get_connections` was removed** — use `get_connections_all`.

Check what you are running with `python -c "import pysisense; print(pysisense.__version__)"`.

### Alternative Package Names

If you search for `pysisense` and find a different package, or if you mistyped the package name, PyPI has redirect stub packages registered:

- **`sisense-py`** — redirects to `pysisense`
- **`pysisense-sdk`** — redirects to `pysisense`
- **`sisense-sdk`** — redirects to `pysisense`

These packages raise an error with a clear message if you try to import them, pointing you to the correct `pysisense` package. The canonical package name is always **`pysisense`**.

---

## 🚀 Quick Start

### 1️⃣ Create your YAML config files

Create one or more YAML files (use the templates in `examples/` as reference only):

- `config.yaml` – for single-environment operations
- `source.yaml` and `target.yaml` – for migration scenarios

Each file should follow this structure:

```yaml
domain: "your-domain.com"
is_ssl: true
token: "<your_api_token>"
```

For **non-SSL** connections (`is_ssl: false`), HTTP requests use port **30845** by default. You can override it with an optional `port` field (ignored when `is_ssl` is `true`):

```yaml
domain: "192.168.1.100"
is_ssl: false
port: 30845   # optional, omit to use the default 30845
token: "<your_api_token>"
```

See [`config.yaml.example`](./examples/config.yaml) for the template.

⚠️ **Do not commit your tokens. The provided YAMLs contain placeholder structure only.**

⚠️ **TLS certificate verification is enabled by default.** Only disable it (`verify_ssl: false`) for trusted internal networks with self-signed certificates; doing so exposes your API token to on-path interception.

If your Sisense server uses a self-signed or internal-CA certificate and you still want verification enabled, point `ssl_path` at the CA bundle file (or directory) instead of disabling verification:

```yaml
domain: "your-domain.com"
is_ssl: true
token: "<your_api_token>"
ssl_path: "/path/to/ca-bundle.pem"
```

`ssl_path` takes precedence over `verify_ssl` when both are set, unless `verify_ssl` is explicitly `false`, in that case verification stays fully disabled and `ssl_path` is ignored.

### 🔑 Tokens and Permissions

The SDK works with **any Sisense user's API token** — admin access is not a general requirement. Permissions are enforced by Sisense itself: every API call runs with the role and access rights of the user whose token you configure, so each method can only see and do what that user could see and do in the Sisense UI.

This means the same method can return different results depending on the token. For example, fetching dashboards with an admin token may return every dashboard on the instance, while the same call with a viewer's token returns only the dashboards shared with that user.

Some operations, however, are inherently administrative and will fail or behave inconsistently without full admin privileges — for these, use a dedicated Sisense admin user's token in your `config.yaml`:

- Folder and dashboard ownership changes
- Granting or modifying permissions across environments
- System-wide migrations (users, groups, data models, dashboards)
- Instance-wide listings and admin exports (e.g., methods using `adminAccess=true`)

---

### 2. Explore Example Guides

The [`examples/`](./examples) folder contains Markdown guides. Each guide explains common workflows and includes copy-pasteable code snippets you can adapt in your own project:

- [`access_management_example.md`](./examples/access_management_example.md)  
  Identity & Governance — manage users, groups, folder access, and governance tasks (e.g., unused assets).

- [`datamodel_example.md`](./examples/datamodel_example.md)  
  Data Modeling — work with datasets, tables, columns, and schema within Sisense data models.

- [`dashboard_example.md`](./examples/dashboard_example.md)  
  Dashboard Lifecycle — retrieve, update, reassign ownership, and manage shares of dashboards.

- [`folder_example.md`](./examples/folder_example.md)  
  Folder Management — create, list, update, and delete Sisense dashboard folders.

- [`migration_example.md`](./examples/migration_example.md)  
  Environment Migration — migrate users, dashboards, and data models across environments (e.g., dev → prod).

- [`wellcheck_example.md`](./examples/wellcheck_example.md)  
  Data Health & Complexity — run structural checks on dashboards and data models (widget counts, pivot fields, island tables, RLS datatypes, import queries, many-to-many relationships, and unused columns).

Note: These guides are not meant to be executed end-to-end. Copy the relevant snippets into your own Python files or notebooks, update configuration (YAML paths, IDs, etc.), and run them in your environment.

---

### 3️⃣ Logs

All logs are saved automatically to a local folder:

```
logs/pysisense.log
```

You don’t need to create this folder manually — it will be created at runtime in the **same directory where you run your scripts**.

Logs rotate automatically at midnight and keep **7 days of history**. The active file is always named `pysisense.log`; each day's log is renamed to `pysisense.log.YYYY-MM-DD` at rotation time, and once more than 7 dated backups exist, the oldest one is deleted. The active log file's name never changes and it is never overwritten mid-rotation — only rotated out at day's end.

---

## ✅ Features

- 👥 **User & Group Management** – Create, update, delete, and fetch users or groups
- 📊 **Dashboard Management** – Export, share, and migrate dashboards
- 📦 **Data Models** – Explore, describe, and update schemas and security
- 🔐 **Permissions** – Resolve and apply share rules (users & groups)
- 🔄 **Cross-Environment Migrations** – Move dashboards, models, and users
- ✅ **WellCheck** – Analyze dashboard and data model health (structure complexity, widget density, pivot fields, island tables, RLS datatypes, import queries, many-to-many relationships, and unused columns)
- 🧠 **Smart Logging & Data Helpers** – Auto log capture, CSV export, and DataFrame conversion
- ➕ **And many more** – Refer to the documentation for full details

---

## 🔧 Design Philosophy

- Pythonic SDK with class-based structure (`Dashboard`, `DataModel`, `AccessManagement`, `Migration`)
- Additional analysis module: `WellCheck` – Run dashboard and data model health checks (structure, complexity, and best-practice validations)
- Modular YAML-based authentication
- Built-in logging and exception handling
- Designed for end-to-end automation and real-world use

---

## 🤖 Stable Contracts for Programmatic Consumers

Tools that generate schemas by introspecting this package (agents, MCP servers, code generators) can rely on the following as **stable public API**:

### Facade registry

`pysisense.FACADES` is an explicit tuple of the tool-bearing facade classes (`AccessManagement`, `DataModel`, `Dashboard`, …). Iterate it to discover the SDK's operational surface — do **not** iterate `__all__`, which also contains TypedDict payload types and utility functions. `SisenseClient` is intentionally excluded (it is the shared HTTP/auth client, not an operation facade).

### Error-dict shape

Failure returns follow one shape across the SDK:

```python
{"ok": False, "error": "<human-readable message>", "status_code": <int>}   # status_code present only when an HTTP status exists
```

Detect failure by the explicit **`"ok": False` marker** (`payload.get("ok") is False`) — the forward-compatible check — or by the presence of the `"error"` key. Never match an exact key set. The failure dict may gain **additive** keys in minor releases (`status_code` arrived in 1.1.0; some methods add context keys), so a consumer checking `keys() == {"error"}` will silently misclassify failures as successes. Renaming or removing `"error"`/`"status_code"` is treated as a breaking change; adding keys is not.

Since 2.0, `"error"` is always a **clean sentence**: either the recognised Sisense reason (from the body's `detail`/`message`/`title`/`error` key) or an honest label like `"unrecognized error body"`. When the body could not be recognized, the redacted, 300-char-truncated dump travels separately in an additive **`"raw_body"`** key, so consumers with different trust boundaries can relay or drop it independently of the sentence.

Two adjacent guarantees:

- **Redaction is part of the contract, not a courtesy:** credential-shaped values are stripped by `redact_secrets()` *before* the message is built, so the `"error"` string is safe to relay verbatim across trust boundaries (e.g. privacy modes where the failure reason is the only data that reaches a model).
- **Resolver envelopes are their own stable shape:** `resolve_dashboard_reference` and `resolve_datamodel_reference` return `{"success", "status_code", "<entity>_id", "<entity>_title", "error"}` on both success and failure. Detect their outcome via `success`, not via error-key matching — they carry payload keys alongside `"error"`, and they will **not** be folded into the generic error dict.

Connection-level failures carry `"error"` without `"status_code"` (no HTTP status exists; the absence is itself signal).

**All live methods follow the contract.** The pre-2.0 failure-shape exceptions (`[]`, `"Error: ..."` strings, `None`, list-wrapped error dicts) were converged onto the error dict in 2.0 — an empty list from a read method now always means a genuinely empty result, never a swallowed failure. Two footnotes:

- **Deprecated aliases are fossils:** methods carrying `__deprecated__` (e.g. `get_user_with_role_and_group_names`, `users_per_group_all`, `get_unused_columns`) keep their old, frozen shapes — including old failure shapes — until removal. Skip them via the `__deprecated__` marker.
- **Write methods return dicts on success too:** `add_dashboard_script` / `add_widget_script` return `{"success": True, "message": ...}`; `add_dashboard_shares` returns `{"success": True, "message": ..., "new_shares": n, "updated_shares": n}`; `get_unused_columns_bulk` always returns `{"results": [...], "errors": [{"ref", "error"}]}` (with `"ok": False` + top-level `"error"` added when nothing could be processed).

### Payload contracts

Dict parameters are typed with TypedDicts from `pysisense/payloads.py` (also exported at package root). Required vs. optional fields introspect via `__required_keys__` / `__optional_keys__`; annotations resolve at runtime (`inspect.signature(..., eval_str=True)`). Deprecated method aliases are decorated with `@typing_extensions.deprecated(...)` (PEP 702), so `__deprecated__` is introspectable. Enum-valued string parameters use `typing.Literal`.

---

📚 Documentation

Comprehensive module-level documentation is available in the `docs/` folder:

-   [Index](docs/index.md) – Overview of the SDK structure and modules
-   [Sisense Client](docs/sisenseclient.md) – Base API wrapper for all HTTP operations
-   [Access Management](docs/access_management.md) – Manage users, groups, roles, and permissions
-   [Data Model](docs/datamodel.md) – Handle datasets, tables, schemas, security, and deployment
-   [Dashboard](docs/dashboard.md) – Retrieve, modify, and share Sisense dashboards
-   [Migration](docs/migration.md) – Migrate users, dashboards, and models between environments
-   [Utils](docs/utils.md) – Helper functions for export, formatting, and data operations
-   [WellCheck](docs/wellcheck.md) – Run health checks on dashboards and data models (structure, complexity, and best-practice validations)

You can also explore:

-   Inline method docstrings using `help()` in Python or directly within your IDE.

---

## 📄 License

This project is licensed under the Sisense End User License Agreement (EULA).
See the [LICENSE](./LICENSE) file for the full text.

© 2026 Sisense Ltd. “Sisense” and related marks are trademarks of Sisense Ltd.
