# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
# Install dev dependencies
uv sync --dev

# Run all unit tests (no integration)
uv run pytest -m "not integration"

# Run a single test file
uv run pytest tests/unit/unit_test_wellcheck.py

# Run a single test by name
uv run pytest tests/unit/unit_test_wellcheck.py -k "test_name"

# Run integration tests (requires live Sisense instance)
uv run pytest -m integration

# Lint (check only)
uv run ruff check .

# Lint with auto-fix
uv run ruff check --fix .

# Format
uv run ruff format .

# Check docstring conventions (whole package)
uv run python tools/check_docstrings.py

# Check docstring conventions on specific files
uv run python tools/check_docstrings.py pysisense/queries/core.py

# Install pre-commit hooks (commit-msg + the file hooks)
uv run pre-commit install --hook-type commit-msg
uv run pre-commit install
```

> The docstring checker (`tools/check_docstrings.py`) enforces the NumPy
> docstring + type-hint conventions below across the whole package, and runs
> automatically via pre-commit and CI. Every public method must have a docstring
> with `Parameters`/`Returns` sections, full type hints, only approved
> `(format: ...)` tags (`email`, `uuid`, `date`, `ipv4`, `ipv6`), and no mention
> of external systems; every facade class (in an `__init__.py`) must have a
> `Modules` section.

---

## pysisense Guidelines

## Project Overview

`pysisense` is an official Python SDK for the **Sisense BI platform**. It wraps the Sisense REST API into a structured, class-based library for automation, migrations, and health checks.

### Modules

| Module | Class | Responsibility |
|---|---|---|
| `sisenseclient.py` | `SisenseClient` | Base HTTP client, auth, logging, shared session |
| `access_management/` | `AccessManagement` | Users, groups, permissions, ownership, RLS, schedules |
| `blox/` | `Blox` | Fetch custom Blox actions (Linux and Windows); save/delete Linux only |
| `custom_code/` | `CustomCode` | Custom-code notebooks: CRUD, export, folder/file rename |
| `dashboard/` | `Dashboard` | Dashboard CRUD, admin export, shares, dashboard/widget scripts |
| `folder/` | `Folder` | Folder CRUD and folder tree retrieval |
| `metadata/` | `Metadata` | Datasource metadata: measures, dimensions, queries, datasource list |
| `encryption/` | `Encryption` | Encrypt/decrypt connection parameters for cross-server datamodel migration |
| `datamodel/` | `DataModel` | Schema provisioning, builds, connections, datasecurity |
| `migration/` | `Migration` | Cross-environment migrations (users, groups, dashboards, models) |
| `mergetool/` | `MergeTool` | Cross-environment custom-code notebook and folder migration |
| `plugins/` | `Plugins` | Plugin listing, enable/disable (single and bulk), state snapshots |
| `queries/` | `Queries` | JAQL and SQL query execution against datasources/elasticubes |
| `wellcheck/` | `WellCheck` | Health/complexity checks across dashboards and data models |
| `utils.py` | — | `convert_to_dataframe`, `export_to_csv`, `convert_utc_to_local` |

### Package structure — mixin pattern

Each module (except `sisenseclient.py` and `utils.py`) is a **package directory**. The `__init__.py` assembles the public class from mixin files. Each mixin file holds a logical group of methods as `class XxxMixin:` with no `__init__`. Mixins access `self.api_client` and `self.logger` which are set by the assembler.

**To find or edit a method — look up its mixin file:**

| Package | File | Public methods |
|---|---|---|
| `blox/` | `core.py` | `get_blox_actions` (OS-routed), `save_blox_action`, `delete_blox_action` |
| `access_management/` | `users.py` | `get_user`, `get_my_user`, `get_roles`, `change_user_password`, `get_users_all`, `get_user_with_role_and_group_names`, `get_users_with_role_names_and_group_names`, `create_user`, `update_user`, `delete_user` |
| `custom_code/` | `core.py` | `get_notebooks`, `export_notebook`, `create_notebook`, `update_notebook`, `delete_notebook`, `list_notebook_folder_contents`, `rename_notebook_file`, `rename_notebook_folder` |
| | `groups.py` | `get_group`, `users_per_group`, `users_per_group_all` |
| | `columns.py` | `get_datamodel_columns`, `get_unused_columns`, `get_unused_columns_bulk` |
| | `ownership.py` | `change_folder_and_dashboard_ownership` |
| | `admin.py` | `get_all_dashboard_shares`, `create_schedule_build` |
| `dashboard/` | `core.py` | `get_all_dashboards`, `get_dashboards`, `get_dashboard_by_id`, `get_dashboard_by_name`, `export_dashboard`, `get_dashboard_widgets`, `resolve_dashboard_reference`, `publish_dashboard`, `rename_dashboard`, `move_dashboard_to_folder`, `can_be_owned` |
| | `shares.py` | `add_dashboard_shares`, `get_dashboard_share`, `get_dashboard_shares_v1` |
| | `columns.py` | `get_dashboard_columns` |
| | `scripts.py` | `add_dashboard_script`, `add_widget_script`, `get_dashboard_script`, `get_widget_script` (`SisenseScript` helper class in same file) |
| `folder/` | `core.py` | `create_folder`, `update_folder`, `get_folder_id`, `get_folders` (structure param, default `"flat"`), `get_folder_ancestors`, `get_navver`, `get_all_folders` (tree shortcut), `delete_folder` |
| `metadata/` | `core.py` | `get_datasource_measures`, `get_datasource_dimensions`, `get_datasources`, `add_datasource_measure`, `post_metadata_query` |
| `encryption/` | `core.py` | `encrypt`, `decrypt` |
| `datamodel/` | `core.py` | `get_datamodel`, `get_all_datamodel`, `describe_datamodel_raw`, `describe_datamodel`, `get_model_schema`, `resolve_datamodel_reference`, `get_elasticubes`, `load_datamodel`, `delete_datamodel` |
| | `connections.py` | `get_connection`, `get_connections`, `update_connection`, `get_table_schema`, `generate_connections_payload`, `create_connections` |
| | `build.py` | `create_datamodel`, `create_dataset`, `create_table`, `setup_datamodel`, `deploy_datamodel` |
| | `security.py` | `get_datasecurity`, `get_datasecurity_detail`, `update_datasecurity`, `set_live_datasecurity_add_many` |
| | `shares.py` | `get_datamodel_shares`, `add_datamodel_shares` |
| | `data.py` | `get_data`, `get_row_count` |
| `migration/` | `groups.py` | `migrate_groups`, `migrate_all_groups` |
| | `users.py` | `migrate_users`, `migrate_all_users` |
| | `dashboards.py` | `migrate_dashboard_shares`, `migrate_dashboards`, `migrate_all_dashboards` |
| | `datamodels.py` | `migrate_datamodels`, `migrate_all_datamodels` |
| | `base.py` | `_emit` and internal helpers (private) |
| `mergetool/` | `custom_code.py` | `migrate_notebooks`, `migrate_all_notebooks` |
| | `folder.py` | `migrate_folders`, `migrate_all_folders` |
| `plugins/` | `core.py` | `get_all_plugins`, `get_plugin`, `enable_plugin`, `disable_plugin`, `enable_plugins`, `disable_plugins` |
| | `snapshots.py` | `save_snapshot`, `restore_snapshot` |
| `queries/` | `core.py` | `elasticube_run_jaql_query`, `elasticubes_run_jaql_csv` |
| `wellcheck/` | `dashboard_checks.py` | `check_dashboard_structure`, `check_dashboard_widget_counts`, `check_pivot_widget_fields` |
| | `datamodel_checks.py` | `check_datamodel_custom_tables`, `check_datamodel_island_tables`, `check_datamodel_rls_datatypes`, `check_datamodel_import_queries`, `check_datamodel_m2m_relationships` |
| | `__init__.py` | `run_full_wellcheck` (orchestrates all checks) |

**Mixin rules:**
- No `__init__` in mixin files — initialization is in `package/__init__.py` only
- No `SisenseClient` import in mixin files — injected by the assembler
- All mixin files start with `from __future__ import annotations`

### Examples and docs

- `examples/*.md` — copy-paste workflow guides; **not meant to be executed end-to-end**.
- `docs/*.md` — full module-level reference documentation.

### Tooling

- Package manager: `uv`
- Lint + format: `ruff` (line length 200)
- Commits: `commitizen` (conventional commits)
- Versioning: `python-semantic-release`
- Pre-commit hooks: ruff + commitizen

---

## Git Workflow

- Branch from **`dev`**, never from `main`
- Branch name: `your-name/short-description` (lowercase, hyphens)
  - e.g. `alice/add-user-migration`, `bob/fix-dashboard-loading`

### Commit messages — Conventional Commits

```
<type>(<scope>): <description>
```

| Type | When to use |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `refactor` | No feature/fix change |
| `docs` | Documentation only |
| `test` | Tests only |
| `chore` | Build, deps, config |
| `perf` | Performance improvement |

Use present-tense imperative: `add user validation`, not `added user validation`.

---

## API Initialization

The SDK **must** use a Sisense admin user's API token.

### Canonical init pattern

```python
import os
from pysisense import SisenseClient, AccessManagement, Blox, CustomCode, Dashboard, DataModel, Encryption, Folder, MergeTool, Metadata, Plugins, Queries, WellCheck

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

access_mgmt = AccessManagement(api_client=api_client)
blox = Blox(api_client=api_client)
dashboard = Dashboard(api_client=api_client)
folder = Folder(api_client=api_client)
datamodel = DataModel(api_client=api_client)
folder = Folder(api_client=api_client)
plugins = Plugins(api_client=api_client)
wellcheck = WellCheck(api_client=api_client)
```

### Config file format (`config.yaml`)

```yaml
domain: ""     # IP or domain — no protocol, no port
is_ssl: false  # true for HTTPS, false uses HTTP (default port 30845)
# port: 30845  # optional, HTTP port when is_ssl is false
token: ""      # Sisense Admin API token
```

> Never commit `config.yaml`, `source.yaml`, or `target.yaml` — they contain real tokens.

### Security rules

- **Never hardcode tokens or credentials** in source code. Always load from a YAML config or environment variable.
- **Debug logs must not expose secrets.** Never write tokens, passwords, auth headers, or raw cookies to `logs/pysisense.log`.

### Exporting results

```python
df = api_client.to_dataframe(result)
api_client.export_to_csv(result, "output.csv")
```

---

## Naming Conventions

| Thing | Style | Example |
|---|---|---|
| Files/modules | snake_case | `access_management.py` |
| Classes | PascalCase | `SisenseClient` |
| Functions/methods | snake_case | `get_user()` |
| Constants | UPPER_SNAKE | `MAX_RETRIES` |
| Private | `_` prefix | `_make_request()` |

---

## Docstrings — NumPy Style (required)

Every public method must have a NumPy-style docstring. Include: summary line, short description (2–6 lines), `Parameters`, `Returns`, optionally `Raises` and `Notes`.

```python
def create_user(self, name: str, email: str, role: str = "viewer") -> dict[str, Any]:
    """Create a new user in Sisense.

    Sends a POST request to create a user account. The role name is resolved
    to its internal Sisense role ID before submission.

    Parameters
    ----------
    name : str
        The full name of the user.
    email : str
        The email address of the user.
    role : str, optional
        Role to assign. One of ``"viewer"``, ``"dashboardDesigner"``, ``"sysAdmin"``.

    Returns
    -------
    dict[str, Any]
        The created user object returned by the API, or ``{"error": "..."}`` on failure.
    """
```

- Always include **type hints** on parameters and return values.
- **Never** mention MCP, tools, LLMs, agents, or any external system in docstrings.
- Do **not** include `Example:` or `Examples` blocks in docstrings.

### Update payload docstrings

For methods accepting an update/patch payload, the docstring must:
- State updates must be provided inside the payload parameter.
- List supported fields using **canonical Sisense payload field names**.
- State: only fields provided are updated; omitted fields are not modified.
- Describe any role/group name-to-ID resolution behavior.

---

## Python Coding Conventions

### Shared `api_client` injection

```python
def __init__(self, api_client=None):
    self.api_client = api_client or SisenseClient()
    self.logger = self.api_client.logger
```

### Type hints (required)

All public methods must have type hints. Target Python 3.10+ — use builtin generics:

```python
# ✅ GOOD
def get_user(self, user_id: str) -> dict[str, Any]: ...
def list_groups(self) -> list[dict[str, Any]]: ...

# ❌ BAD
from typing import Dict, List  # don't use deprecated aliases
```

### Error handling — return dicts, don't raise

Methods return `{"error": "..."}` dicts on failure. Do not raise exceptions from public methods unless the module already uses exceptions consistently.

```python
# ✅ GOOD
response = self.api_client.get(url)
if "error" in response:
    self.logger.error(f"Failed to get dashboard: {response['error']}")
    return {"error": response["error"]}
return response
```

### Logging — levels and secrets policy

Use the shared logger from `SisenseClient`. File-only logging to `logs/pysisense.log`. **Never use `print`.**

| Level | When to use |
|---|---|
| `debug` | Step-by-step decisions, endpoints, counts, mapping sizes |
| `info` | Success summaries — what changed, counts, completion messages |
| `error` | Failures with status code (if available) and a safe error summary |

**Never log:** tokens, passwords, auth headers, raw cookies, or sensitive payload fields.

```python
# ✅ GOOD
self.logger.debug(f"Resolving {len(group_names)} group names to IDs")
self.logger.info(f"Updated user '{user_id}' — fields: {list(payload.keys())}")

# ❌ BAD
print("Done")
self.logger.debug(f"Sending payload: {full_payload}")
```

### Smart reference resolvers

Methods that accept a dashboard or data model reference handle either a 24-char ID or a title string:

```python
ref = self.resolve_dashboard_reference(dashboard)
dashboard_id = ref["dashboard_id"]
```

### Input normalization

Methods accepting `list[str]` should also tolerate a bare `str`:

```python
if isinstance(dashboards, str):
    dashboards = [dashboards]
```

### Role name mapping

| Sisense internal | User-facing |
|---|---|
| `consumer` | `viewer` |
| `super` | `sysAdmin` |
| `contributor` | `dashboardDesigner` |

### SSL

SSL verification is always disabled (`verify=False`). Default non-SSL ports: `30845` for Linux, `8081` for Windows. Override with optional `port` in `config.yaml`.

### OS-specific API routing

Some Sisense API endpoints differ between Linux and Windows deployments. Follow these rules when implementing OS-specific logic:

- **Linux is always the default** — write the Linux path as the `else` branch.
- **Windows is always the conditional** — use `if self.api_client.operating_system == "windows":`.
- Always include `os=` in the debug log so the route taken is visible.
- Methods that are **Linux-only** must guard the Windows case explicitly and return `{"error": "...not supported on Windows..."}` rather than silently hitting a wrong endpoint.

```python
# ✅ GOOD — Linux default, Windows conditional
os = self.api_client.operating_system
endpoint = "/windows/endpoint" if os == "windows" else "/linux/endpoint"
self.logger.debug(f"Fetching data (os={os})")

# For Linux-only methods, guard at the top of the method:
if self.api_client.operating_system == "windows":
    msg = "this_method is not supported on Windows deployments."
    self.logger.error(msg)
    return {"error": msg}
```

---

## PATCH / Update Safety Rules

### Only send explicitly provided fields

```python
# ✅ GOOD — only what the caller set
payload = model.model_dump(by_alias=True, exclude_unset=True, exclude_none=True)

# ❌ BAD — injects defaults the caller never intended
payload = {"userName": user_data.get("userName", ""), "groups": user_data.get("groups", [])}
```

### Never set `groups` to `[]` unless explicitly requested

An empty list clears all group memberships. If `groups` was not provided, omit it entirely.

### ID resolution — only when provided

| Field | Resolution rule |
|---|---|
| `role` (name) → `roleId` | Resolve **only** when `role` is explicitly provided |
| `groups` (names) → group IDs | Resolve **only** when `groups` is explicitly provided |

Fail fast if a referenced role or group cannot be resolved — return `{"error": "..."}` with a clear message.

### Validate inputs before calling the API

Run Pydantic validation before any HTTP call. Return a structured error immediately if validation fails.

---

## Pydantic Patterns (v2)

Use Pydantic for structured payload validation on `create`, `update`/`patch`, and migration operations.

```python
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Any

class UpdateUserPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    user_name: str | None = Field(None, alias="userName")
    email: str | None = None
    first_name: str | None = Field(None, alias="firstName")
    last_name: str | None = Field(None, alias="lastName")
    role: str | None = None
    groups: list[str] | None = None
```

- **Forbid unknown fields** on update models — use `extra="forbid"`.
- **Safe aliases** — support common variants while emitting canonical API keys.
- **Normalize types** — ensure `groups` is always a list when applicable using `@model_validator`.
- **PATCH payloads** — always use `exclude_unset=True, exclude_none=True`.

---

## DataModel Conventions

### Model types

| Type | String | Notes |
|---|---|---|
| Elasticube | `"extract"` | Supports all build behaviors |
| Live | `"live"` | `build_behavior_config` is ignored |

### `build_behavior_config` modes (extract only)

| Mode | Requires |
|---|---|
| `"replace"` | — |
| `"replace_changes"` | — |
| `"append"` | — |
| `"increment"` | `column_name` (the incremental column) |

### `deploy_datamodel` parameters

```python
# Extract model
datamodel.deploy_datamodel(name, build_type="by_table", schema_origin="latest")
# build_type:    "by_table" | "full" | "schema_changes"
# schema_origin: "latest" | "running"

# Live model — no extra params needed
datamodel.deploy_datamodel(name)
```

### Supported connection types for `generate_connections_payload`

`"Athena"`, `"RedShift"`, `"BigQuery"`, `"DataBricks"` (case-insensitive)

### `get_datasecurity` edge case

If no RLS rules exist, returns a single entry with empty values — never an empty list.

### Share permissions

`"EDIT"` | `"USE"` | `"READ"` (uppercase required)

---

## Migration Module Patterns

### Two initialization modes

```python
# YAML config files
m = Migration(source_yaml="source.yaml", target_yaml="target.yaml", debug=False)

# Pass SisenseClient instances directly
m = Migration(source_client=src, target_client=tgt)
```

### Progress reporting via `_emit`

```python
m = Migration(source_client=src, target_client=tgt)
m._emit = my_progress_callback  # defaults to print
```

### Migration order matters

1. Groups first
2. Users (reference groups)
3. Data models (referenced by dashboards)
4. Dashboards last (reference users, groups, data models)

---

## WellCheck Conventions

### `run_full_wellcheck` return structure

```python
{
    "dashboards": {
        "structure":           [...],
        "widget_counts":       [...],
        "pivot_widget_fields": [...],
    },
    "datamodels": {
        "custom_tables":     [...],
        "island_tables":     [...],
        "rls_datatypes":     [...],
        "import_queries":    [...],
        "m2m_relationships": [...],
        "unused_columns":    [...],
    }
}
```

### Per-check output fields

| Check | Key fields |
|---|---|
| `check_dashboard_structure` | `pivot_count`, `tabber_count`, `jtd_count`, `accordion_count` |
| `check_dashboard_widget_counts` | `widget_count` |
| `check_pivot_widget_fields` | `field_count`, `has_more_fields` |
| `check_datamodel_custom_tables` | `has_union` (`"yes"` / `"no"`) |
| `check_datamodel_island_tables` | `relation` (`"no"` = island), `type` (`fact`/`dim`/`custom`) |
| `check_datamodel_rls_datatypes` | `datatype` |
| `check_datamodel_import_queries` | `has_import_query` (`"yes"` / `"no"`) |
| `check_datamodel_m2m_relationships` | `is_m2m` (bool) — runs real SQL queries |

### Thresholds and edge cases

- `check_pivot_widget_fields(max_fields=20)` — triggers on `field_count > max_fields` (strictly greater, not ≥)
- `check_datamodel_m2m_relationships` executes aggregate SQL — can be slow
- `unused_columns` requires `access_mgmt` to be configured on the `WellCheck` instance

### Inputs accept IDs or titles

All check methods accept either a 24-char model/dashboard ID or a title string.

---

## Testing Conventions

### Test structure

```
tests/
  unit/          # No real HTTP calls — use fakes/stubs
  integration/   # Require a live Sisense instance
```

Integration tests are marked with `@pytest.mark.integration`:

```bash
pytest -m "not integration"
```

### Unit test approach — fake injection

Do **not** mock `SisenseClient`. Create a fake `api_client` and inject it directly:

```python
class FakeApiClient:
    def get(self, url, **kwargs):
        return self._fixture_data.get(url, {})
```

### Fixture data

Keep fixture dicts inline in the test file rather than loading from external JSON files.

---

## Documentation Maintenance

When a public method **signature or documented behavior changes**:

1. Update the relevant `docs/<module>.md` page — parameters, return shape, and semantics.
2. Update `examples/<module>_example.md` — add or edit the snippet for that method.

When **adding a new public method to an existing module**:

1. Add the method name to the **mixin lookup table** below (and in `.cursor/rules/project-overview.mdc`).
2. Add a usage snippet to `examples/<module>_example.md`. **This is not optional** — every new public method needs at least a one-liner showing the call and what comes back.
3. Update `docs/<module>.md` with the parameter table and return shape.

When **adding a new module**:

1. Add the module to the **Modules** table in `CLAUDE.md` and `.cursor/rules/project-overview.mdc`.
2. Add the mixin file and its public methods to the **mixin lookup table** in both files.
3. Add the new class to the canonical init pattern in `CLAUDE.md` if it is a top-level SDK class.
4. Create `examples/<module>_example.md` with copy-paste usage snippets for every public method.
5. Create `docs/<module>.md` with full reference documentation.

> **Reminder:** `examples/*.md` files are the first place users look. Skipping them means users have no copy-paste starting point. Always update them.

### Quality bar and minimal-change policy

- **Preserve existing behavior** unless fixing a bug.
- **Prefer small, safe refactors** over large rewrites.
- **Match the existing code style, naming conventions, and logging patterns** in the module you are editing.
- Do not introduce cross-module logic unless explicitly requested.
- Public SDK methods should be stable and well-documented. Use `_` prefixed helpers for internal shared logic.
