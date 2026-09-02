# pysisense SDK Documentation

Welcome to the documentation for the `pysisense` Python SDK — a community project maintained by the Sisense Field Engineering team (not an official Sisense product; see the [README disclaimer](../README.md#%EF%B8%8F-disclaimer--community--field-engineering-project)).

This SDK provides a structured, Pythonic interface for interacting with the Sisense REST APIs. It simplifies common tasks such as user management, dashboard access, data model operations, and cross-environment migrations.

---

## Modules

The documentation is organized by feature/module. Click on any section to learn more:

- [Access Management](access_management.md)  
  Manage users, groups, roles, and share permissions.
  [Access Management Examples](../examples/access_management_example.md)

- [Blox](blox.md)  
  Fetch, save, and delete custom BloX actions; read and update BloX widget styles.
  [Blox Examples](../examples/blox_example.md)

- [Dashboard](dashboard.md)  
  Read, export, share, change ownership, and manage widgets on dashboards.
  [Dashboard Examples](../examples/dashboard_example.md)

- [Folder](folder.md)  
  Create, read, update, and delete Sisense dashboard folders.
  [Folder Examples](../examples/folder_example.md)

- [Data Model](datamodel.md)  
  Inspect datasets, tables, columns, and schema definitions.
  [Data Model Examples](../examples/datamodel_example.md)

- [Migration](migration.md)  
  [Migration Examples](../examples/migration_example.md)

- [Report Manager](report_manager.md)  
  Scheduled report CRUD and on-demand run through the Report Manager plugin.
  [Report Manager Examples](../examples/report_manager_example.md)

- [Sisense Client](sisenseclient.md)  
  Automate cross-environment migration of users, dashboards, and models.

- [Upgrading](upgrading.md)  
  Moving between major SDK versions: version differences, old-to-new field mapping, and a symptom → cause → fix table.

- [Utils](utils.md)  
  Automate cross-environment migration of users, dashboards, and models.

---

## Configuration

Before using the SDK, make sure you have a properly structured `config.yaml`:

```yaml
domain: "your-domain.sisense.com"
is_ssl: true
token: "<your_api_token>"
```

For non-SSL (`is_ssl: false`), the default HTTP port is **30845**. Set optional `port` in the YAML to override (ignored when `is_ssl` is `true`).

TLS certificate verification is enabled by default. Set `verify_ssl: false` to disable it, only for trusted internal networks with self-signed certificates, since disabling it exposes your API token to on-path interception.

**Tokens and permissions:** The SDK works with any Sisense user's API token — permissions are enforced by Sisense itself, so each call is scoped to what the token's user can see and do (e.g., an admin token may list every dashboard on the instance, while a viewer's token lists only dashboards shared with that user).  
Inherently administrative operations require a dedicated Sisense admin user's token, and will fail or behave inconsistently with restricted or scoped users, especially:

- Folder and dashboard ownership changes
- Granting permissions across environments
- System-wide migrations

The same keys can be supplied as a JSON file (`config_file="config.json"`) or as a Python dict (`config_file={...}`); `Migration` and `MergeTool` accept the same forms via `source_config` / `target_config`.

See [`examples/config.yaml`](../examples/config.yaml) or [`examples/config.json`](../examples/config.json) for a template.

---

## Getting Started

To install the SDK:

```bash
pip install pysisense
```

To use in development mode:

```bash
pip install -e .
```