# pysisense SDK Documentation

Welcome to the official documentation for the `pysisense` Python SDK.

This SDK provides a structured, Pythonic interface for interacting with the Sisense REST APIs. It simplifies common tasks such as user management, dashboard access, data model operations, and cross-environment migrations.

---

## Modules

The documentation is organized by feature/module. Click on any section to learn more:

- [Access Management](access_management.md)  
  Manage users, groups, roles, and share permissions.
  [Access Management Examples](../examples/access_management_example.md)

- [Dashboard](dashboard.md)  
  Read, export, share, and change ownership of dashboards.
  [Dashboard Examples](../examples/dashboard_example.md)

- [Data Model](datamodel.md)  
  Inspect datasets, tables, columns, and schema definitions.
  [Data Model Examples](../examples/datamodel_example.md)

- [Migration](migration.md)  
  [Migration Examples](../examples/migration_example.md)

- [Sisense Client](sisenseclient.md)  
  Automate cross-environment migration of users, dashboards, and models.

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

**Important:** It is recommended to use a new dedicated Sisense admin user's token to ensure all API methods function as expected.  
Using restricted or scoped users may result in failures or inconsistent behavior, especially for:

- Folder and dashboard ownership changes
- Granting permissions across environments
- System-wide migrations

See [`examples/config.yaml`](../examples/config.yaml) for a template.

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