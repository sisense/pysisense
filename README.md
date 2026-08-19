# 📊 Sisense SDK (`pysisense`)

**pysisense** is a Python SDK designed for seamless and structured interaction with the **Sisense API**.  
It simplifies complex API operations and allows you to automate and manage **users**, **groups**, **dashboards**, **data models**, and more.

> ✅ Built for automation, debugging, and extensibility.

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

### Alternative Package Names

If you search for `pysisense` and find a different package, or if you mistyped the package name, PyPI has redirect stub packages registered:

- **`sisense-py`** — redirects to `pysisense`
- **`pysisense-sdk`** — redirects to `pysisense`
- **`sisense-sdk`** — redirects to `pysisense`

These packages raise an error with a clear message if you try to import them, pointing you to the correct `pysisense` package. The official package is always **`pysisense`**.

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

### ⚠️ Important: Use a Dedicated Admin Token

Some methods in this SDK require full administrative privileges to interact with Sisense resources (such as ownership changes, user migrations, or folder/dashboard access).

To avoid permission-related issues or incomplete operations:

It is recommended to use a new dedicated Sisense admin user's token when authenticating via your `config.yaml`.

Using restricted or scoped users may result in failures or inconsistent behavior, especially for:

- Folder and dashboard ownership changes
- Granting permissions across environments
- System-wide migrations

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
  Data Health & Complexity — run structural checks on dashboards and data models (widget counts, pivot fields, island tables, RLS datatypes, import queries, many-to-many relationships, unused columns, and unused columns).

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
