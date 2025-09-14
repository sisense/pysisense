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

⚠️ **Do not commit your tokens. The provided YAMLs contain placeholder structure only.**

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

- [`migration_example.md`](./examples/migration_example.md)  
  Environment Migration — migrate users, dashboards, and data models across environments (e.g., dev → prod).

Note: These guides are not meant to be executed end-to-end. Copy the relevant snippets into your own Python files or notebooks, update configuration (YAML paths, IDs, etc.), and run them in your environment.

---

### 3️⃣ Logs

All logs are saved automatically to a local folder:

```
logs/pysisense.log
```

You don’t need to create this folder manually — it will be created at runtime in the **same directory where you run your scripts**.

---

## ✅ Features

- 👥 **User & Group Management** – Create, update, delete, and fetch users or groups
- 📊 **Dashboard Management** – Export, share, and migrate dashboards
- 📦 **Data Models** – Explore, describe, and update schemas and security
- 🔐 **Permissions** – Resolve and apply share rules (users & groups)
- 🔄 **Cross-Environment Migrations** – Move dashboards, models, and users
- 🧠 **Smart Logging & Data Helpers** – Auto log capture, CSV export, and DataFrame conversion
- ➕ **And many more** – Refer to the documentation for full details

---

## 🔧 Design Philosophy

- Pythonic SDK with class-based structure (`Dashboard`, `DataModel`, `AccessManagement`, `Migration`)
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

You can also explore:

-   Inline method docstrings using `help()` in Python or directly within your IDE.

---

## 📄 License

This project is licensed under the Sisense End User License Agreement (EULA).
See the [LICENSE](./LICENSE) file for the full text.

© 2025 Sisense Ltd. “Sisense” and related marks are trademarks of Sisense Ltd.