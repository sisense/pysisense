# Sisense Migration & Merge Tool - User Manual 🚀


## Table of Contents
- 1. Introduction
- 2. Getting Started
  - Prerequisites
  - Installation
  - Running the Application
- 3. Initial Configuration
  - How Configuration is Stored
  - Step 1: Add Your Servers
  - Step 2: Select Servers for Migration
- 4. Running a Migration
  - Step 1: Define the Migration Plan
  - Step 2: Configure Migration Details
  - Step 3: Save Your Settings
  - Step 4: Run the Migration
  - Step 5: Monitor the Output
- 5. Advanced Features
  - Performance Tuning
  - Data Integrity and Customization
  - Folder Hierarchy & Permissions
  - Automation and Error Handling
  - Dashboard Validation (Validation Tab)
- 6. Troubleshooting
- 7. Appendix: Practical Examples
  - Remapping SQL Server Connections

---

## 1. Introduction 👋

Welcome to the Sisense Migration & Merge Tool! This application is designed to help Sisense administrators and data teams migrate assets—such as dashboards, users, groups, and datamodels—between different Sisense environments (e.g., from a Windows-based instance to a Linux-based one, or between two Linux instances).

The tool provides a user-friendly web interface to configure, run, and monitor your migration tasks in real time.

### Key Features

*   **Comprehensive Migration:** Transfer dashboards, users, groups, folders, datamodels, data security rules, saved filters, saved formulas and Blox actions.
*   **Standalone Executable**: Runs as a local web server that you can access from your browser.
*   **Web-Based UI:** Configure and control everything from your web browser. No complex command-line usage is required for normal operation.
*   **Connection Testing:** Built-in test connection functionality to verify server connectivity and API token validity before running migrations.
*   **Live Monitoring:** Watch the migration progress with real-time progress bars and see log outputs as they happen in a dedicated browser tab.
*   **Flexible Configuration:** Fine-tune your migration or merge job with detailed options, including include/exclude lists, overwrite behavior, and performance settings.
*   **Safe & Reusable Settings:** All configurations are saved to simple `settings.yaml` and `servers.yaml` files, which you can back up, share, and reuse.

---

## 2. Getting Started 🚀

### Prerequisites

*   **Network Access**: The computer running the tool must have network access to the Sisense API endpoints for all source and target servers.
*   **API Tokens**: You will need API tokens with administrative privileges for all source and target Sisense servers.

### Installation

The tool is provided as a self-contained package (e.g., a `.zip` file containing an `.exe`). To install it:

1.  **Download the latest release from GitLab**. The application is provided as a self-contained package (e.g., `win2linux-merge-tool-web-latest.zip`).
2.  Extract the contents of the zip file to a folder on your computer (e.g., `C:\Tools\SisenseMigrationTool`).

### Running the Application

1.  Navigate to the folder where you extracted the application.
2.  Double-click the executable file: `win2linux-merge-tool-web.exe`.
3.  A console window will appear. This window is the server and **must remain open** while you use the tool. It will display startup information and logs.
4.  Once the server has started, it will display a message like `--- Running in Server Mode ---`.
5.  Open your web browser (Chrome, Firefox, or Edge is recommended) and navigate to: **http://localhost:5001**

**macOS users (running the `.app`):** If the app does not open or macOS reports that it is damaged, clear extended attributes first. From Terminal run: `xattr -cr /path/to/win2linux-merge-tool-web.app` (replace with the actual path to your app). Then open the app normally.

You should now see the main interface of the Migration & Merge Tool with a modern layout:
- **Top Navigation**: Three tabs—**Migration & Merge**, **Validation**, and **Manage Servers**—let you switch between configuring migrations, running dashboard validation, and managing server definitions.
- **Left Panel** (Migration & Merge view): Migration switches and navigation for different asset types.
- **Right Panel** (Migration & Merge view): Configuration options that appear when you select a migration type.
- **Top Action Bar** (Migration & Merge view): Buttons for loading/saving settings and running migrations.

---

## 3. Initial Configuration ⚙️

Before you can run a migration, you must configure the source and target Sisense servers.

### How Configuration is Stored

The tool stores its configuration in two files (`settings.yaml` and `servers.yaml`) in a dedicated folder on your computer. This location is typically:

*   **Windows:** `C:\Users\<YourUsername>\AppData\Roaming\sisense_merge_and_migration\`

These files are created automatically the first time you run the application.

### Step 1: Add Your Servers

1.  In the web UI, open the **Manage Servers** tab in the top navigation (or click **Manage Servers** from the Migration & Merge view if your layout still shows it). The server management view opens with a list of servers on the left and the server details form on the right.
2.  To add a server manually, click the **`+`** icon and fill in the details in the form.
3.  To import a list of servers from a file, click the **Upload** icon (upward arrow) and select your `servers.yaml` file. This is a great way to copy a configuration from a colleague.
4.  Fill in the details for your Sisense instance:
    *   **Server Name:** A friendly name for you to identify the server (e.g., "Production Windows" or "Staging Linux").
    *   **Host:** The hostname or IP address of the Sisense server (e.g., `my.sisense.com`).
    *   **Port:** The port for the Sisense API (usually `443` for HTTPS or `8081` for HTTP).
    *   **Protocol:** Select `https` (recommended) or `http`.
    *   **OS:** Select `Windows` or `Linux`. This is crucial for API compatibility.
    *   **API Token:** Your Sisense REST API token.
    *   **Allowed Roles:**
        *   Check **Source** if you will be migrating *from* this server.
        *   Check **Target** if you will be migrating *to* this server.
        *   You can check both.
5.  **Test Your Server Configuration**: Before saving, you can test the connection to verify your settings:
    *   Click the **"Test Connection"** button to validate the connection and API token.
    *   The test will check both the HTTP connection and API token authentication.
    *   Results will appear below the button with a green background for success or red background for errors.
    *   Test results are automatically cleared when you modify any server settings.
6.  Click **Save Server**.
7.  Repeat these steps for all Sisense servers you intend to use.
8.  **Bulk delete:** You can select multiple servers for removal. Check the box next to each server you want to remove, then click **Delete selected** (the button shows the count when at least one server is selected). Confirm when prompted; the selected servers will be removed and the list updated.
9.  To export your current server list to a file for backup or sharing, click the **Export** icon (downward arrow).
10. When you are finished, switch back to **Migration & Merge** or **Validation** using the top navigation tabs.

### Step 2: Select Servers for Migration

1.  In the left panel, click on **Connection** to access connection settings.
2.  In the right panel, you'll see connection configuration options:
    *   **`selected_source_server`**: Choose the server you want to migrate *from*.
    *   **`selected_target_server`**: Choose the server you want to migrate *to*.
    *   **`verify`**: Enable SSL certificate verification (recommended).
3.  **Test Your Connections**: After selecting your servers, you can test the connections to ensure they're working properly:
    *   Click the **test connection icon** (✓) next to each server dropdown to verify the connection and API token.
    *   The icon will turn **green** for successful connections or **red** for failed connections.
    *   Test results will appear next to the button, showing connection status and authenticated user information.
    *   Results are automatically cleared when you change server selections or click the test button again.
4.  Click the **Save Settings** button in the top action bar to save your configuration.

---

## 4. Running a Migration 🏃‍♂️💨

### Step 1: Define the Migration Plan

The left panel contains migration switches where you select which assets you want to migrate.

1.  In the **left panel**, you'll see a list of migration options under "Migration Items".
2.  Toggle the switches for each type of asset you wish to migrate:
    - **Users** - Migrate user accounts
    - **Groups** - Migrate user groups
    - **Folders** - Migrate folder structure
    - **Dashboards** - Migrate dashboards
    - **Datamodels** - Migrate data models
    - **Saved Formulas** - Migrate saved formulas
    - **Saved Filters** - Migrate saved filters/perspectives
    - **Data Security** - Migrate data security rules
    - **Blox** - Migrate Blox actions
    - **Custom Code** - Migrate custom code (notebooks)
3.  As you enable switches, the corresponding configuration options will appear in the **right panel**.

### Step 2: Configure Migration Details

When you enable a migration switch in the left panel, the corresponding configuration options will appear in the right panel. Click on each category in the left panel to configure the specific settings for that migration type.

*   **Dashboards Configuration:**
    *   `dashboard_import_mode`: Choose `skip` (default) to avoid touching dashboards that already exist on the target, or `overwrite` to replace them.
    *   `dashboard_migration_mode`: Choose `ALL` to migrate everything, `By Name` to migrate dashboards by name, or `By OID` to migrate dashboards by OID (specified in `dashboard_include_list`).
    *   `dashboard_migration_concurrency`: Number of dashboards to migrate in parallel.
    *   `skip_dashboards_with_missing_owner`: Skip dashboards whose owner doesn't exist on the target.
*   **Datamodels Configuration:**
    *   `datamodel_overwrite`: Check this to replace datamodels on the target that have the same name.
    *   `enable_update_connections`: Enable dynamic connection updates during migration. When enabled, the `update_connections` and `post_import_update_connection_function` fields will become visible.
    *   `update_connections`: Define rules for updating connection details in datamodels (only visible when `enable_update_connections` is checked).
    *   `post_import_update_connection_function`: Path to a Python file containing custom functions for updating datamodel connections (only visible when `enable_update_connections` is checked).
    *   `auto_migrate_missing_custom_code_notebooks`: When enabled, if a datamodel requires custom code notebooks that are missing on the target server, the tool will automatically migrate those notebooks from the source server before continuing with the datamodel migration. The notebook IDs and codePaths are automatically updated to match the target environment.
*   **Custom Code Configuration:**
    *   `migrate_custom_code`: Enable migration of custom code (notebooks) from source to target server.
    *   `notebook_import_mode`: Choose `skip` (default) to avoid touching notebooks that already exist, `overwrite` to replace them, or `duplicate` to create duplicates with modified IDs.
    *   `notebook_include_list`: List of notebook IDs or display names to include in migration. Leave empty or set to 'ALL' to migrate all notebooks.
*   **Users Configuration:**
    *   `update_users_password`: Check this to set a default password for all newly migrated users.
    *   `migrate_users_chunk_size`: Number of users to process in each batch.
*   **Saved Filters Configuration:**
    *   When enabled, saved filters/perspectives will be migrated from source to target system.
*   **Saved Formulas Configuration:**
    *   When enabled, saved formulas will be migrated from source to target system.
*   **Data Security Configuration:**
    *   `migrate_datasecurity_chunk_size`: Number of data security rules to process in each batch.
*   **Blox Configuration:**
    *   `overwrite_existing_blox_actions`: Replace existing Blox actions on the target.

*   **Logging Configuration:**
    *   `file_logLevel` and `console_logLevel`: Set the global logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    *   `migration_log_levels`: Fine-grained control per migration item. In the Logging section, you can set a log level for each migration type (Groups, Users, Folders, Dashboards, Datamodels, etc.). Use "inherit" to fall back to the global level, or override (e.g., DEBUG for Dashboards) to get verbose output only for that item.

Review the settings in each category carefully. Hover over the `ⓘ` icon next to any setting for a detailed explanation.

### Step 3: Save Your Settings

Before running, it is critical to save your configuration.

*   Click the **Save Settings** button in the top action bar. You will see a confirmation message in the status bar at the bottom of the screen.

### Step 4: Run the Migration

1.  Click the **Run App** button in the top action bar.
2.  A confirmation dialog will appear. If you have `overwrite` mode enabled for dashboards or datamodels, you will see an additional warning.
3.  Click **Save and Run** (recommended) or **Run Without Saving**.

### Step 5: Monitor the Output

A new browser tab will automatically open to `migration_output.html`. This page shows a **live stream** of the logs from the migration script with **real-time progress bars** for active migration tasks. You can watch the progress here and see any errors or warnings as they occur.

The progress tracking includes:
- **Dashboard Migration Progress:** Visual progress bar showing current item and total count
- **Dashboard Sharing Progress:** Progress indicator for the "Share Source Dashboards with Migration User" preflight step
- **Smart Progress Management:** Only the currently active task's progress is displayed for a cleaner experience

---

## 5. Advanced Features 🧠

This section details advanced settings that provide fine-grained control over the migration process, helping you optimize performance, ensure data integrity, and automate complex workflows.

### Performance Tuning
These settings allow you to adjust how the tool interacts with the Sisense APIs to maximize speed and avoid rate-limiting issues.

*   **Dashboard & User Concurrency/Chunking:**
    *   `dashboard_migration_concurrency` (Dashboards Configuration): Specifies how many dashboards are migrated in parallel.
        *   **Use Case:** Increase this value on powerful machines with a stable network to speed up large dashboard migrations.
    *   `dashboard_fetch_chunk_size` (Dashboards Configuration): The number of dashboard metadata objects to fetch in a single API call.
        *   **Use Case:** Larger values reduce the number of API calls but increase memory usage. The default of 1000 is generally optimal.
    *   `migrate_users_chunk_size` (Users Configuration): The number of users to create in a single bulk API call.
        *   **Use Case:** This setting controls both performance and error handling. A larger chunk size (e.g., 1000) is more efficient as it reduces the number of API calls. However, if an error occurs while processing a large chunk (e.g., due to one invalid user record), the entire chunk fails. The tool is designed to handle this gracefully: it will automatically retry migrating the remaining users in that batch with a smaller chunk size, eventually narrowing it down to a single user to isolate the problem. Adjust this value based on the stability and performance of your target Sisense instance.

*   **API Rate Limiting:**
    *   `wait_between_chunks` & `wait_chunk_size_threshold` (Users Configuration): Instructs the tool to pause for a few seconds between large API requests.
        *   **Use Case:** Use this to prevent overwhelming a busy Sisense server or to avoid hitting API rate limits.

*   **Direct MongoDB Fetch:**
    *   `use_mongo_for_target_dashboards` & `target_mongo_connection_string` (Dashboards Configuration): Instead of using the API to check for existing dashboards on the target, the tool can connect directly to the Sisense application database (MongoDB).
        *   **Use Case:** This is the single most effective way to speed up dashboard migrations, as it reduces thousands of API calls to a single, fast database query. It requires network access to the target's MongoDB.

*   **Enhanced Performance Features:**
    *   **Batch Log Rendering:** The tool now uses batch log rendering to prevent page unresponsiveness during large migrations.
    *   **Fallback Dashboard Schema Strategy:** Implemented robust fallback mechanism for handling different dashboard schemas across different Sisense versions, automatically reducing migration failures.
    *   **Production-Ready Server:** Built on Waitress server for improved stability and better handling of concurrent requests.

### Data Integrity and Customization
These features allow for advanced mapping and transformation of data during the migration.

*   **Custom Dashboard OIDs:**
    *   `use_custom_dashboard_oid` & `oid_host_mapping` (Dashboards Configuration): This powerful feature allows you to create predictable, unique Object IDs (OIDs) for dashboards on the target server. The tool takes the original OID and replaces its prefix with a code you define for each source server.
        *   **Use Case:** Essential when merging multiple Sisense instances into one. It prevents OID collisions and makes it easy to identify the original source of a migrated dashboard.

*   **Datamodel Custom Code Support:**
    *   The tool automatically detects and validates custom code dependencies when migrating datamodels. This feature ensures that datamodels with custom_code tables (which reference notebooks) can only be migrated if the required notebooks already exist on the target server.
    *   **How It Works:**
        1.  **Automatic Detection:** Before migrating a datamodel, the tool scans the datamodel structure for tables of type "custom_code".
        2.  **Notebook ID Extraction:** For each custom_code table found, the tool extracts the `noteBookId` from the `customCode.noteBookId` field.
        3.  **Validation:** The tool checks if notebooks with these IDs exist on the target server.
        4.  **Auto-Migration (Optional):** If `auto_migrate_missing_custom_code_notebooks` is enabled and notebooks are missing, the tool will automatically migrate them from the source server. During auto-migration, the notebook IDs are updated to match the source IDs, and the `codePath` is automatically updated to reflect the correct notebook ID, ensuring proper references in the datamodel.
        5.  **CodePath Update:** If a notebook is found on the target (either pre-existing or auto-migrated), the tool automatically updates the `codePath` in the datamodel's smodel structure to match the notebook's codePath on the target server.
        6.  **Migration Safety:** If any required notebook is missing and auto-migration is disabled, the entire datamodel migration is skipped to prevent incomplete or broken datamodels.
    *   **Use Case:** This feature is essential when migrating datamodels that use custom code tables. It ensures that:
        *   Datamodels are only migrated when their dependencies (notebooks) are available
        *   The codePath references are automatically corrected to match the target environment
        *   Missing notebooks can be automatically migrated on-the-fly during datamodel migration
        *   Incomplete migrations are prevented, maintaining data integrity
    *   **Best Practice:** 
        *   **Option 1 (Recommended):** Enable `auto_migrate_missing_custom_code_notebooks` to automatically migrate missing notebooks during datamodel migration. This ensures all dependencies are handled automatically.
        *   **Option 2:** Migrate notebooks first (using the Custom Code migration feature) before migrating datamodels that depend on them. The tool will automatically validate this dependency and skip datamodel migration if notebooks are missing.

*   **Custom Datamodel Connection Updates:**
    *   This feature (`enable_update_connections`, `update_connections`, `post_import_update_connection_function`) in the Datamodels Configuration allows for dynamic modification of data model connections during migration. It's designed to solve the common problem of needing to change database connection details (server addresses, credentials, etc.) when moving from a development to a production environment.
    *   **Note:** The `update_connections` and `post_import_update_connection_function` fields are only visible when `enable_update_connections` is checked.
    *   **High-Level Flow:**
        1.  **Pre-Import Modification (`update_connections`):** This is the primary method. It allows you to modify a data model's connection details *in memory* right before it's imported into the target system.
        2.  **Post-Import Modification (`post_import_update_connection_function`):** This runs a single, global function *after* all selected data models have been imported. It's useful for bulk updates or verification steps that need to happen on the target system via its API.
    *   **Detailed Step-by-Step Process:**
        1.  **The Master Switch:** The entire process is controlled by `enable_update_connections` in your settings. If this is `false`, the rest of the logic is skipped.
        2.  **Rule Matching:** When enabled, the tool iterates through each dataset in a data model. It checks the dataset's `provider` (e.g., "SQLServer", "PostgreSQL") against the rules defined in the `update_connections` list in your settings.
        3.  **Dynamic Function Execution:** If a matching rule is found, the tool dynamically loads the `connection_update_functions.py` module and executes the function specified in the rule (e.g., `update_connection`).
            *   This custom function receives the `connection` object.
            *   You have complete freedom to modify the connection details within this function.
            *   The function **must return** the modified `connection` object, which then overwrites the original.
        4.  **Post-Import Hook:** After all data models are imported, the tool checks for the `post_import_update_connection_function` setting. If a function name is provided, it is executed once for any final, global adjustments on the target server.
    *   **Use Case:** Automatically retarget data models from a development database to a production database, or update credentials as part of the migration workflow.
    For a detailed walkthrough, see the Connection Remapping Example in the appendix.

### Folder Hierarchy & Permissions
*   `share_source_dashboards_with_migration_user` (Folders Configuration), `update_target_folders_owner` (Folders Configuration): The folder migration process is designed to replicate your source folder structure on the target system. It works by recursively scanning the source hierarchy and creating each folder on the target.
    *   **Use Case:** This is fundamental for organizing dashboards. The process relies on two key settings:
        1.  **`share_source_dashboards_with_migration_user`**: Before migration, the tool can share all source folders with the migration user. This is a critical pre-flight step to ensure the tool has permission to *see* and export the folder structure.
        2.  **`update_target_folders_owner`**: After a folder is created on the target, this setting ensures its ownership is updated to match the original owner (based on the user mapping). This maintains correct permissions and visibility.
    The tool creates a `folders_map.json` file to track the relationship between source and target folder OIDs, which is then used by the dashboard migration to place dashboards in the correct location.

### Automation and Error Handling
These settings help automate parts of the migration and gracefully handle common issues.

*   **Automatic Folder Migration:**
    *   `run_folder_migration_if_missing` (Folders Configuration): If the tool detects that a dashboard's parent folder doesn't exist on the target, it can automatically trigger a full folder migration first.
        *   **Use Case:** Saves time by ensuring the necessary folder structure is in place before migrating dashboards, preventing them from landing in the root folder.

*   **Skipping Logic:**
    *   `skip_dashboards_with_missing_owner` (Dashboards Configuration): If a dashboard's owner on the source system does not exist on the target system, the migration for that dashboard will be skipped.
        *   **Use Case:** Prevents migration failures and orphaned dashboards when you are intentionally not migrating all users.
    *   `skip_dashboards_with_missing_ancestor_folder` (Dashboards Configuration): If a dashboard's parent folder cannot be found or created on the target, the dashboard will be skipped.
        *   **Use Case:** Ensures dashboards are only migrated if their intended folder structure exists, maintaining organizational integrity.

*   **Multi-tenancy Support:**
    *   **Enhanced Connection Testing:** Improved connection testing capabilities specifically designed for multi-tenant environments.
    *   **Better Error Handling:** More robust error handling for various API response formats and connection scenarios.
    *   **Content-Type Handling:** Improved handling of incorrect Content-Type headers from export endpoints.

### Dashboard Validation (Validation Tab)
The **Validation** tab lets you run dashboard validation against a single server (e.g., after a migration) to check that dashboards and their widgets and JAQL queries respond correctly.

*   **Running validation:** Select a server from the dropdown, optionally enable **Include JAQL checks** (and choose JAQL mode), then click **Run validation**. Progress and live log output appear in the results area.
*   **Pause / Resume / Stop:** Use the control buttons to pause, resume, or stop a running validation job.
*   **Filtering results:** When validation has completed (or stopped), use the **Show:** dropdown in the results area to view **All** results or **Failed only**. "Failed only" shows only dashboards that have at least one failure (dashboard-level error, failed widget, or failed JAQL), making it easier to focus on issues.
*   **Export:** Export the full results as JSON or CSV using the Export button (export always includes all results, not just the filtered view).

---

## 6. Troubleshooting ❓

| Problem                                             | Solution                                                                                                                                                                                                                                                        |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Application doesn't start or closes immediately** | Your antivirus software might be blocking the executable. Try temporarily disabling it or adding an exception for `win2linux-merge-tool-web.exe`.                                                                                                                  |
| **"This site can’t be reached" in the browser**     | - Make sure the black console window is still open and did not show any critical errors on startup.<br>- Check your system's firewall to ensure it isn't blocking the application's network access.<br>- Ensure you are navigating to `http://localhost:5001`. |
| **Migration fails to start or errors out**          | - Check the `migration_output.html` tab for specific error messages.<br>- Open the `logs` folder (created next to the `.exe`) and review the most recent `.log` and `.err` files.<br>- **Common Causes:** Invalid API token, incorrect server host/port, or a network issue preventing connection to the Sisense server. |
| **Connection test fails**                            | - **Red icon/error message**: Check your API token, server host, port, and protocol settings.<br>- **"Connection successful but API token invalid"**: Your API token may be expired or lack sufficient permissions.<br>- **"Connection to [server] failed"**: Verify network connectivity and server availability.<br>- Use the test connection buttons in both the Connection panel and Manage Servers screen to diagnose issues. |
| **Settings are not saving**                         | The application needs permission to write to its configuration directory (`%APPDATA%\sisense_merge_and_migration`). Ensure your user account has standard permissions.                                                                                              |
| **A specific dashboard/user/etc. didn't migrate**   | Check the log files. The script is very verbose and will usually state why an item was skipped (e.g., it already exists, the owner is missing, it was in an exclude list).                                                                                             |

---

## 7. Appendix: Practical Examples

### Example Usage: Remapping SQL Server Connections

Here is a practical example of how to use this feature to automatically change the server and port for all `SQLServer` connections during a data model migration.

**1. Configure the Migration in the UI**

First, you need to enable the feature and define rules that tell the migration tool to run your custom `update_connection` function for any relevant data source providers.

1.  In the left panel, enable the **Datamodels** switch. This will show the Datamodels configuration in the right panel.
2.  Click on **Datamodels** in the left panel to access the configuration options.
3.  Find the `enable_update_connections` setting and check the box to enable it.
4.  Locate the `update_connections` setting, which is an editor for a list of rules.
5.  Click the **Add Connection Update** button. A new row with two input fields will appear.
6.  In the first field (labeled "Provider"), enter `sql`.
7.  In the second field (labeled "Function"), enter `update_connection`.
8.  Click **Add Connection Update** again.
9.  In the new row, enter `sql2005` for the "Provider" and `update_connection` for the "Function".
10. Click the **Save Settings** button in the top action bar to save your configuration.

**2. Create the Python Logic in `connection_update_functions.py`**

Next, you create a file named `connection_update_functions.py` in the same directory as the migration tool executable. This file contains the Python functions that will be executed.

In this example, the script contains all the necessary logic, including a hardcoded list of server names to be mapped to IP addresses. It also includes special handling for legacy `sql2005` providers.

### Example Usage: Multi-Tenancy Migration Configuration

Multi-tenancy migration allows you to migrate data from a single-tenant Sisense instance to a multi-tenant Sisense instance. This section explains how to configure the tool for multi-tenant environments.

**Understanding Multi-Tenancy in Sisense**

Multi-tenant Sisense instances use tenant-specific URLs and have different role structures compared to single-tenant instances. The migration tool automatically detects multi-tenant targets and adjusts the migration process accordingly.

**1. Configure Multi-Tenant Target Server**

When setting up your target server for multi-tenancy:

1. **Server URL Format**: Use the full tenant path in your server configuration:
   - **Single-tenant**: `https://my-sisense.com`
   - **Multi-tenant**: `https://my-sisense.com/tenant-name`

2. **Migration User Requirements**: The user account associated with your API token must have `tenantAdmin` permissions on the target tenant. Regular user accounts or admin accounts from the parent instance will not have sufficient permissions to perform migration operations within the tenant.

3. **API Token**: Ensure your API token has the appropriate permissions for the specific tenant.


**2. Automatic Role Mapping**

The tool automatically handles role mapping for multi-tenant targets:

- **Source roles** `super` and `admin` are automatically mapped to `tenantAdmin` on the target
- This ensures proper permissions are maintained in the tenant environment
- The tool will log these mappings during migration for transparency

**3. Configuration Steps**

1. **Add Multi-Tenant Target Server**:
   - In the left panel, click **Connection** to access connection settings
   - In the right panel, set your `selected_target_server` to the multi-tenant server
   - The tool will automatically detect the multi-tenant environment

2. **Configure Migration Settings**:
   - Enable the migration switches for the assets you want to migrate
   - The tool will automatically apply multi-tenant-specific logic during migration

3. **User and Group Migration**:
   - When migrating users, the tool automatically maps roles appropriately
   - Groups are migrated with tenant-specific permissions
   - The `tenantAdmin` role is used for users who had `super` or `admin` roles

4. **Dashboard and Folder Migration**:
   - Dashboards are migrated with proper tenant context
   - Folder structures are replicated within the tenant
   - Permissions are adjusted for the tenant environment

**4. Multi-Tenancy Limitations**

When migrating to multi-tenant environments, be aware of the following limitations:

1. **Custom Connection Update Functions**: The `update_connections` feature and custom `connection_update_functions.py` are not supported for multi-tenant targets. The dynamic connection modification functionality is disabled when the target server is detected as multi-tenant.

2. **Datamodel Migration with Custom Encryption**: Datamodel migration will not work when the target multi-tenant server uses a custom encryption key. This is due to encryption key differences between the source and target environments that cannot be automatically resolved.

This configuration ensures that all migrations to this server will be handled with multi-tenant logic, including automatic role mapping and tenant-specific API calls.
