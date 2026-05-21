# Release Notes

## Release v4.3.2

See [RELEASE_NOTES_v4.3.2.md](RELEASE_NOTES_v4.3.2.md) for detailed release notes.

**Highlights:**
- Fixed datamodel import encryption error when servers have different encryption keys
- Connection parameters are now automatically re-encrypted for target server before import
- Updated dashboard ownership type parameter for better retrieval

## Release v4.3.1

See [RELEASE_NOTES_v4.3.1.md](RELEASE_NOTES_v4.3.1.md) for detailed release notes.

**Highlights:**
- Fixed codePath update in auto-migration of custom code notebooks
- Auto-migration now properly updates notebook codePaths after ID update
- Consistent behavior between auto-migration and manual custom code migration

## Release v4.3.0

See [RELEASE_NOTES_v4.3.0.md](RELEASE_NOTES_v4.3.0.md) for detailed release notes.

**Highlights:**
- Datamodel custom code support with notebook validation
- New "By OID" dashboard migration mode
- Conditional visibility for connection update fields
- Race condition fix for datamodel migration

## Release v3.2.2

This release focuses on enhancing the extensibility and robustness of the data model migration feature, improving diagnostic logging, and fixing key bugs.

### ✨ New Features

*   **External Hook for Data Model Connections:** The application can now dynamically load a user-provided `connection_update_functions.py` file placed next to the executable. This allows for powerful, on-the-fly modifications of data model connection strings during migration without altering the core application.

### 🚀 Improvements

*   **Enhanced Dashboard Exclusion Logging:** The migration log now includes detailed `DEBUG` level entries explaining the exact reason why a specific dashboard was excluded, making it much easier to troubleshoot filtering issues.
*   **Comprehensive User Manual:** The `USER_MANUAL.md` has been significantly updated with a new appendix containing a practical, step-by-step example for remapping database connections using the new external hook feature.
*   **Redacted Code Examples:** The documentation now includes a safe, redacted version of the custom connection script for users to reference.
*   **Clearer README:** The main `README.md` has been updated to better distinguish between installation instructions for end-users and setup for developers.

### 🐛 Bug Fixes

*   **Data Model Migration Stability:** Fixed a `TypeError` that could occur when migrating data models containing datasets without a connection object.
*   **Connection Hook Robustness:** The custom connection hook function is now more resilient and no longer raises a `KeyError` when processing data sources that do not have a `server` property (e.g., CSV files).
*   **Build Process:** The PyInstaller build spec has been corrected to exclude the `connection_update_functions.py` file, ensuring the application correctly uses the external user-provided script instead of a bundled one.

## Version 3.1.1

This release focuses on significant performance enhancements, new data integrity features, and improved user experience in the web UI.

### ✨ Features

*   **High-Performance Dashboard Fetching via MongoDB**: Added a new option to fetch target dashboards directly from the Sisense application database (MongoDB). This dramatically reduces the time required for dashboard migrations by replacing thousands of API calls with a single, fast database query.
*   **Concurrent Dashboard Migration**: Enabled parallel processing for dashboard migrations, allowing multiple dashboards to be migrated simultaneously for a significant performance boost.
*   **Export Settings & Servers**: Users can now export their `settings.yaml` and `servers.yaml` files directly from the web UI, making it easier to back up, share, and manage configurations.
*   **Runtime Analytics**: Added an optional setting (`enable_runtime_analytics`) to generate a `dashboard_migration_timing.json` file, providing detailed performance metrics for troubleshooting and optimization.

### 🚀 Improvements & Fixes

*   **Live Migration Output Stream**: The "Run App" button now opens a new browser tab that streams the live log output from the migration script, providing real-time visibility into the process.
*   **JAQL Data Integrity**: The internal JAQL scanning function (`scan_and_fix_jaql`) has been enhanced to more robustly correct formatting issues in dashboard metadata, preventing potential widget errors after migration.
*   **Configuration & Validation**:
    *   Settings validation is now more comprehensive, checking for optional keys, warning about unknown keys, and correctly coercing data types.
    *   The YAML preview now correctly includes all advanced settings.
*   **Logging**:
    *   Fixed a critical bug that could cause an "I/O operation on closed file" error by improving how the logger is initialized and its handlers are managed.
    *   Corrected the logging shutdown sequence to prevent resource leaks.
*   **API & Fetching**:
    *   The `dashboard_fetch_chunk_size` setting is now correctly applied to all dashboard fetching operations from the source API.
*   **Unicode Error Fix**: Resolved a `UnicodeEncodeError` that occurred on Windows when running the migration as a subprocess by forcing UTF-8 encoding.
