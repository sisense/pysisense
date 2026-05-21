# Release Notes - v4.3.0

**Release Date:** January 2025  
**Version:** 4.3.0  
**Type:** Minor Release

## 🎉 Overview

This release introduces datamodel custom code support with notebook validation, improves the dashboard migration workflow with a new "By OID" option, and enhances the user interface with conditional field visibility. Version 4.3.0 focuses on improving datamodel migration reliability and providing more flexible dashboard migration options.

## 🚀 New Features

### Datamodel Custom Code Support
- **Custom Code Table Detection** - Automatically scans datamodels for custom_code tables before migration
- **Notebook Validation** - Checks if required notebooks exist on the target server before migrating datamodels
- **Automatic CodePath Updates** - Updates codePath in smodel when matching notebooks are found on target server
- **Migration Safety** - Skips datamodel migration if required notebooks are missing, preventing incomplete migrations

### Dashboard Migration Enhancements
- **By OID Migration Mode** - New option to migrate dashboards by specifying their OIDs (Object IDs)
- **Flexible Dashboard Selection** - Users can now choose between "ALL", "By Name", or "By OID" for dashboard migration
- **Dynamic UI Labels** - Dashboard include list labels and placeholders update dynamically based on selected mode

## 🔧 Improvements

### User Interface Enhancements
- **Conditional Field Visibility** - Connection update fields (`update_connections` and `post_import_update_connection_function`) are now only visible when `enable_update_connections` is checked
- **Cleaner Datamodel Options Tab** - Reduced UI clutter by hiding advanced options until needed
- **Improved User Experience** - Better organization of datamodel configuration options

### Reliability & Safety
- **Race Condition Fix** - Rechecks datamodel existence immediately before import to prevent conflicts
- **Better Error Handling** - Enhanced error handling for notebook validation and custom code table processing
- **Comprehensive Logging** - Detailed logging for custom code table scanning and notebook validation

## 🐛 Bug Fixes

### Datamodel Migration
- **Race Condition Prevention** - Fixed potential issue where datamodel existence check could become stale during custom_code scanning
- **Notebook Validation Errors** - Improved error handling when checking notebooks on target server

### Configuration Validation
- **Mode Validation** - Updated validation to accept "By OID" as a valid dashboard migration mode
- **Field Visibility** - Fixed field visibility issues in both web and desktop UIs

## 📊 Technical Details

### Key Changes

#### Datamodel Custom Code Support
- Scans smodel structure (datasets → schema → tables) for tables of type "custom_code"
- Extracts `noteBookId` from `customCode.noteBookId` field
- Validates notebook existence on target server before migration
- Updates `codePath` in smodel when notebooks are found
- Skips entire datamodel migration if required notebooks are missing

#### Dashboard Migration Mode
- Added "By OID" to `DASHBOARD_MIGRATION_MODES` configuration
- Updated validation in `config_loader.py` to accept "By OID"
- Enhanced UI to support OID-based dashboard selection
- Dynamic label and placeholder updates based on selected mode

#### UI Improvements
- Conditional visibility for connection update fields
- Works in both web UI (index.html) and desktop UI (main_window.py)
- Fields hidden by default, shown when `enable_update_connections` is enabled

### Files Modified
- `sisense_migration_and_merge_tool.py` - Added custom code support and race condition fix
- `frontend/config.py` - Added "By OID" to migration modes
- `config_loader.py` - Updated validation for new migration mode
- `frontend/index.html` - Added conditional visibility and "By OID" option
- `frontend/main_window.py` - Added conditional visibility and "By OID" support
- `USER_MANUAL.md` - Updated documentation

### Breaking Changes
- None - All changes are backward compatible

### Migration Notes
- Existing configurations remain compatible
- New "By OID" mode is optional
- Custom code validation is automatic and non-intrusive
- Connection update fields visibility is automatic based on checkbox state

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.

