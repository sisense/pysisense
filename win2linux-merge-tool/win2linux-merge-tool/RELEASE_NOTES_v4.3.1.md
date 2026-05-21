# Release Notes - v4.3.1

**Release Date:** January 2025  
**Version:** 4.3.1  
**Type:** Patch Release

## 🎉 Overview

This patch release fixes a critical issue with the auto-migration of custom code notebooks during datamodel migration. The fix ensures that notebook codePaths are properly updated when notebooks are automatically migrated, matching the behavior of the standard custom code migration process.

## 🐛 Bug Fixes

### Auto-Migration CodePath Update
- **Fixed CodePath Update** - Auto-migrated notebooks now properly update their `codePath` after the notebook ID is updated
- **Consistent Behavior** - Auto-migration now uses the same codePath update logic as the standard `migrate_custom_code()` function
- **Proper ID Replacement** - CodePath correctly replaces the original notebook ID with the created notebook ID, ensuring accurate references in datamodels

### Impact
- **Before:** When notebooks were auto-migrated during datamodel migration, the notebook ID was updated but the codePath was not, causing incorrect references in the datamodel
- **After:** Both notebook ID and codePath are now properly updated during auto-migration, ensuring datamodels reference the correct notebook paths

## 📊 Technical Details

### Key Changes

#### Auto-Migration CodePath Update
- Added codePath update logic in the auto-migration section of `migrate_datamodels()`
- After updating notebook ID from `created_notebook_id` to `notebook_id`, the codePath is now updated to replace all instances of `notebook_id` with `created_notebook_id`
- Matches the exact behavior implemented in `migrate_custom_code()` function
- Includes proper error handling and logging for codePath update operations

### Files Modified
- `sisense_migration_and_merge_tool.py` - Added codePath update logic in auto-migration section (lines 2315-2344)
- `USER_MANUAL.md` - Updated documentation to reflect codePath update during auto-migration

### Breaking Changes
- None - This is a bug fix that improves existing functionality

### Migration Notes
- No configuration changes required
- Existing migrations will benefit from this fix automatically
- Auto-migration behavior is now consistent with manual custom code migration

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.

