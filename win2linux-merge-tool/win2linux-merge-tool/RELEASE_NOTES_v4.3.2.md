# Release Notes - v4.3.2

**Release Date:** January 2025  
**Version:** 4.3.2  
**Type:** Patch Release

## 🎉 Overview

This patch release fixes a critical issue with datamodel migration when source and target servers use different encryption keys. The fix ensures that connection parameters are properly re-encrypted for the target server before import, preventing decryption errors. Additionally, this release includes an improvement to dashboard retrieval by updating the ownership type parameter.

## 🐛 Bug Fixes

### Datamodel Migration Encryption Error
- **Fixed Connection Parameter Re-encryption** - Connection parameters are now automatically decrypted using the source server's API and re-encrypted using the target server's API before datamodel import
- **Resolves 500 Error** - Fixes the "Request failed with status code 400" error during datamodel import when servers have different encryption keys
- **Handles Multiple Formats** - Supports both encrypted string format and decrypted dictionary format for connection parameters
- **Comprehensive Error Handling** - Includes proper error handling and logging for decryption/re-encryption operations

### Impact
- **Before:** Datamodel imports failed with a 500 error when the target server attempted to decrypt connection parameters encrypted with the source server's key
- **After:** Connection parameters are automatically re-encrypted for the target server, allowing successful datamodel imports between servers with different encryption keys

## 🔧 Improvements

### Dashboard Retrieval
- **Updated Ownership Type** - Changed `ownershipType` parameter from `"root"` to `"allRoot"` in `get_dashboards_admin` API call
- **Better Dashboard Fetching** - May improve dashboard retrieval behavior in certain scenarios

## 📊 Technical Details

### Key Changes

#### Connection Parameter Re-encryption
- Added automatic decryption/re-encryption logic before datamodel import
- Processes all datasets in the smodel that contain connection parameters
- Decrypts parameters using source server's `/v1/encryption/decrypt` endpoint
- Re-encrypts parameters using target server's `/v1/encryption/encrypt` endpoint
- Handles both string (encrypted) and dict (decrypted) parameter formats
- Includes comprehensive error handling with detailed logging

#### Dashboard API Parameter Update
- Updated `ownershipType` from `"root"` to `"allRoot"` in dashboard admin API calls
- May improve dashboard retrieval in multi-tenant or complex permission scenarios

### Files Modified
- `sisense_migration_and_merge_tool.py` - Added connection parameter re-encryption logic and updated ownershipType parameter

### Breaking Changes
- None - This is a bug fix that improves existing functionality

### Migration Notes
- No configuration changes required
- Existing migrations will benefit from this fix automatically
- The re-encryption process is transparent and happens automatically before import
- If decryption/re-encryption fails, the migration continues and will fail with a clearer error message

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.

