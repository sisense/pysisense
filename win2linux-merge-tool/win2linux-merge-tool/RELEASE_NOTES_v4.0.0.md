# Release Notes - v4.0.0

**Release Date:** January 2025  
**Version:** 4.0.0  
**Type:** Major Release

## 🎉 Overview

This major release introduces significant improvements to the Sisense Migration and Merge Tool, focusing on enhanced reliability, better user experience, and improved performance. Version 4.0.0 represents a major milestone with substantial feature additions and stability improvements.

## 🚀 New Features

### Enhanced Migration Capabilities
- **Saved Filters (Perspectives) Migration** - Added support for migrating saved filters/perspectives between Sisense instances
- **Decoupled Saved Formula Migration** - Improved saved formula migration with better separation of concerns
- **Multi-tenancy Support** - Enhanced support for multi-tenant environments with improved connection testing

### Real-time Progress Tracking
- **Real-time Progress Bars** - Added live progress tracking for dashboard migration operations
- **Dashboard Sharing Progress** - New progress bar specifically for "Share Source Dashboards with Migration User" preflight step
- **Enhanced Progress Visibility** - Improved progress bar management to show only active tasks

### Production-Ready Server
- **Waitress Server Integration** - Replaced Flask development server with production-ready Waitress server
- **Improved Performance** - Better handling of concurrent requests and improved stability

## 🔧 Improvements

### API & Connection Handling
- **Fallback Dashboard Schema Strategy** - Implemented robust fallback mechanism for handling different dashboard schemas
- **Enhanced Error Handling** - Improved error handling for non-JSON API responses
- **Content-Type Handling** - Better handling of incorrect Content-Type headers from export endpoints
- **Connection Testing** - Enhanced connection testing capabilities for multi-tenant targets

### User Interface Enhancements
- **Progress Bar Improvements** - Enhanced progress bar visibility and interaction
- **UI Layout Fixes** - Improved layout for checkbox options and form elements
- **Left Panel Interaction** - Fixed left panel highlighting and interaction issues
- **Modal Behavior** - Prevented server manager modal from closing on outside clicks
- **Save Button Logic** - Corrected server manager save button functionality

### Performance & Stability
- **Batch Log Rendering** - Implemented batch log rendering to prevent page unresponsiveness
- **Unicode Error Prevention** - Fixed UnicodeDecodeError in log streaming
- **Logging Improvements** - Corrected logger calls to prevent AttributeError
- **Data Model Hooks** - Prevented connection hook errors with empty function names

## 🐛 Bug Fixes

### Frontend Fixes
- **Syntax Error Resolution** - Removed duplicated code causing syntax errors in index.html
- **UI Interaction Issues** - Fixed various UI interaction and highlighting problems
- **Modal Behavior** - Corrected modal closing behavior and save button logic

### Backend Fixes
- **API Response Handling** - Improved handling of various API response formats
- **Error Prevention** - Fixed multiple error conditions in data model and connection handling
- **Logging Issues** - Resolved logging-related errors and improved error reporting

### Configuration & Setup
- **Gitignore Updates** - Added .DS_Store to .gitignore and cleaned up existing files
- **Multi-tenant Settings** - Fixed connection update settings for multi-tenant targets

## 📊 Technical Details

### Commits Included (20 commits)
- **Dashboard Schema Handling** - Enhanced fallback strategy for different dashboard formats
- **Multi-tenancy Support** - Improved support for multi-tenant environments
- **Progress Tracking** - Real-time progress bars for migration operations
- **Server Infrastructure** - Production-ready server implementation
- **UI/UX Improvements** - Multiple frontend enhancements and bug fixes
- **Error Handling** - Comprehensive error handling improvements
- **Performance Optimizations** - Various performance and stability improvements

### Breaking Changes
- **Server Configuration** - Server now runs on Waitress instead of Flask dev server (may require configuration updates)
- **Progress API** - New progress tracking endpoints (backward compatible)

### Migration Notes
- No database schema changes required
- Existing configurations remain compatible
- New progress tracking features are optional and backward compatible

## 🔄 Upgrade Instructions

1. **Backup Current Installation** - Always backup your current installation before upgrading
2. **Update Configuration** - Review server configuration if using custom settings
3. **Test Connection** - Verify connections to both source and target systems
4. **Run Pre-flight Checks** - Execute pre-flight checks to ensure compatibility

## 📈 Performance Improvements

- **Faster Dashboard Migration** - Improved performance through better schema handling
- **Reduced Memory Usage** - Optimized log rendering and batch processing
- **Better Error Recovery** - Enhanced error handling reduces migration failures
- **Improved UI Responsiveness** - Batch rendering prevents UI freezing during large migrations

## 🛡️ Security & Reliability

- **Production Server** - Waitress server provides better security and stability
- **Enhanced Error Handling** - More robust error handling prevents crashes
- **Better Logging** - Improved logging for better debugging and monitoring

## 📝 Documentation Updates

- Updated user manual with new features
- Enhanced API documentation
- Improved troubleshooting guides

## 🎯 What's Next

This release sets the foundation for future enhancements including:
- Advanced migration strategies
- Enhanced reporting capabilities
- Additional data source support
- Performance monitoring features

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.
