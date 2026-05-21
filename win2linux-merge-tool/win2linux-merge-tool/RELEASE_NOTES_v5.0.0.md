# Release Notes - v5.0.0

**Release Date:** February 2026  
**Version:** 5.0.0  
**Type:** Major Release

## Overview

This major release introduces **macOS build support** and significant user experience improvements. Version 5.0.0 makes the Sisense Migration and Merge Tool available as a native macOS application, while adding convenient server management features that work across all platforms.

## New Features

### macOS Application Support
• **Native macOS Build** - Full PyInstaller support for creating `.app` bundles on macOS
• **Automated Build Script** - `build-macos.sh` script automates the entire build process including:
  - Icon conversion (PNG → ICNS format)
  - Virtual environment setup with `uv`
  - Dependency installation
  - PyInstaller bundling
  - App bundle creation with proper structure
• **Icon Support** - Automatic icon handling with `fileicon` integration for proper macOS icon display
• **Portable App Bundles** - App bundles work correctly when deployed to other macOS systems

### Server Management Enhancements
• **Stop Server Button** - New "Stop Server" button in the web UI for graceful server shutdown
  - Confirmation dialog before shutdown
  - Works on both Windows and macOS
  - No need to use Terminal/Command Prompt to stop the server
• **Automatic Browser Opening** - Browser automatically opens to the application URL when the app starts
  - 1.5 second delay to ensure server is ready
  - Cross-platform compatible (Windows, macOS, Linux)
  - Falls back gracefully if browser cannot be opened

### Custom Code Migration Improvements
• **Fixed Custom Code Processing Logic** - Resolved critical bug where custom code processing loop was incorrectly placed inside exception handler
  - Loop now executes correctly when notebooks are successfully retrieved
  - Proper control flow ensures custom code migration only runs when appropriate
  - Fixed indentation and exception handling structure
• **Enhanced Notebook Migration** - Improved notebook migration capabilities with better permission handling
  - Better handling of 401/403 errors when checking notebooks on target server
  - Graceful error handling for notebook permission issues
  - Improved logging for notebook operations (changed 'notebook not found' from WARNING to INFO)
• **Code Cleanup** - Removed debug instrumentation and agent debug code from migration functions
• **Testing Tools** - Added `test_notebook_update_permissions.py` for independent testing of notebook update permissions
  - Helps diagnose 401 errors during notebook updates
  - Tests API token permissions for PATCH endpoint
  - Can be run standalone to verify notebook update capabilities

## Improvements

### Build System
• **macOS Build Spec** - New `build-web-macos.spec` file for macOS PyInstaller builds
• **Icon Management** - Standalone `set-app-icon.sh` script for icon management
• **Build Automation** - Comprehensive build script with cleanup options (`--clean` flag)
• **Dependency Management** - Improved dependency handling with `uv` package manager

### Server Infrastructure
• **Graceful Shutdown Endpoint** - New `/api/shutdown` endpoint for clean server termination
• **Cross-Platform Compatibility** - All new features work on both Windows and macOS
• **Improved Startup Messages** - Clear console messages indicating server status and available actions

### Frontend Enhancements
• **UI Improvements** - Enhanced action button layout with new "Stop Server" button
• **Better Error Handling** - Improved error messages and user feedback
• **Status Bar Updates** - Enhanced status bar messaging for server operations

## Bug Fixes

### Custom Code Migration
• **Fixed Control Flow Bug** - Resolved critical issue where custom code processing loop was incorrectly placed inside exception handler
  - Loop now only executes when notebooks are successfully retrieved
  - Proper error handling prevents loop execution when `skip_migration = True`
  - Fixed indentation that caused loop to always execute regardless of error state
• **Improved Error Handling** - Better handling of 401/403 errors when checking notebooks on target server
  - Graceful handling of permission errors
  - Proper skip logic when notebooks cannot be checked
• **Code Quality** - Removed debug instrumentation and cleaned up debug code from migration functions

### Build & Deployment
• **Syntax Error Fix** - Fixed orphaned `else:` block in `sisense_migration_and_merge_tool.py` that prevented macOS builds
• **Module Import Errors** - Resolved `ModuleNotFoundError` issues by ensuring all dependencies are properly bundled
• **Library Loading** - Fixed Python shared library loading issues in macOS app bundles
• **File Path Issues** - Resolved `FileNotFoundError` for bundled resources in macOS app bundles

### Configuration
• **Setuptools Warning** - Suppressed `pkg_resources` deprecation warning by pinning `setuptools<81`
• **Requirements Updates** - Updated `requirements.txt` with proper dependency versions

## Technical Details

### New Files
• `build-web-macos.spec` - PyInstaller spec file for macOS builds
• `build-macos.sh` - Automated build script for macOS
• `set-app-icon.sh` - Icon management script
• `assets/icon.icns` - macOS icon file
• `test_notebook_update_permissions.py` - Notebook permission testing script
• `FIX_PLAN_custom_code_logic.md` - Custom code migration documentation

### Modified Files
• `server.py` - Added shutdown endpoint and auto-open browser functionality
• `frontend/index.html` - Added "Stop Server" button and related UI enhancements
• `sisense_migration_and_merge_tool.py` - Fixed syntax error, fixed custom code processing logic, improved error handling, removed debug code
• `SisenseRESTAPIClientClass.py` - Enhanced API client capabilities
• `config_loader.py` - Configuration improvements
• `utils/logger_setup.py` - Logger setup improvements

### Build Process
- **macOS Build**: Run `./build-macos.sh` (or `./build-macos.sh --clean` for a clean build)
- **Windows Build**: Continue using existing `build-web-windows.spec` file
- **Icon Handling**: Automatic conversion and embedding for macOS

## Platform Support

### Supported Platforms
• **Windows** - Full support (existing)
• **macOS** - Full support (new in v5.0.0)

### Build Requirements
- **Windows**: PyInstaller (existing setup)
- **macOS**: 
  - Python 3.x
  - `uv` package manager (recommended) or standard pip
  - `sips` and `iconutil` (built-in macOS tools)
  - `fileicon` (optional, auto-installed via Homebrew if available)

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.

**Download:** 
• macOS: Available via GitLab CI/CD artifacts
• Windows: Available via GitLab CI/CD artifacts
