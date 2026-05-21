# Release Notes - v5.0.1

**Release Date:** February 2026  
**Version:** 5.0.1  
**Type:** Patch Release

## Overview

This patch release includes improvements to the build and deployment process, along with code quality enhancements. Version 5.0.1 focuses on streamlining the release workflow and improving code maintainability.

## Improvements

### CI/CD Enhancements
• **Automatic Release Notes Integration** - GitLab CI/CD now automatically detects and includes release notes when creating releases
  - Automatically finds `RELEASE_NOTES_v{VERSION}.md` files based on tag version
  - Extracts version from tag and includes corresponding release notes
  - Falls back to default description if release notes file not found
  - Streamlines the release creation process

### Code Quality
• **Code Cleanup** - Removed redundant notebook update logic from `migrate_datamodels()` function
  - Streamlined migration process
  - Improved code clarity and maintainability
  - Eliminated duplicate code paths

## Technical Details

### Modified Files
• `.gitlab-ci.yml` - Enhanced `create_release` job to automatically include release notes from markdown files
• `sisense_migration_and_merge_tool.py` - Removed redundant notebook update logic

### Build Process
• No changes to build process
• Existing build scripts continue to work as before

## Platform Support

### Supported Platforms
• **Windows** - Full support (existing)
• **macOS** - Full support (existing)

---


**Contributors:** davidhogeg

**Support:** For issues or questions, please refer to the project documentation or contact the development team.

**Download:** 
• macOS: Available via GitLab CI/CD artifacts
• Windows: Available via GitLab CI/CD artifacts
