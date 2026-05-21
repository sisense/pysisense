# Release Notes - v6.0.0

**Release Date:** February 2026  
**Version:** 6.0.0  
**Type:** Major Release

## Overview

This major release focuses on **navigation and workflow improvements** in the web UI. Version 6.0.0 introduces a top-level **Manage Servers** tab, **bulk delete** for servers, and a **validation results filter**, making server management and post-migration validation more efficient.

## New Features

### Navigation & Layout
• **Manage Servers in Top Navigation** - "Manage Servers" is now a primary tab in the top navigation bar alongside "Migration & Merge" and "Validation".
  - Full-page server management view instead of a modal.
  - Click a server in the list to view or edit its details; use the same form to add a new server.
  - Switch between Migration & Merge, Validation, and Manage Servers via tabs—no separate "Back" button in the Manage Servers view.

### Server Management
• **Bulk Delete Servers** - In the Manage Servers view you can select multiple servers for deletion.
  - Checkboxes next to each server in the list; use **Delete selected (N)** in the list header to remove all selected servers after confirmation.
  - Works in both the Manage Servers tab and the legacy Manage Servers modal (when opened from Migration & Merge).
  - Single-server delete (from the details form) still available; deleted server is removed from the bulk selection set.

### Validation
• **Validation Results Filter** - In the Validation tab results area, a **Show:** dropdown lets you switch between:
  - **All** – show every dashboard result (default).
  - **Failed only** – show only dashboards that have at least one failure (dashboard-level error, failed widget, or failed JAQL).
  - Filter applies to both completed and stopped validation runs; changing the filter re-renders the list instantly. Export always includes the full result set.

## Improvements

### User Manual
• **USER_MANUAL.md** updated for v6.0.0:
  - Top navigation (Migration & Merge, Validation, Manage Servers) and layout description.
  - Manage Servers as a tab with bulk delete steps.
  - New "Dashboard Validation (Validation Tab)" section describing how to run validation and use the results filter.

### UI/UX
• **Consistent server list behavior** - Server list click populates the details form in both the tab view and the modal.
• **Clearer validation workflow** - Filter makes it easier to focus on failed dashboards after a run.

## Technical Details

### Modified Files
• `server.py` - Version set to 6.0.0.
• `frontend/index.html` - Top nav with Manage Servers tab; server list with checkboxes and bulk delete; validation results filter (All / Failed only); shared helpers for validation rendering.
• `USER_MANUAL.md` - Navigation, Manage Servers, bulk delete, and Validation tab documentation.

### Version
• **API/UI version:** 6.0.0 (exposed via `/api/version` and in the UI footer).

## Platform Support

No change from v5.x. Supported platforms:
• **Windows** - Full support  
• **macOS** - Full support  

---


**Support:** For issues or questions, please refer to the project documentation or contact the development team.

**Download:** Available via GitLab CI/CD artifacts (Windows and macOS).
