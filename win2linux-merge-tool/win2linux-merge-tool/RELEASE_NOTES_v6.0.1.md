# Release Notes - v6.0.1

**Release Date:** February 2026  
**Version:** 6.0.1  
**Type:** Patch Release

## Overview

This patch release improves the **macOS application** (validation in the frozen app, removal of pkg_resources warnings) and refines the **web UI** (minimal top nav, Stop Server in nav, Migration view title and layout).

## New Features & Improvements

### macOS Application
• **Validation in frozen app** – Dashboard validation now runs correctly when the app is built with PyInstaller (e.g. the macOS `.app` bundle).
  - The frozen executable supports **`--run-validation-fetch`** and **`--run-validation-run`** modes so validation runs in subprocesses instead of invoking `.py` scripts (which the bundle cannot run).
  - Dashboard fetch and validation run are bundled and invoked via these flags from the server.
• **pkg_resources warning addressed** – Root cause fix by pinning **setuptools to 69.x** (`setuptools>=69.0.0,<70.0.0`) in `requirements.txt`. Setuptools 70+ emits a deprecation warning when PyInstaller’s loader imports `pkg_resources`; 69.x does not. Build the macOS app in an environment with this pinned setuptools for a clean log.
• **Fallback suppression** – Warning filters and `PYTHONWARNINGS` for validation subprocesses remain as a fallback if a newer setuptools is present.

### Web UI
• **Minimal top navigation** – The header uses a minimal layout: full title and tagline, then a single row with **underline-style tabs** (Migration & Merge, Validation, Manage Servers) and **Stop Server** aligned right as a text link. No heavy bar or card around the tabs.
• **Stop Server in top nav** – The Stop Server control is in the top nav area (right side) and is available from every view, not only the Migration view.
• **Migration view title** – The Migration & Merge view now has a title block (“Migration & Merge”) and short description, consistent with the Validation and Manage Servers views.
• **Migration view layout** – The title block, action buttons row, and main two-panel content are joined as one continuous card with no vertical gap between them (shared borders, rounded top and bottom).

## Technical Details

### Modified Files
• `server.py` – Version set to 6.0.1; frozen-app `--run-validation-fetch` / `--run-validation-run` dispatch; warning filter and `PYTHONWARNINGS` for validation subprocesses.
• `frontend/index.html` – Minimal nav with underline tabs and Stop Server; Migration view title block and single-card layout.
• `validation_fetch_standalone.py` – pkg_resources warning filter.
• `validation_run_standalone.py` – pkg_resources warning filter.
• `requirements.txt` – setuptools pinned to `>=69.0.0,<70.0.0`.
• `build-web-macos.spec` – `validation_fetch_standalone` and `validation_run_standalone` in datas and hiddenimports; comment about setuptools pin.

### Version
• **API/UI version:** 6.0.1 (exposed via `/api/version` and in the UI footer).

## Platform Support

No change from v6.0.0. Supported platforms:
• **Windows** – Full support  
• **macOS** – Full support (validation and logs improved in frozen app)

---


**Support:** For issues or questions, please refer to the project documentation or contact the development team.

**Download:** Available via GitLab CI/CD artifacts (Windows and macOS).
