# Sisense Migration & Merge Tool

A robust tool for migrating and merging Sisense environments using a modern web UI. Designed for data teams and administrators to safely transfer dashboards, users, datamodels, and more between Sisense instances.

**➡️ [View the User Manual](USER_MANUAL.md) ⬅️**

---

## Table of Contents
- [Features](#features)
- [Screenshots](#screenshots)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Web UI](#web-ui)
  - [Command Line](#command-line)
- [Migration Settings](#migration-settings)
- [Advanced Options](#advanced-options)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Features
- **Migrate Assets:** Transfer Dashboards, Users, Groups, Folders, Datamodels, Data Security, and Blox Actions between Sisense environments.
- **Modern Web UI:** A Flask and Tailwind-based interface for easy configuration and real-time monitoring.
- **Live Output Streaming**: See migration logs and progress in real time.
- **Configurable Migration Plan**: Select what to migrate, filter dashboards, set concurrency, and more.
- **Robust Logging**: File and console logs, with log rotation and error tracking.
- **Safe Settings Management**: YAML-based config, with validation and backup.
- **Stop/Cancel Migration**: Terminate long-running migrations from the web UI.
- **Customizable**: Extend with your own connection update functions and advanced settings.

---

## Screenshots
> _Add screenshots of the web UI, migration output, etc. here!_

---

## Requirements
- Python 3.8+
- [See `requirements.txt`](requirements.txt) for all dependencies
- Sisense REST API access (admin credentials)
- (Optional) MongoDB access for direct fetch features

---

## Installation
1. **Clone the repository:**
   