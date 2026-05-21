# frontend/utils.py

import os
import sys  # Import sys
import yaml
from config_loader import (SERVERS_FILE, APP_NAME)  # Use relative import
from utils.utils import get_user_config_path


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # Not running as a bundled app, use the normal path relative to this script
        # Go up one level from utils.py (frontend) to the project root
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    return os.path.join(base_path, relative_path)


def load_servers():
    """Loads server details from servers.yaml."""
    # servers_path = resource_path(SERVERS_FILE)  # Use helper function
    servers_path = get_user_config_path(SERVERS_FILE, APP_NAME)  # Use helper function
    try:
        if os.path.exists(servers_path):
            with open(servers_path, 'r', encoding='utf-8') as f:
                servers = yaml.safe_load(f)
                # Ensure allowed_roles exists and is a list, default to both if missing
                if isinstance(servers, dict):
                    for name, details in servers.items():
                        if not isinstance(details.get('allowed_roles'), list):
                            print(
                                f"Warning: Server '{name}' missing or invalid 'allowed_roles'. Defaulting to ['source', 'target'].")
                            details['allowed_roles'] = ['source', 'target']
                return servers if isinstance(servers, dict) else {}
        else:
            return {}  # Return empty dict if file doesn't exist
    except (IOError, yaml.YAMLError) as e:
        print(f"Warning: Could not load server details from {servers_path}: {e}")
        return {}


def save_servers(servers_dict):
    """Saves the server details dictionary to servers.yaml."""
    # servers_path = resource_path(SERVERS_FILE)  # Use helper function
    servers_path = get_user_config_path(SERVERS_FILE, APP_NAME)  # Use helper function
    try:
        # Ensure directory exists (relative to the potentially temporary path)
        os.makedirs(os.path.dirname(servers_path), exist_ok=True)
        with open(servers_path, 'w', encoding='utf-8') as f:
            yaml.dump(servers_dict, f, default_flow_style=None, sort_keys=True, indent=2)
        return True
    except (IOError, yaml.YAMLError) as e:
        print(f"Error: Could not save server details to {servers_path}: {e}")
        return False
