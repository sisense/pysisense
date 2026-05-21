# /migration/dashboards_migration.py

import json
import logging
import os
import sys
import re

from utils.utils import get_user_config_path
from config_loader import APP_NAME

if getattr(sys, 'frozen', False):
    # --- Case 1: Application is frozen ---
    application_path = os.path.dirname(sys.executable)
else:
    # --- Case 2: Application is a standard script ---
    try:
        application_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        application_path = os.getcwd()  # Use current working directory as fallback


def _sanitize_filename(name: str) -> str:
    """Sanitizes a string to be used as a valid filename."""
    if not name:
        return ""
    # Replace invalid filename characters with an underscore
    return re.sub(r'[<>:"/\\|?*]', '_', name)


def save_dashboard_oid_map(dashboard_oid_map, settings, logger: logging.Logger):
    """
    Saves the dashboard OID map to a JSON file, prefixed with the target server host.

    Args:
        dashboard_oid_map (dict): A dictionary mapping dashboard OIDs.
        settings (dict): The application settings.
        logger (logging.Logger): The logger instance.
    """
    try:
        target_host = settings.get('target_host')
        if not target_host:
            logger.error("Cannot save dashboard OID map: 'target_host' is not defined in settings.")
            return

        safe_host_name = _sanitize_filename(target_host)
        filename = f"{safe_host_name}_dashboard_oid_map.json"

        dashboard_oid_map_file_path = get_user_config_path(filename, APP_NAME)

        # Save the dashboard OID map to the file
        with open(dashboard_oid_map_file_path, "w") as f:
            json.dump(dashboard_oid_map, f, indent=4)

        logger.info(f"Dashboard OID map saved to {dashboard_oid_map_file_path}")

    except Exception as e:
        logger.error(f"Error saving dashboard OID map: {e}")


def load_dashboard_oid_map(settings, logger: logging.Logger):
    """
    Loads the dashboard OID map from a JSON file, prefixed with the target server host.

    Args:
        settings (dict): The application settings.
        logger (logging.Logger): The logger instance.

    Returns:
        dict: The dashboard OID map.
    """
    try:
        target_host = settings.get('target_host')
        if not target_host:
            logger.warning("Cannot load dashboard OID map: 'target_host' is not defined. Returning empty map.")
            return {}

        safe_host_name = _sanitize_filename(target_host)
        filename = f"{safe_host_name}_dashboard_oid_map.json"

        dashboard_oid_map_file_path = get_user_config_path(filename, APP_NAME)

        # Load the dashboard OID map from the file
        with open(dashboard_oid_map_file_path, "r") as f:
            dashboard_oid_map = json.load(f)

        logger.info(f"Dashboard OID map loaded from {dashboard_oid_map_file_path}")

        return dashboard_oid_map

    except FileNotFoundError:
        logger.warning(f"Dashboard OID map file '{filename}' not found. A new one will be created.")
        return {}

    except Exception as e:
        logger.error(f"Error loading dashboard OID map from '{filename}': {e}")
        return {}


def get_target_dashboards_owned_by_migration_user(target_api, logger: logging.Logger):
    """
    Gets the dashboards owned by the migration user on the target server.

    Args:
        target_api: The target Sisense API client.
        logger (logging.Logger): The logger instance.

    Returns:
        list: A list of dashboards owned by the migration user.
    """
    try:
        # Get the migration user
        migration_user = target_api.get_my_user()

        # Get the dashboards owned by the migration user
        dashboards = target_api.get_dashboards_admin({"owner": migration_user["_id"]})

        logger.info(f"Found {len(dashboards)} dashboards owned by the migration user on the target server.")

        return dashboards

    except Exception as e:
        logger.error(f"Error getting dashboards owned by the migration user: {e}")
        return []