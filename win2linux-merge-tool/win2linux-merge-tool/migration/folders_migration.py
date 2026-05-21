# /migration/folders_migration.py

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


def save_folders_map(folders_map, settings, logger: logging.Logger, backup=True):
    """
    Saves the folders map to a JSON file, prefixed with the target server host.

    Args:
        folders_map (list): A list of folder mappings.
        settings (dict): The application settings.
        logger (logging.Logger): The logger instance.
        backup (bool): Whether to create a backup of the existing folders map file.
    """
    try:
        target_host = settings.get('target_host')
        if not target_host:
            logger.error("Cannot save folders map: 'target_host' is not defined in settings.")
            return None

        safe_host_name = _sanitize_filename(target_host)
        filename = f"{safe_host_name}_folders_map.json"

        folders_map_file_path = get_user_config_path(filename, APP_NAME)

        # Create a backup of the existing folders map file
        if backup and os.path.exists(folders_map_file_path):
            backup_file_path = f"{folders_map_file_path}.bak"
            os.rename(folders_map_file_path, backup_file_path)
            logger.info(f"Folders map file backed up to {backup_file_path}")

        # Save the folders map to the file
        with open(folders_map_file_path, "w") as f:
            json.dump(folders_map, f, indent=4)

        logger.info(f"Folders map saved to {folders_map_file_path}")

        return folders_map_file_path

    except Exception as e:
        logger.error(f"Error saving folders map: {e}")
        return None # Explicitly return None on error


def load_folders_map(settings, logger: logging.Logger):
    """
    Loads the folders map from a JSON file, prefixed with the target server host.

    Args:
        settings (dict): The application settings.
        logger (logging.Logger): The logger instance.

    Returns:
        list: The folders map.
    """
    try:
        target_host = settings.get('target_host')
        if not target_host:
            logger.warning("Cannot load folders map: 'target_host' is not defined. Returning empty map.")
            return []

        safe_host_name = _sanitize_filename(target_host)
        filename = f"{safe_host_name}_folders_map.json"

        folders_map_file_path = get_user_config_path(filename, APP_NAME)

        # Load the folders map from the file
        with open(folders_map_file_path, "r") as f:
            folders_map = json.load(f)

        logger.info(f"Folders map loaded from {folders_map_file_path}")

        return folders_map

    except FileNotFoundError:
        logger.warning(f"Folders map file '{filename}' not found. A new one will be created on the next run.")
        return []

    except Exception as e:
        logger.error(f"Error loading folders map from '{filename}': {e}")
        return []