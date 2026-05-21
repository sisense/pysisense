# utils/utils.py

import os
import sys
import yaml
from config_loader import APP_NAME, SERVERS_FILE, SETTINGS_FILE
import logging

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # Not running as a bundled app, use the normal path
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


def get_user_config_path(filename, app_name):
    """
    Returns a platform-specific path to a configuration file.
    - Windows: %LOCALAPPDATA%\\<app_name>
    - macOS:   ~/Library/Application Support/<app_name>
    - Linux:   ~/.config/<app_name>
    Creates the directory if it doesn't exist.
    """
    if sys.platform == "win32":
        # Use %LOCALAPPDATA% on Windows
        base_dir = os.environ.get('LOCALAPPDATA')
        if not base_dir: # Fallback if LOCALAPPDATA is not set
            base_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local')
        config_dir = os.path.join(base_dir, app_name)
    elif sys.platform == "darwin":
        # Use ~/Library/Application Support on macOS
        base_dir = os.path.expanduser('~/Library/Application Support')
        config_dir = os.path.join(base_dir, app_name)
    else:
        # Use ~/.config on Linux and other platforms
        base_dir = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
        config_dir = os.path.join(base_dir, app_name)

    # Ensure the directory exists
    try:
        os.makedirs(config_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating config directory at {config_dir}: {e}")
        # Fallback to a local directory if user home is not writable
        config_dir = os.path.join(os.getcwd(), 'config')
        os.makedirs(config_dir, exist_ok=True)

    return os.path.join(config_dir, filename)


def get_reports_path(relative_path, app_name=APP_NAME):
    """
    Returns a platform-specific path to a report file in a writable reports directory.
    Use this instead of hardcoded 'reports/...' paths to avoid read-only filesystem errors
    when running from bundled apps (e.g. macOS .app) where CWD may be read-only.
    - Windows: %LOCALAPPDATA%\\<app_name>\\reports\\
    - macOS:   ~/Library/Application Support/<app_name>/reports/
    - Linux:   ~/.config/<app_name>/reports/
    """
    # Strip leading 'reports/' if present
    filename = relative_path.replace("reports/", "").replace("reports\\", "")
    # Try app config directory first
    try:
        reports_dir = get_user_config_path("reports", app_name)
        base_dir = os.path.dirname(reports_dir)
        reports_base = os.path.join(base_dir, "reports")
        os.makedirs(reports_base, exist_ok=True)
        return os.path.join(reports_base, filename)
    except OSError as e:
        print(f"Error using reports directory: {e}")
    # Fallback: ~/reports (always writable on macOS/Linux/Windows)
    fallback_base = os.path.join(os.path.expanduser("~"), "reports")
    try:
        os.makedirs(fallback_base, exist_ok=True)
        return os.path.join(fallback_base, filename)
    except OSError as e2:
        print(f"Error using fallback reports directory at {fallback_base}: {e2}")
        raise


def load_yaml_config(file_path, app_name):
    """
    Loads a YAML configuration file.
    If file_path is a relative path, it's resolved against the user's config directory.
    """
    # If the path is not absolute, resolve it against the user config path
    if not os.path.isabs(file_path):
        file_path = get_user_config_path(file_path, app_name)

    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Configuration file not found: {file_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file {file_path}: {e}")
        return None

# --- FIX: Removed the flawed function ---
# def remove_duplicate_objects_from_json(input_file, output_file): ...


def fetch_all_dashboards(api_client, logger: logging.Logger, chunk_size=100):
    """
    Fetches all dashboards from the Sisense API with fallback strategy.
    First tries the paginated search_get_all_dashboards method.
    If it encounters a 500 error, falls back to get_dashboards_admin method.

    Args:
        api_client: An instance of the Sisense REST API client.
        logger: A logger instance for logging messages.
        chunk_size (int): The number of dashboards to fetch per API call.

    Returns:
        A list of all dashboard objects.
    """
    logger.info(f"Starting to fetch all dashboards with a chunk size of {chunk_size}.")
    
    # First, try the original paginated approach
    try:
        return _fetch_dashboards_paginated(api_client, logger, chunk_size)
    except Exception as e:
        # Check if it's a 500 error or similar server error
        error_str = str(e).lower()
        if '500' in error_str or 'internal server error' in error_str or 'server error' in error_str:
            logger.warning(f"Received server error with paginated approach: {e}")
            logger.info("Falling back to get_dashboards_admin method...")
            return _fetch_dashboards_admin_fallback(api_client, logger)
        else:
            # Re-raise if it's not a server error
            logger.error(f"Non-server error occurred: {e}")
            raise


def _fetch_dashboards_paginated(api_client, logger: logging.Logger, chunk_size=100):
    """
    Original paginated approach using search_get_all_dashboards.
    """
    all_dashboards = []
    skip = 0
    total_fetched = 0

    while True:
        try:
            logger.debug(f"Fetching dashboard chunk: skip={skip}, limit={chunk_size}")
            response = api_client.search_get_all_dashboards(limit=chunk_size, skip=skip)

            if not response or 'items' not in response:
                logger.warning(f"Received an invalid or empty response from the API at skip={skip}.")
                break

            chunk = response['items']
            if not chunk:
                logger.info("No more dashboards to fetch. Concluding fetch process.")
                break  # Exit loop if the last chunk was empty

            all_dashboards.extend(chunk)
            total_fetched += len(chunk)
            logger.info(f"Fetched {len(chunk)} dashboards. Total fetched so far: {total_fetched}.")

            # Prepare for the next iteration
            skip += chunk_size

        except Exception as e:
            logger.error(f"An error occurred while fetching dashboards at skip={skip}: {e}")
            raise  # Re-raise to trigger fallback

    logger.info(f"Finished fetching dashboards with paginated approach. Total dashboards retrieved: {len(all_dashboards)}.")
    return all_dashboards


def _fetch_dashboards_admin_fallback(api_client, logger: logging.Logger):
    """
    Fallback approach using get_dashboards_admin method (MIT version).
    """
    all_dashboards = []
    total_fetched = 0

    try:
        logger.debug(">>>> Fetching dashboard chunk using get_dashboards_admin fallback")
        response = api_client.get_dashboards_admin("dashboardType=owner")
        logger.debug(f"Response from get_dashboards_admin: {response}")
        
        if not response:
            logger.warning("Received an invalid or empty response from the fallback API.")
            return all_dashboards

        if not response:
            logger.info("No dashboards found with fallback method.")
            return all_dashboards

        all_dashboards.extend(response)
        total_fetched += len(response)
        logger.info(f"Fetched {len(response)} dashboards using fallback method. Total fetched: {total_fetched}.")

    except Exception as e:
        logger.error(f"An error occurred while fetching dashboards with fallback method: {e}")
        logger.error("Both primary and fallback methods failed.")
        raise

    logger.info(f"Finished fetching dashboards with fallback approach. Total dashboards retrieved: {len(all_dashboards)}.")
    return all_dashboards