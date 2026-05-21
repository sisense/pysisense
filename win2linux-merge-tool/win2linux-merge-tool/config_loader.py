# config_loader.py

import argparse
import yaml
import os
import sys

# --- Global configuration variables ---
# These will be populated by load_config()
args = None
settings = None


# --- Configuration Constants ---
# Define paths relative to the project root (where the bundled app/script runs)
# These are used by resource_path() in utils.py
SETTINGS_FILE = 'settings.yaml'
SERVERS_FILE = 'servers.yaml'
APP_NAME = "sisense_merge_and_migration"

if getattr(sys, 'frozen', False):
    # --- Case 1: Application is frozen ---
    application_path = os.path.dirname(sys.executable)
else:
    # --- Case 2: Application is a standard script ---
    try:

        application_path = os.path.dirname(os.path.abspath(__file__))
    except NameError:

        application_path = os.getcwd() # Use current working directory as fallback



# --- Argument Parser Setup ---
parser = argparse.ArgumentParser(description="Load configuration for the migration tool.")
# Ensure this line uses '--settings-file' for the command-line flag
parser.add_argument(
    '--settings-file',  # Use hyphen for command-line
    dest='settings_file', # This ensures args.settings_file in Python (argparse does this by default if dest isn't set)
    type=str,
    # required=True, # Consider if this should be required
    # default='settings.yaml', # Or provide a default if suitable
    help='Path to the settings YAML file. Example: --settings-file settings.template.yaml'
)
# Add the new flag here so the parser doesn't fail when the worker process is called.
# argparse.SUPPRESS hides it from the help message, as it's for internal use.
parser.add_argument('--run-migration', action='store_true', help=argparse.SUPPRESS)


# You can add other command-line arguments here if needed.
# For example, to override specific settings from the YAML file:
# parser.add_argument('--src_host', type=str, help='Override the source host from the settings file.')


def load_config():
    """
    Parses command-line arguments and loads settings from the specified YAML file.
    Populates the global 'args' and 'settings' variables.
    """
    global args, settings, application_path

    current_cli_args = parser.parse_args()
    args = current_cli_args  # Store parsed CLI arguments globally

    settings_file_name = current_cli_args.settings_file
    resolved_settings_file_path = None

    # 1. Try path relative to this config.py script
    path_near_script = os.path.join(application_path, settings_file_name)
    if os.path.isabs(settings_file_name) and os.path.exists(settings_file_name):
        resolved_settings_file_path = settings_file_name
    elif os.path.exists(path_near_script):
        resolved_settings_file_path = path_near_script
    else:
        # 2. Try path relative to current working directory
        path_in_cwd = os.path.join(os.getcwd(), settings_file_name)
        if os.path.exists(path_in_cwd):
            resolved_settings_file_path = path_in_cwd
        else:
            print(f"Error: Settings file '{settings_file_name}' not found.", file=sys.stderr)
            print(f"  Checked: '{path_near_script}' (relative to script location)", file=sys.stderr)
            print(f"  Checked: '{path_in_cwd}' (relative to CWD)", file=sys.stderr)
            if os.path.isabs(settings_file_name):
                 print(f"  Checked: '{settings_file_name}' (as absolute path)", file=sys.stderr)
            sys.exit(1)

    try:
        with open(resolved_settings_file_path, 'r') as f:
            loaded_yaml = yaml.safe_load(f)
            if not isinstance(loaded_yaml, dict):
                print(f"Error: Content of '{resolved_settings_file_path}' is not a valid YAML dictionary.", file=sys.stderr)
                sys.exit(1)
            settings = loaded_yaml
        print(f"INFO: Settings successfully loaded from: {resolved_settings_file_path}", file=sys.stderr)
    except yaml.YAMLError as e:
        print(f"Error: Could not parse YAML settings file '{resolved_settings_file_path}'.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: An unexpected error occurred while loading settings from '{resolved_settings_file_path}'.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        sys.exit(1)

    # Optional: Override settings from YAML with any CLI arguments if defined for that purpose.
    # Example:
    # if hasattr(args, 'src_host') and args.src_host:
    #     settings['src_host'] = args.src_host
    #     print(f"INFO: Overrode 'src_host' with CLI argument: {args.src_host}")


def coerce_types(cfg_settings):
    int_keys = [
        'dashboard_migration_concurrency',
        'dashboard_share_concurrency',
        'wait_between_chunks',
        'wait_chunk_size_threshold',
        'migrate_datasecurity_chunk_size',
        'migrate_users_chunk_size',
        'dashboard_fetch_chunk_size',
    ]
    for key in int_keys:
        if key in cfg_settings and isinstance(cfg_settings[key], str):
            try:
                cfg_settings[key] = int(cfg_settings[key])
            except ValueError:
                pass  # Leave as is if conversion fails
    # Add more coercion as needed for other types


def settings_validation(cfg_settings):
    """
    Validates the loaded application settings.
    Prints errors and calls sys.exit(1) if validation fails.
    """
    coerce_types(cfg_settings)
    print("INFO: Starting settings validation...", file=sys.stderr)

    # Define required keys and their types
    required_keys = {
        'selected_source_server': (str, type(None)),
        'selected_target_server': (str, type(None)),
        'migrate_users': bool,
        'migrate_groups': bool,
        'migrate_folders': bool,
        'migrate_dashboards': bool,
        'migrate_datamodels': bool,
        'migrate_datasecurity': bool,
        'folders': dict,
        'dashboard_share_with_migration_user': bool,
        'dashboard_share_concurrency': int,
        'dashboard_migration_mode': str,
        'dashboard_include_list': (str, list),
        'exclude_dashboards_by_name': str,
        'exclude_dashboards_by_oid': str,
        'skip_dashboards_with_missing_ancestor_folder': bool,
        'file_logLevel': str,
        'console_logLevel': str,
        'logFileName': str,
        'override_logs': bool,
        'dashboard_import_mode': str,
        'dashboard_migration_concurrency': int,
        'wait_between_chunks': int,
        'wait_chunk_size_threshold': int,
        'verify': bool,
        'dashboard_fetch_chunk_size': int,
    }

    optional_keys = {
        'datamodel_overwrite': bool,
        'enable_update_connections': bool,
        'auto_migrate_missing_custom_code_notebooks': bool,
        'exact_match_in_dashboard_search': bool,
        'exclude_datamodels': (str, list),
        'include_datamodels': (str, list),
        'include_datamodels_datasecurity': (str, list),
        'exclude_datamodels_datasecurity': (str, list),
        'migrate_blox_actions': bool,
        'migrate_custom_code': bool,
        'notebook_import_mode': str,
        'notebook_include_list': (str, list),
        'migrate_datasecurity_chunk_size': int,
        'migrate_users_chunk_size': int,
        'new_password_for_migrated_users': (str, type(None)),
        'oid_host_mapping': dict,
        'overwrite_existing_blox_actions': bool,
        'post_import_update_connection_function': str,
        'skip_dashboards_with_missing_owner': bool,
        'update_connections': list,
        'update_users_password': bool,
        'ignore_custom_roles': bool,
        'use_custom_dashboard_oid': bool,
        'validate_dashboards_migration': bool,
        'migration_log_levels': dict,
        'write_migration_reports': bool,
    }

    # Validate migration_log_levels if present
    MIGRATION_LOG_LEVEL_ITEMS = {'groups', 'users', 'preflight', 'folders', 'datamodels', 'datasecurity', 'dashboards', 'blox', 'custom_code'}
    VALID_LOG_LEVELS = {'debug', 'info', 'warning', 'error', 'critical', 'inherit', 'default', ''}
    if 'migration_log_levels' in cfg_settings:
        mll = cfg_settings['migration_log_levels']
        if not isinstance(mll, dict):
            print("Error: 'migration_log_levels' must be a dictionary.", file=sys.stderr)
            sys.exit(1)
        for key, val in mll.items():
            if key not in MIGRATION_LOG_LEVEL_ITEMS:
                print(f"Warning: Unknown migration item '{key}' in migration_log_levels (ignored).", file=sys.stderr)
                continue
            val_normalized = str(val).strip().lower() if val is not None else ''
            # Treat boolean True/False or 'true'/'false' as inherit
            if val is True or val_normalized == 'true':
                val_normalized = 'inherit'
            elif val is False or val_normalized == 'false':
                val_normalized = 'inherit'
            if val_normalized not in VALID_LOG_LEVELS:
                print(f"Error: migration_log_levels['{key}'] must be DEBUG, INFO, WARNING, ERROR, CRITICAL, or inherit (got {val!r}).", file=sys.stderr)
                sys.exit(1)
            if val_normalized in ('inherit', 'default', ''):
                cfg_settings['migration_log_levels'][key] = 'inherit'
            else:
                cfg_settings['migration_log_levels'][key] = val_normalized

    # Check for missing required keys
    for key, expected_type in required_keys.items():
        if key not in cfg_settings:
            print(f"Error: Missing required setting '{key}' in the settings file.", file=sys.stderr)
            sys.exit(1)
        if not isinstance(cfg_settings[key], expected_type):
            print(f"Error: Setting '{key}' should be of type {expected_type}, got {type(cfg_settings[key])}.", file=sys.stderr)
            sys.exit(1)

    # Value checks for required keys
    if not (1 <= cfg_settings['dashboard_share_concurrency'] <= 20):
        print("Error: 'dashboard_share_concurrency' must be between 1 and 20.", file=sys.stderr)
        sys.exit(1)
    # Normalize dashboard_migration_mode (accept "All", "all", "by name", etc.)
    _dmm = (cfg_settings['dashboard_migration_mode'] or '').strip()
    if _dmm.upper() == 'ALL':
        cfg_settings['dashboard_migration_mode'] = 'ALL'
    elif _dmm.lower() in ('by name', 'byname'):
        cfg_settings['dashboard_migration_mode'] = 'By Name'
    elif _dmm.lower() in ('by oid', 'byoid'):
        cfg_settings['dashboard_migration_mode'] = 'By OID'
    if cfg_settings['dashboard_migration_mode'] not in ['ALL', 'By Name', 'By OID']:
        print("Error: 'dashboard_migration_mode' must be 'ALL', 'By Name', or 'By OID'.", file=sys.stderr)
        sys.exit(1)
    if cfg_settings['dashboard_import_mode'] not in ['skip', 'overwrite']:
        print("Error: 'dashboard_import_mode' must be 'skip' or 'overwrite'.", file=sys.stderr)
        sys.exit(1)
    if 'notebook_import_mode' in cfg_settings and cfg_settings['notebook_import_mode'] not in ['skip', 'overwrite', 'duplicate']:
        print("Error: 'notebook_import_mode' must be 'skip', 'overwrite', or 'duplicate'.", file=sys.stderr)
        sys.exit(1)
    for log_key in ['file_logLevel', 'console_logLevel']:
        if cfg_settings[log_key] not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            print(f"Error: '{log_key}' must be a valid logging level.", file=sys.stderr)
            sys.exit(1)

    # Nested folders validation
    if 'folders' in cfg_settings:
        folders = cfg_settings['folders']
        if not isinstance(folders, dict):
            print("Error: 'folders' must be a dictionary.", file=sys.stderr)
            sys.exit(1)
        for subkey in ['share_source_dashboards_with_migration_user', 'update_target_folders_owner']:
            if subkey not in folders:
                print(f"Error: Missing '{subkey}' in 'folders' section.", file=sys.stderr)
                sys.exit(1)
            if not isinstance(folders[subkey], bool):
                print(f"Error: 'folders.{subkey}' must be a boolean.", file=sys.stderr)
                sys.exit(1)

    # Check optional keys if present
    for key, expected_type in optional_keys.items():
        if key in cfg_settings and not isinstance(cfg_settings[key], expected_type):
            print(f"Error: Optional setting '{key}' should be of type {expected_type}, got {type(cfg_settings[key])}.", file=sys.stderr)
            sys.exit(1)

    # Warn on unknown keys
    known_keys = set(required_keys) | set(optional_keys)
    unknown_keys = set(cfg_settings) - known_keys
    if unknown_keys:
        print(f"Warning: Unknown keys found in settings: {', '.join(sorted(unknown_keys))}", file=sys.stderr)

    print("INFO: Settings validation passed.", file=sys.stderr)