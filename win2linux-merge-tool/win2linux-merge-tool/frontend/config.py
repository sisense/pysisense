# frontend/config.py

import os

BACKUP_SUFFIX = ".bak"
BACKUP_ARCHIVE_PREFIX = "settings_backups_"
MAX_BACKUP_FILES_BEFORE_ARCHIVE = 10

APP_TITLE = "Sisense Migration & Merge Settings"
WINDOW_WIDTH = 850
WINDOW_HEIGHT = 700
HELP_ICON_TEXT = "ⓘ"

# --- Key Mappings and Orders ---
MIGRATION_CONTROL_KEYS = [
    'migrate_users',
    'migrate_groups',
    'migrate_folders',
    'migrate_dashboards',
    'migrate_datamodels',
    'migrate_datasecurity'
]
MIGRATION_CONTROL_KEYS_SET = set(MIGRATION_CONTROL_KEYS)

TAB_CONTROL_MAP = {
    'migrate_users': 'Users & Groups',
    'migrate_groups': 'Users & Groups',
    'migrate_folders': 'Folders',
    'migrate_dashboards': 'Dashboards',
    'migrate_datamodels': 'Datamodels',
    'migrate_datasecurity': 'Data Security'
}

FOLDER_SUB_KEYS = [
    'share_source_dashboards_with_migration_user',
    'update_target_folders_owner'
]
FOLDER_SUB_KEYS_SET = set(FOLDER_SUB_KEYS)

DASHBOARD_SHARE_KEY = 'dashboard_share_with_migration_user'  # Now in Preflight

SELECTED_SOURCE_SERVER_KEY = 'selected_source_server'
SELECTED_TARGET_SERVER_KEY = 'selected_target_server'
SERVER_DETAIL_KEYS = ['host', 'port', 'protocol', 'os', 'api_token', 'allowed_roles']

DASHBOARD_MIGRATION_MODE_KEY = 'dashboard_migration_mode'
DASHBOARD_INCLUDE_LIST_KEY = 'dashboard_include_list'
EXCLUDE_DASHBOARDS_BY_NAME_KEY = 'exclude_dashboards_by_name'
EXCLUDE_DASHBOARDS_BY_OID_KEY = 'exclude_dashboards_by_oid'
SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY = 'skip_dashboards_with_missing_ancestor_folder'  # New Key
DASHBOARD_MIGRATION_MODES = ['ALL', 'By Name', 'By OID']
DASHBOARD_IMPORT_MODES = ['skip', 'overwrite']
NOTEBOOK_IMPORT_MODES = ['skip', 'overwrite']

CATEGORY_MAP = {
    # Connection
    SELECTED_SOURCE_SERVER_KEY: 'Connection',
    SELECTED_TARGET_SERVER_KEY: 'Connection',
    'verify': 'Connection',
    # Logging
    'file_logLevel': 'Logging',
    'console_logLevel': 'Logging',
    'logFileName': 'Logging',
    'override_logs': 'Logging',
    'write_migration_reports': 'Logging',
    'migration_log_levels': 'Logging',
    # Dashboards
    'dashboard_import_mode': 'Dashboards',
    DASHBOARD_MIGRATION_MODE_KEY: 'Dashboards',
    DASHBOARD_INCLUDE_LIST_KEY: 'Dashboards',
    EXCLUDE_DASHBOARDS_BY_NAME_KEY: 'Dashboards',
    EXCLUDE_DASHBOARDS_BY_OID_KEY: 'Dashboards',
    'use_custom_dashboard_oid': 'Dashboards',
    'oid_host_mapping': 'Dashboards',
    'exact_match_in_dashboard_search': 'Dashboards',
    'validate_dashboards_migration': 'Dashboards',
    'skip_dashboards_with_missing_owner': 'Dashboards',
    SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY: 'Dashboards',  # New Key Added
    'dashboard_migration_concurrency': 'Dashboards',
    'dashboard_fetch_chunk_size': 'Dashboards',
    # Datamodels
    'datamodel_overwrite': 'Datamodels',
    'exclude_datamodels': 'Datamodels',
    'include_datamodels': 'Datamodels',
    'enable_update_connections': 'Datamodels',
    'update_connections': 'Datamodels',
    'post_import_update_connection_function': 'Datamodels',
    'auto_migrate_missing_custom_code_notebooks': 'Datamodels',
    'notebook_import_mode': 'Datamodels',
    'migrate_saved_formulas': 'Datamodels',
    # Users & Groups
    'update_users_password': 'Users & Groups',
    'new_password_for_migrated_users': 'Users & Groups',
    'migrate_users_chunk_size': 'Users & Groups',
    'wait_between_chunks': 'Users & Groups',
    'wait_chunk_size_threshold': 'Users & Groups',
    # Data Security
    'migrate_datasecurity_chunk_size': 'Data Security',
    'include_datamodels_datasecurity': 'Data Security',
    'exclude_datamodels_datasecurity': 'Data Security',
    # Folders
    'share_source_dashboards_with_migration_user': 'Folders',
    'update_target_folders_owner': 'Folders',
    # Blox
    'migrate_blox_actions': 'Blox',
    'overwrite_existing_blox_actions': 'Blox',
    # Custom Code
    'migrate_custom_code': 'Custom Code',
    'notebook_import_mode': 'Custom Code',
    'notebook_include_list': 'Custom Code',
    # Preflight
    DASHBOARD_SHARE_KEY: 'Preflight',
    'dashboard_share_concurrency': 'Preflight',
    'ignore_custom_roles': 'Preflight',
    # Migration Plan
    'migrate_users': 'Migration Plan',
    'migrate_groups': 'Migration Plan',
    'migrate_folders': 'Migration Plan',
    'migrate_dashboards': 'Migration Plan',
    'migrate_datamodels': 'Migration Plan',
    'migrate_datasecurity': 'Migration Plan',
    'migrate_custom_code': 'Migration Plan',
}

DASHBOARD_FIELD_ORDER = [
    'dashboard_import_mode',
    DASHBOARD_MIGRATION_MODE_KEY,
    DASHBOARD_INCLUDE_LIST_KEY,
    EXCLUDE_DASHBOARDS_BY_NAME_KEY,
    EXCLUDE_DASHBOARDS_BY_OID_KEY,
    'exact_match_in_dashboard_search',
    'use_custom_dashboard_oid',
    'oid_host_mapping',
    'skip_dashboards_with_missing_owner',
    SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY,  # New Key Added
    'validate_dashboards_migration',
    'dashboard_migration_concurrency',
    'dashboard_fetch_chunk_size',
]

PREFLIGHT_FIELD_ORDER = [
    DASHBOARD_SHARE_KEY,
    'dashboard_share_concurrency',
    'ignore_custom_roles',
]

LOGGING_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# Migration items that can have per-item log levels
MIGRATION_LOG_LEVEL_ITEMS = [
    ('groups', 'Groups'),
    ('users', 'Users'),
    ('preflight', 'Preflight'),
    ('folders', 'Folders'),
    ('datamodels', 'Datamodels'),
    ('datasecurity', 'Data Security'),
    ('dashboards', 'Dashboards'),
    ('blox', 'Blox'),
    ('custom_code', 'Custom Code'),
]

TAB_ORDER = [
    'Migration Plan',
    'Connection',
    'Logging',
    'Dashboards',
    'Datamodels',
    'Users & Groups',
    'Data Security',
    'Folders',
    'Blox',
    'Custom Code',
    'Preflight',
]

COMMA_STRING_KEYS = [
    'include_datamodels',
    'exclude_datamodels',
    'include_datamodels_datasecurity',
    'exclude_datamodels_datasecurity',
    DASHBOARD_INCLUDE_LIST_KEY,
    EXCLUDE_DASHBOARDS_BY_NAME_KEY,
    EXCLUDE_DASHBOARDS_BY_OID_KEY,
    'notebook_include_list',
]

KEY_VALUE_EDITOR_KEYS = [
    'oid_host_mapping',
]

LIST_OF_DICTS_EDITOR_KEYS = {
    'update_connections': {'provider': 'Provider', 'function': 'Function'},
}

# --- Help Texts ---
HELP_TEXTS = {
    'store_bearer_tokens': "If checked, the application will attempt to save the bearer tokens obtained after successful login to a local file.",
    'bearer_file_storage_file_name': "The name of the file used to store bearer tokens (if 'store_bearer_tokens' is enabled). Stored in the 'dist' directory.",
    'use_stored_bearer_tokens': "If checked, the application will try to load and use previously stored bearer tokens instead of prompting for login credentials.",
    'verify': "Verify SSL certificates for API requests. Should generally be kept enabled unless you have specific reasons (e.g., self-signed certificates) and understand the security implications.",
    'mssql_connection': "ODBC connection string for MS SQL Server, used for specific data source updates if required by 'post_import_update_connection_function'. Example: DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=your_user;PWD=your_password",
    'file_logLevel': "Set the logging level for the log file (e.g., 'DEBUG', 'INFO', 'WARNING').",
    'console_logLevel': "Set the logging level for the console output (e.g., 'DEBUG', 'INFO', 'WARNING').",
    'logFileName': "Name of the log file to be created in the 'dist/logs' directory.",
    'override_logs': "If checked, existing log files with the same name will be overwritten on each run. Otherwise, new logs will be appended.",
    'dashboard_import_mode': "'skip': Do not import dashboards that already exist on the target. 'overwrite': Replace existing dashboards on the target with the ones from the source.",
    DASHBOARD_MIGRATION_MODE_KEY: "'ALL': Migrate all accessible dashboards. 'By Name': Migrate only the dashboards listed in 'dashboard_include_list' by name. 'By OID': Migrate only the dashboards listed in 'dashboard_include_list' by OID.",
    DASHBOARD_INCLUDE_LIST_KEY: "List of dashboard names (when 'By Name') or OIDs (when 'By OID'), one per line, to include when 'dashboard_migration_mode' is 'By Name' or 'By OID'.",
    EXCLUDE_DASHBOARDS_BY_NAME_KEY: "List of dashboard names (one per line) to explicitly exclude from migration.",
    EXCLUDE_DASHBOARDS_BY_OID_KEY: "List of dashboard OIDs (Object IDs) (one per line) to explicitly exclude from migration.",
    'use_custom_dashboard_oid': "Enable mapping of dashboard OIDs based on the source host. Requires 'oid_host_mapping' to be configured.",
    'oid_host_mapping': "Map source hostnames/IPs to target OID prefixes. Used when 'use_custom_dashboard_oid' is enabled to generate predictable OIDs on the target.",
    'exact_match_in_dashboard_search': "If checked, dashboard search (for include/exclude lists) will require an exact name match. Otherwise, it performs a substring match.",
    'validate_dashboards_migration': "Perform post-migration checks to verify dashboard structure and data sources (can increase migration time).",
    'skip_dashboards_with_missing_owner': "If checked, dashboards whose owners do not exist on the target system will be skipped during migration.",
    SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY: "If checked, dashboards whose parent folder (or any ancestor folder) is missing on the target system will be skipped during migration. This helps prevent dashboards from being migrated to the root if their folder structure cannot be replicated.",
    # New Help Text
    DASHBOARD_SHARE_KEY: "During folder migration, share dashboards from the *source* system with the migration user *on the source* (useful for ensuring the migration user can access folders and dashboards to migrate them).",
    'dashboard_migration_concurrency': "Number of dashboards to process concurrently during migration. Higher values may speed up migration but increase load.",
    'datamodel_overwrite': "If checked, existing datamodels on the target server will be overwritten by those from the source.",
    'exclude_datamodels': "List of datamodel titles (one per line) to exclude from migration.",
    'include_datamodels': "List of datamodel titles (one per line) to include in the migration (if empty, all non-excluded datamodels are considered).",
    'enable_update_connections': "Enable the modification of connection details within migrated datamodels based on the 'update_connections' mapping.",
    'update_connections': "Define rules for updating connection details in datamodels. Map 'Provider' (e.g., 'CSV', 'SQLServer') to a 'Function' name defined in 'post_import_update_connection_function'.",
    'post_import_update_connection_function': "Path to a Python file containing custom functions used for updating datamodel connections (referenced in 'update_connections').",
    'auto_migrate_missing_custom_code_notebooks': "If checked, when migrating a datamodel that requires custom code notebooks that are missing on the target server, the tool will automatically migrate those notebooks before continuing with the datamodel migration.",
    'update_users_password': "If checked, the passwords of migrated users will be updated to the value specified in 'new_password_for_migrated_users'.",
    'new_password_for_migrated_users': "The new password to set for users if 'update_users_password' is enabled.",
    'migrate_users_chunk_size': "Number of users to process in each batch during migration.",
    'ignore_custom_roles': "If checked, the 'custom_' prefix will be stripped from role names when matching roles between source and target systems. For example, a source role 'custom_admin' will match a target role 'admin' (or 'custom_admin'). This is useful when source systems use custom roles but target systems use standard roles with the same base name.",
    'migrate_datasecurity_chunk_size': "Number of data security rules to process in each batch.",
    'include_datamodels_datasecurity': "List of datamodel titles (one per line) to include for data security migration (if empty, all non-excluded datamodels are considered).",
    'exclude_datamodels_datasecurity': "List of datamodel titles (one per line) to exclude from data security migration.",
    'share_source_dashboards_with_migration_user': "During folder migration, share dashboards from the *source* system with the migration user *on the source* (useful for ensuring the migration user can access folders and dashboards to migrate them).",
    'update_target_folders_owner': "Update the owner of migrated folders on the target system to match the owner from the source system.",
    'migrate_blox_actions': "Enable the migration of BloX actions.",
    'overwrite_existing_blox_actions': "If checked, existing BloX actions on the target with the same name will be overwritten.",
    'migrate_custom_code': "Enable the migration of custom code (notebooks).",
    'notebook_import_mode': "'skip': Do not import notebooks that already exist on the target. 'overwrite': Replace existing notebooks on the target using PATCH.",
    'notebook_include_list': "List of notebook IDs or display names (one per line) to include in the migration. Leave empty or set to 'ALL' to migrate all notebooks.",
    'wait_between_chunks': "Time (in seconds) to wait between processing chunks (e.g., user chunks, dashboard chunks).",
    'wait_chunk_size_threshold': "Apply the 'wait_between_chunks' delay only if the number of items processed exceeds this threshold.",
    'migrate_users': "Master switch to enable or disable the migration of users.",
    'migrate_groups': "Master switch to enable or disable the migration of user groups.",
    'migrate_folders': "Master switch to enable or disable the migration of folders and their permissions.",
    'migrate_dashboards': "Master switch to enable or disable the migration of dashboards.",
    'migrate_datamodels': "Master switch to enable or disable the migration of datamodels.",
    'migrate_datasecurity': "Master switch to enable or disable the migration of data security rules.",
    'dashboard_share_concurrency': "Number of dashboards to process concurrently during the sharing step (if enabled). Value must be between 1 and 20.",
    'dashboard_fetch_chunk_size': "Number of dashboards to fetch per API call when retrieving dashboards from the target system. Lower values may reduce memory usage but increase API calls. Default: 100.",
    'migrate_saved_formulas': "If checked, saved formulas (measures) from the source datasource will be migrated to the target server.",
}

# --- Official Sisense Stylesheet (Refined v4) ---
# Define colors from Sisense Brand Guidelines 2024
SISENSE_DARK_BLUE = "#131F29"  # Retained for text where needed
SISENSE_WHITE = "#FFFFFF"
SISENSE_LIGHT_BLUE = "#1DE4EB"  # Secondary color for buttons now
SISENSE_LIGHT_GREEN = "#D7F77D"
SISENSE_RED = "#F05959"
SISENSE_LIGHT_GRAY = "#F2F4F4"  # Used for main background
SISENSE_LIGHTER_BLUE = "#94F5F0"
# Define derived/intermediate colors if needed
SISENSE_MEDIUM_GRAY = "#E0E0E0"
SISENSE_DARKER_GRAY = "#BDBDBD"
SISENSE_TEXT_DARK = "#333333"  # Dark gray for primary text
# Adjusted hover/pressed for light blue buttons
SISENSE_BUTTON_HOVER_BLUE = "#1AC9CF"
SISENSE_BUTTON_PRESSED_BLUE = "#17B5BB"

# Define Fonts
FONT_PRIMARY = "Poppins"  # For headings, buttons, titles
FONT_SECONDARY = "DM Sans"  # For body text, general labels

STYLESHEET = f"""
    /* Global Widget Styles */
    QWidget {{
        color: {SISENSE_TEXT_DARK}; /* Use dark gray text */
        font-family: "{FONT_SECONDARY}", Arial, sans-serif; /* Base font */
        font-size: 10pt;
    }}
    QMainWindow {{
        background-color: {SISENSE_LIGHT_GRAY};
    }}
    QDialog {{
        background-color: {SISENSE_LIGHT_GRAY};
    }}

    /* GroupBox Styles */
    QGroupBox {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_MEDIUM_GRAY};
        border-radius: 5px;
        margin-top: 15px;
        padding: 15px;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        subcontrol-position: top left;
        padding: 0 8px;
        color: {SISENSE_TEXT_DARK}; /* Dark gray title */
        left: 10px;
        font-family: "{FONT_PRIMARY}";

    }}

    /* Label Styles */
    QLabel {{
        background-color: transparent;
        padding: 2px;
        font-family: "{FONT_SECONDARY}";
    }}
    QWidget > QHBoxLayout > QLabel {{
         font-family: "{FONT_SECONDARY}";
         font-weight: normal;
    }}
    /* Style for Slider Value Label */
    QLabel#SliderValueLabel {{
        font-weight: bold;
        min-width: 30px; /* Ensure space for two digits + brackets */
        alignment: AlignCenter;
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 3px;
        padding: 2px;
        background-color: {SISENSE_WHITE};
    }}


    /* Button Styles - Using Secondary Color */
    QPushButton {{
        background-color: {SISENSE_LIGHTER_BLUE}; /* Lighter Blue */
        color: {SISENSE_TEXT_DARK}; /* Dark text for contrast */
        border: 1px solid {SISENSE_BUTTON_HOVER_BLUE};
        border-radius: 4px;
        padding: 4px 12px; /* Reduced top/bottom padding */
        min-height: 20px; /* Adjusted min height */
        font-family: "{FONT_PRIMARY}";
        font-weight: 600;
    }}
    QPushButton:hover {{
        background-color: {SISENSE_BUTTON_HOVER_BLUE};
        border: 1px solid {SISENSE_BUTTON_HOVER_BLUE};
    }}
    QPushButton:pressed {{
        background-color: {SISENSE_BUTTON_PRESSED_BLUE};
        border: 1px solid {SISENSE_BUTTON_PRESSED_BLUE};
    }}
    QPushButton:disabled {{
        background-color: {SISENSE_MEDIUM_GRAY};
        color: #999999;
        border: 1px solid {SISENSE_DARKER_GRAY};
    }}
    /* Style for flat help buttons */
    QPushButton[flat="true"] {{
        background-color: transparent;
        border: none;
        color: {SISENSE_TEXT_DARK};
        padding: 1px;
        font-weight: normal;
        min-height: 0px;
        font-family: "{FONT_SECONDARY}";
    }}
    QPushButton[flat="true"]:hover {{
        background-color: {SISENSE_MEDIUM_GRAY};
    }}
     QPushButton[flat="true"]:pressed {{
        background-color: {SISENSE_DARKER_GRAY};
    }}
    QPushButton:default {{
        border: 2px solid {SISENSE_LIGHT_GREEN}; /* Or another color that stands out */
        /* You might want to adjust padding slightly if the border makes the button too large */
        /* padding: 7px 15px; */ /* Example: 1px less padding due to 1px thicker border */
    }}

    /* Input Field Styles */
    QLineEdit, QTextEdit, QPlainTextEdit {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 4px;
        padding: 5px;
        color: {SISENSE_TEXT_DARK};
        font-family: "{FONT_SECONDARY}";
    }}
    QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus {{
        border: 1px solid {SISENSE_LIGHTER_BLUE}; /* Use accent blue for focus */
    }}
    QPlainTextEdit {{
        font-family: "Courier New", Courier, monospace;
    }}

    /* ComboBox Styles - REFINED v4 */
    QComboBox {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 4px;
        padding: 4px 8px;
        font-family: "{FONT_SECONDARY}";
        min-height: 20px;
        color: {SISENSE_TEXT_DARK};
    }}
    QComboBox::down-arrow {{ 

        image: url(assets/down-arrow.svg);


    }}
    QComboBox::drop-down {{
        subcontrol-origin: padding;
        subcontrol-position: top right;
        width: 30px;
        border-top-right-radius: 3px;
        border-bottom-right-radius: 3px;
        background-color: {SISENSE_WHITE};
    }}
    /*QComboBox {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 4px;
        padding: 4px 8px;
        font-family: "{FONT_SECONDARY}";
        min-height: 20px;
        color: {SISENSE_TEXT_DARK};
    }}
    QComboBox::drop-down {{
        subcontrol-origin: padding;
        subcontrol-position: top right;
        width: 20px;
        border-left-width: 1px;
        border-left-color: {SISENSE_DARKER_GRAY};
        border-left-style: solid;
        border-top-right-radius: 3px;
        border-bottom-right-radius: 3px;
        background-color: {SISENSE_LIGHT_GRAY};
    }}

    QComboBox::down-arrow {{ 

         width: 10px;
         height: 10px;
    }}
    QComboBox::down-arrow:on {{
         position: relative;
         top: 1px; left: 1px;
    }}

    QComboBox QAbstractItemView {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_DARKER_GRAY};
        selection-background-color: {SISENSE_LIGHTER_BLUE}; 
        selection-color: {SISENSE_TEXT_DARK}; 
        padding: 4px;
        outline: 0px;
        font-family: "{FONT_SECONDARY}";
    }}
    QComboBox:!editable {{
         padding-right: 25px; 
    }}

    */
    /* CheckBox Styles - REFINED v4 */
    QCheckBox {{
        spacing: 8px;
        font-family: "{FONT_SECONDARY}";
    }}
    QCheckBox::indicator {{
        width: 16px;
        height: 16px;
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 3px;
        background-color: {SISENSE_WHITE};
    }}
    QCheckBox::indicator:checked {{
        background-color: {SISENSE_LIGHTER_BLUE}; /* Use accent color for background */
        border: 1px solid {SISENSE_LIGHTER_BLUE};
        /* No explicit image - let default checkmark show */
    }}
    QCheckBox::indicator:checked:hover {{
        background-color: {SISENSE_BUTTON_HOVER_BLUE};
        border: 1px solid {SISENSE_BUTTON_HOVER_BLUE};
    }}
    QCheckBox::indicator:unchecked:hover {{
        border: 1px solid {SISENSE_DARK_BLUE}; /* Keep dark border on hover */
    }}

    /* Slider Styles */
    QSlider::groove:horizontal {{
        border: 1px solid {SISENSE_DARKER_GRAY};
        height: 8px;
        background: {SISENSE_MEDIUM_GRAY};
        margin: 2px 0;
        border-radius: 4px;
    }}
    QSlider::handle:horizontal {{
        background: {SISENSE_LIGHTER_BLUE};
        border: 1px solid {SISENSE_LIGHTER_BLUE};
        width: 18px;
        margin: -4px 0;
        border-radius: 9px;
    }}
     QSlider::handle:horizontal:hover {{
        background: {SISENSE_BUTTON_HOVER_BLUE};
        border: 1px solid {SISENSE_BUTTON_HOVER_BLUE};
    }}
    QSlider::handle:horizontal:pressed {{
         background: {SISENSE_BUTTON_PRESSED_BLUE};
        border: 1px solid {SISENSE_BUTTON_PRESSED_BLUE};
    }}


    /* Tab Styles */
    QTabWidget::pane {{
        border: 1px solid {SISENSE_MEDIUM_GRAY};
        background-color: {SISENSE_WHITE};
        border-radius: 4px;
        margin-top: -1px;
    }}
    QTabBar::tab {{
        background: {SISENSE_LIGHT_GRAY};
        border: 1px solid {SISENSE_MEDIUM_GRAY};
        border-bottom: none;
        border-top-left-radius: 4px;
        border-top-right-radius: 4px;
        padding: 6px 10px; /* Reduced horizontal padding */
        margin-right: 2px;
        color: {SISENSE_TEXT_DARK};
        font-family: "{FONT_SECONDARY}";
    }}
    QTabBar::tab:selected {{
        background: {SISENSE_WHITE};
        margin-bottom: -1px;
        font-weight: bold;
        border-color: {SISENSE_MEDIUM_GRAY};
    }}
    QTabBar::tab:!selected:hover {{
        background: {SISENSE_MEDIUM_GRAY};
    }}
    QTabBar::tab:disabled {{
        color: #aaaaaa;
        background: #eeeeee;
    }}

    /* Scroll Area */
    QScrollArea {{
        border: none;
        background-color: transparent;
    }}

    /* Scrollbar Styles */
    QScrollBar:vertical {{
        border: none;
        background: {SISENSE_LIGHT_GRAY};
        width: 12px;
        margin: 0px 0px 0px 0px;
    }}
    QScrollBar::handle:vertical {{
        background: {SISENSE_DARKER_GRAY};
        min-height: 25px;
        border-radius: 6px;
    }}
    QScrollBar::handle:vertical:hover {{
        background: {SISENSE_TEXT_DARK}; /* Dark gray hover */
    }}
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
        height: 0px;
        background: none;
    }}
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{
        background: none;
    }}
    QScrollBar:horizontal {{
        border: none;
        background: {SISENSE_LIGHT_GRAY};
        height: 12px;
        margin: 0px 0px 0px 0px;
    }}
    QScrollBar::handle:horizontal {{
        background: {SISENSE_DARKER_GRAY};
        min-width: 25px;
        border-radius: 6px;
    }}
     QScrollBar::handle:horizontal:hover {{
        background: {SISENSE_TEXT_DARK}; /* Dark gray hover */
    }}
    QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
        width: 0px;
        background: none;
    }}
    QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{
        background: none;
    }}

    /* Status Bar */
    QStatusBar {{
        background-color: {SISENSE_DARKER_GRAY}; /* Darker gray status bar */
        color: {SISENSE_TEXT_DARK}; /* Dark text */
        font-weight: bold;
        font-family: "{FONT_SECONDARY}";
    }}

    /* List Widget (Server Editor) */
    QListWidget {{
        background-color: {SISENSE_WHITE};
        border: 1px solid {SISENSE_DARKER_GRAY};
        border-radius: 4px;
        padding: 4px;
        font-family: "{FONT_SECONDARY}";
    }}
    QListWidget::item {{
        padding: 4px; /* Add padding to items */
    }}
    QListWidget::item:selected {{
        background-color: {SISENSE_LIGHTER_BLUE}; /* Accent blue selection */
        color: {SISENSE_TEXT_DARK};
        border-radius: 3px;
    }}
    QListWidget::item:hover {{
        background-color: {SISENSE_LIGHT_GRAY};
        border-radius: 3px;
    }}
"""
