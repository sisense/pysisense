# frontend/main_window.py

import sys
import os
import yaml
from collections.abc import Mapping
import traceback
import shutil
from datetime import datetime
import subprocess
import re
import glob
import tarfile

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QScrollArea, QPushButton, QLabel, QLineEdit,
    QCheckBox, QTextEdit, QFormLayout, QMessageBox, QStatusBar,
    QSizePolicy, QGroupBox, QComboBox, QSlider, QLayout, QFileDialog, QSpinBox
)
from PySide6.QtCore import Qt, Slot, Signal
from PySide6.QtGui import QFont, QTextCursor, QCursor, QIntValidator, QShortcut, QKeySequence

from config_loader import (SETTINGS_FILE, SERVERS_FILE, APP_NAME)
from utils.utils import get_user_config_path

# Relative imports from other modules in the frontend package
from .config import (
    APP_TITLE, WINDOW_WIDTH, WINDOW_HEIGHT, HELP_ICON_TEXT, # SETTINGS_FILE removed
    BACKUP_SUFFIX, BACKUP_ARCHIVE_PREFIX, MAX_BACKUP_FILES_BEFORE_ARCHIVE,
    MIGRATION_CONTROL_KEYS, MIGRATION_CONTROL_KEYS_SET, TAB_CONTROL_MAP,
    FOLDER_SUB_KEYS, FOLDER_SUB_KEYS_SET, DASHBOARD_SHARE_KEY,
    SELECTED_SOURCE_SERVER_KEY, SELECTED_TARGET_SERVER_KEY, SERVER_DETAIL_KEYS,
    DASHBOARD_MIGRATION_MODE_KEY, DASHBOARD_INCLUDE_LIST_KEY,
    EXCLUDE_DASHBOARDS_BY_NAME_KEY, EXCLUDE_DASHBOARDS_BY_OID_KEY,
    SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY, # New Key Imported
    DASHBOARD_MIGRATION_MODES, DASHBOARD_IMPORT_MODES, CATEGORY_MAP,
    DASHBOARD_FIELD_ORDER, PREFLIGHT_FIELD_ORDER, LOGGING_LEVELS, TAB_ORDER,
    COMMA_STRING_KEYS, KEY_VALUE_EDITOR_KEYS, LIST_OF_DICTS_EDITOR_KEYS,
    HELP_TEXTS # script_dir, settings_dir removed
)

# Import resource_path helper
from .utils import load_servers, save_servers, resource_path
from .widgets import KeyValueEditorWidget, ListOfDictsEditorWidget
from .dialogs import YamlPreviewDialog, ServerEditorDialog

# Define paths using resource_path at the module level if needed,
# or resolve them within functions. Resolving within functions is often safer.
# Example: SETTINGS_FILE_PATH = resource_path('config/settings.yaml')


# --- Main Application Window Class ---
ADVANCED_KEYS = {'target_mongo_connection_string', 'use_mongo_for_target_dashboards', 'enable_runtime_analytics'}
class SettingsApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.setGeometry(100, 100, WINDOW_WIDTH, WINDOW_HEIGHT) # x, y, width, height

        # Resolve paths needed by this class instance
        # self.settings_file_path = resource_path(SETTINGS_FILE)
        self.settings_file_path = get_user_config_path(SETTINGS_FILE, APP_NAME)
        self.settings_dir_path = os.path.dirname(self.settings_file_path)
        # Path to the external script relative to the project root
        self.external_script_rel_path = 'sisense_migration_and_merge_tool.py'


        # Data storage
        self.settings_widgets = {} # Stores {'key': {'widget': QWidget, 'label_widget': QWidget, 'layout': QLayout, ...}}
        self.original_settings = {}
        self.current_config = {}
        self.tab_layouts = {}
        self.migration_plan_widgets = {} # *** ONLY Plan widgets stored here ***
        self.tab_name_to_index = {}
        self.available_servers = {} # Cache loaded server names and details

        # --- Central Widget and Main Layout ---
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)

        # --- Top Buttons ---
        self.button_layout = QHBoxLayout()
        self.manage_servers_button = QPushButton("Manage Servers") # Renamed button
        self.load_button = QPushButton("Load Settings")
        self.upload_settings_button = QPushButton("Upload Settings") # New Button
        self.save_button = QPushButton("Save Settings")
        self.export_button = QPushButton("Export Settings")  # New Export Button
        self.preview_button = QPushButton("Preview YAML")
        self.run_button = QPushButton("Run App")

        # Set cursor for top buttons
        self.manage_servers_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.load_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.upload_settings_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # New Button Cursor
        self.save_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.export_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))  # Set cursor for export
        self.preview_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.run_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))

        self.button_layout.addWidget(self.manage_servers_button) # Keep button
        self.button_layout.addWidget(self.load_button)
        self.button_layout.addWidget(self.upload_settings_button) # Add New Button to Layout
        self.button_layout.addWidget(self.save_button)
        self.button_layout.addWidget(self.export_button)  # Add Export Button to Layout
        self.button_layout.addWidget(self.preview_button)
        self.button_layout.addWidget(self.run_button)
        self.button_layout.addStretch()

        self.main_layout.addLayout(self.button_layout)

        # --- Tab Widget ---
        self.tab_widget = QTabWidget()
        self.main_layout.addWidget(self.tab_widget)

        # --- Hidden Advanced Tab ---
        self.advanced_tab_widget = QWidget()
        self.advanced_tab_layout = QFormLayout(self.advanced_tab_widget)
        self.advanced_tab_widget.setLayout(self.advanced_tab_layout)
        # Add fields for advanced settings
        self.mongo_conn_string_edit = QLineEdit()
        self.use_mongo_checkbox = QCheckBox("Use Mongo for Target Dashboards")
        self.enable_runtime_analytics_checkbox = QCheckBox("Enable Runtime Analytics")
        self.advanced_tab_layout.addRow("Target Mongo Connection String:", self.mongo_conn_string_edit)
        self.advanced_tab_layout.addRow(self.use_mongo_checkbox)
        self.advanced_tab_layout.addRow(self.enable_runtime_analytics_checkbox)
        self.advanced_tab_visible = False
        # Do not add to tab_widget yet

        # Keyboard shortcut to show/hide advanced tab
        self.advanced_tab_shortcut = QShortcut(QKeySequence("Ctrl+Alt+A"), self)
        self.advanced_tab_shortcut.activated.connect(self.toggle_advanced_tab)

        # Create tabs and scroll areas
        bold_font = QFont()
        bold_font.setBold(True)
        default_font = QApplication.font()

        for index, tab_name in enumerate(TAB_ORDER): # Use the updated TAB_ORDER
            tab_content_widget = QWidget()
            scroll_area = QScrollArea()
            scroll_area.setWidgetResizable(True)
            scroll_area.setWidget(tab_content_widget)
            self.tab_name_to_index[tab_name] = index

            if tab_name == "Migration Plan":
                plan_layout = QFormLayout(tab_content_widget)
                plan_layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
                plan_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                plan_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
                # *** Set Field Growth Policy ***
                plan_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
                self.tab_layouts[tab_name] = plan_layout
            elif tab_name == "Connection":
                # *** Connection Tab Layout with Dropdowns ***
                connection_main_layout = QVBoxLayout(tab_content_widget)
                connection_main_layout.setContentsMargins(9, 9, 9, 9)
                connection_main_layout.setSpacing(15) # Increased spacing

                # Server Selection Group
                selection_group = QGroupBox("Server Selection")
                # selection_group.setFont(bold_font)
                selection_layout = QFormLayout(selection_group)
                selection_layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
                selection_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                selection_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
                selection_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow) # Policy for this form

                # Create ComboBoxes but populate later
                self.source_server_combo = QComboBox()
                self.target_server_combo = QComboBox()
                selection_layout.addRow(QLabel("Source Server:"), self.source_server_combo)
                selection_layout.addRow(QLabel("Target Server:"), self.target_server_combo)
                connection_main_layout.addWidget(selection_group)

                # Other Connection Settings GroupBox
                other_conn_group = QGroupBox("Other Connection Settings")
                # other_conn_group.setFont(default_font) # Use default font
                other_layout = QFormLayout()
                other_layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
                other_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                other_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
                # *** Set Field Growth Policy ***
                other_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
                other_conn_group.setLayout(other_layout)
                connection_main_layout.addWidget(other_conn_group)

                connection_main_layout.addStretch() # Push groups to the top

                # Store layouts
                self.tab_layouts[tab_name] = {
                    'selection': selection_layout, # Layout for dropdowns
                    'other': other_layout      # Layout for other connection settings
                }
            else:
                # Standard layout for other tabs
                form_layout = QFormLayout(tab_content_widget)
                form_layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
                form_layout.setLabelAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                form_layout.setFormAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
                # *** Set Field Growth Policy ***
                form_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
                self.tab_layouts[tab_name] = form_layout

            self.tab_widget.addTab(scroll_area, tab_name)

        # --- Status Bar ---
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        # --- Connect Signals to Slots ---
        self.manage_servers_button.clicked.connect(self._open_server_editor) # Connect to new dialog
        self.load_button.clicked.connect(self.load_settings)
        self.upload_settings_button.clicked.connect(self._upload_settings_file) # New Button Connection
        self.save_button.clicked.connect(self.save_settings)
        self.export_button.clicked.connect(self.export_settings)
        self.preview_button.clicked.connect(self.preview_yaml)
        self.run_button.clicked.connect(self.run_app_script)

        # --- Initial Load ---
        self._refresh_server_dropdowns() # Load servers first for dropdowns
        self.load_settings() # Load main settings.yaml

    # --- Methods for Clearing Widgets ---
    def _clear_layout(self, layout):
        """Helper function to recursively clear widgets from a layout."""
        if layout is None:
            return
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
            else:
                sub_layout = item.layout()
                if sub_layout is not None:
                    self._clear_layout(sub_layout)

    def _clear_settings_widgets(self):
        """Removes all widgets from all tab layouts, handling nested layouts."""
        # Clear the central widget registry FIRST
        self.settings_widgets = {}
        self.migration_plan_widgets = {} # Clear plan widgets too

        # Then clear the actual layouts
        for tab_name, layout_or_dict in self.tab_layouts.items():
            if tab_name == "Connection":
                 # Clear only the 'other' layout in the Connection tab
                 other_layout = layout_or_dict.get('other')
                 if isinstance(other_layout, QFormLayout):
                     while other_layout.rowCount() > 0:
                         other_layout.removeRow(other_layout.rowCount() - 1)
                 else:
                     self._clear_layout(other_layout) # Fallback
            elif isinstance(layout_or_dict, QFormLayout):
                 # Clear standard form layouts completely
                 while layout_or_dict.count(): layout_or_dict.removeRow(0)
            else: # Fallback for other layout types if added later
                self._clear_layout(layout_or_dict)


    # --- Methods for Loading Settings and Widgets ---
    @Slot()
    def load_settings(self):
        """Loads settings from settings.yaml and populates the GUI tabs."""
        # Don't clear server dropdowns here, they are refreshed separately
        self._clear_settings_widgets() # Clear widgets and registry (excluding server combos)
        self.status_bar.showMessage(f"Loading {os.path.basename(self.settings_file_path)}...")
        QApplication.processEvents()

        # settings_dir_path defined in __init__
        loaded_data = {}
        try:
            os.makedirs(self.settings_dir_path, exist_ok=True)

            if not os.path.exists(self.settings_file_path):
                self.status_bar.showMessage(f"'{os.path.basename(self.settings_file_path)}' not found. Creating default.")
                try:
                    with open(self.settings_file_path, 'w', encoding='utf-8') as f:
                        # Create default config
                        default_config = {}
                        for key in MIGRATION_CONTROL_KEYS:
                            default_config[key] = False
                        # Add non-bool defaults separately
                        default_config['folders'] = {key: False for key in FOLDER_SUB_KEYS}
                        default_config[DASHBOARD_SHARE_KEY] = False
                        default_config['dashboard_share_concurrency'] = 10
                        default_config[SELECTED_SOURCE_SERVER_KEY] = None
                        default_config[SELECTED_TARGET_SERVER_KEY] = None
                        default_config[DASHBOARD_MIGRATION_MODE_KEY] = 'ALL'
                        default_config[DASHBOARD_INCLUDE_LIST_KEY] = ''
                        default_config[EXCLUDE_DASHBOARDS_BY_NAME_KEY] = ''
                        default_config[EXCLUDE_DASHBOARDS_BY_OID_KEY] = ''
                        default_config[SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY] = False # New Key Default
                        default_config['store_bearer_tokens'] = True
                        default_config['bearer_file_storage_file_name'] = 'bearerTokens'
                        default_config['use_stored_bearer_tokens'] = False
                        default_config['verify'] = True
                        default_config['mssql_connection'] = None
                        default_config['dashboard_import_mode'] = 'skip'
                        default_config['dashboard_migration_concurrency'] = 20
                        default_config['wait_between_chunks'] = 0
                        default_config['wait_chunk_size_threshold'] = 0
                        default_config['dashboard_fetch_chunk_size'] = 100
                        for key in COMMA_STRING_KEYS:
                            if key not in default_config: default_config[key] = ""
                        for key in KEY_VALUE_EDITOR_KEYS: default_config[key] = {}
                        for key in LIST_OF_DICTS_EDITOR_KEYS: default_config[key] = []
                        yaml.dump(default_config, f)
                    loaded_data = default_config
                    QMessageBox.information(self, "Settings File Created", f"'{os.path.basename(self.settings_file_path)}' created with default values.")
                except IOError as create_err:
                    self.status_bar.showMessage(f"Error creating '{os.path.basename(self.settings_file_path)}': {create_err}")
                    QMessageBox.critical(self, "File Creation Error", f"Could not create '{os.path.basename(self.settings_file_path)}':\n{create_err}")
                    self.current_config = {}
                    return
            else:
                with open(self.settings_file_path, 'r', encoding='utf-8') as f:
                    loaded_data = yaml.safe_load(f)
                    if loaded_data is None: loaded_data = {}

            # Clean up potential duplicate folder keys (prioritize 'folders' values)
            cleaned_data = loaded_data.copy()
            folders_section = cleaned_data.get('folders', {})
            keys_to_remove_top_level = []
            if isinstance(folders_section, dict):
                for key in cleaned_data:
                    if key in FOLDER_SUB_KEYS_SET and key != 'migrate_folders':
                         keys_to_remove_top_level.append(key)
            for key in keys_to_remove_top_level:
                if key in cleaned_data:
                    print(f"INFO (Load): Removing duplicate top-level key '{key}' also found in 'folders'.")
                    del cleaned_data[key]

            # Remove old performance keys if they exist
            cleaned_data.pop('retries', None)
            cleaned_data.pop('wait_between_retries', None)


            self.current_config = cleaned_data
            self.original_settings = yaml.safe_load(yaml.dump(self.current_config)) if self.current_config else {}

            # Ensure advanced keys are always present
            if 'enable_runtime_analytics' not in self.current_config:
                self.current_config['enable_runtime_analytics'] = False
            if 'target_mongo_connection_string' not in self.current_config:
                self.current_config['target_mongo_connection_string'] = ''
            if 'use_mongo_for_target_dashboards' not in self.current_config:
                self.current_config['use_mongo_for_target_dashboards'] = False

            # --- Populate Widgets based on TAB_ORDER and defined orders ---
            processed_keys = set()
            unprocessed_keys = set(self.current_config.keys()) # Start with all keys

            # --- Create and Link Migration Plan Widgets FIRST ---
            plan_layout = self.tab_layouts.get("Migration Plan")
            if plan_layout:
                 for key in MIGRATION_CONTROL_KEYS:
                     value = self.current_config.get(key, False)
                     plan_cb = QCheckBox()
                     is_checked = False
                     if isinstance(value, bool): is_checked = value
                     elif isinstance(value, str): is_checked = value.lower() == 'true'
                     plan_cb.setChecked(is_checked)
                     plan_cb.setFont(QApplication.font())
                     label_container_widget = self._create_label_with_help(key)
                     plan_layout.addRow(label_container_widget, plan_cb)
                     self.migration_plan_widgets[key] = plan_cb
                     try: plan_cb.stateChanged.disconnect()
                     except (TypeError, RuntimeError): pass
                     plan_cb.stateChanged.connect(lambda state, k=key: self._update_tab_enabled_state(k, (state == Qt.CheckState.Checked.value)))
                     processed_keys.add(key)
                     unprocessed_keys.discard(key)
            else:
                 print("Error: 'Migration Plan' layout not found.")


            # --- Process remaining tabs ---
            for tab_name in TAB_ORDER:
                if tab_name == 'Migration Plan': continue
                layout_info = self.tab_layouts.get(tab_name)
                if not layout_info:
                    print(f"Warning: Layout info not found for tab '{tab_name}'. Skipping.")
                    continue

                keys_in_tab = []
                target_layout = None

                if tab_name == 'Dashboards':
                    keys_in_tab = DASHBOARD_FIELD_ORDER
                    target_layout = layout_info
                    all_dashboard_keys = {k for k, v in CATEGORY_MAP.items() if v == 'Dashboards'}
                    # Ensure all dashboard keys from CATEGORY_MAP are included if not in DASHBOARD_FIELD_ORDER
                    missing_keys = sorted(list(all_dashboard_keys - set(keys_in_tab) - MIGRATION_CONTROL_KEYS_SET))
                    keys_in_tab.extend(missing_keys)
                elif tab_name == 'Connection':
                    target_layout = layout_info.get('other')
                    keys_in_tab = [k for k, v in CATEGORY_MAP.items() if v == 'Connection' and k not in [SELECTED_SOURCE_SERVER_KEY, SELECTED_TARGET_SERVER_KEY]]
                elif tab_name == 'Folders':
                    keys_in_tab = list(FOLDER_SUB_KEYS_SET)
                    target_layout = layout_info
                elif tab_name == 'Preflight':
                    keys_in_tab = PREFLIGHT_FIELD_ORDER
                    target_layout = layout_info
                else:
                    keys_in_tab = sorted([k for k, v in CATEGORY_MAP.items() if v == tab_name and k not in MIGRATION_CONTROL_KEYS_SET])
                    target_layout = layout_info

                if target_layout is None and tab_name != 'Connection':
                     print(f"Warning: Could not find target layout for tab '{tab_name}'. Skipping.")
                     continue

                for key in keys_in_tab:
                    if key in processed_keys: continue
                    value = None
                    is_folder_key = key in FOLDER_SUB_KEYS_SET
                    config_source = self.current_config.get('folders', {}) if is_folder_key else self.current_config
                    if key in config_source:
                        value = config_source[key]
                    else:
                        print(f"Info: Creating default widget for missing key '{key}' in tab '{tab_name}'.")
                        default_val = False
                        if key in ['store_bearer_tokens', 'use_stored_bearer_tokens', 'verify', DASHBOARD_SHARE_KEY, 'skip_dashboards_with_missing_owner', SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY]: # Added new key here
                            default_val = False
                        elif key == 'bearer_file_storage_file_name': default_val = 'bearerTokens'
                        elif key == 'mssql_connection': default_val = None
                        elif key == DASHBOARD_MIGRATION_MODE_KEY: default_val = 'ALL'
                        elif key in [DASHBOARD_INCLUDE_LIST_KEY, EXCLUDE_DASHBOARDS_BY_NAME_KEY, EXCLUDE_DASHBOARDS_BY_OID_KEY]: default_val = ''
                        elif key == 'dashboard_import_mode': default_val = 'skip'
                        elif key == 'dashboard_migration_concurrency': default_val = 20
                        elif key == 'dashboard_share_concurrency': default_val = 10
                        elif key in ['wait_between_chunks', 'wait_chunk_size_threshold']: default_val = 0
                        elif key == 'dashboard_fetch_chunk_size':
                            default_val = 100
                        value = default_val
                        if is_folder_key:
                             if 'folders' not in self.current_config: self.current_config['folders'] = {}
                             self.current_config['folders'][key] = default_val
                        else:
                             self.current_config[key] = default_val
                    if target_layout:
                        self._add_setting_widget(key, value, target_layout)
                        processed_keys.add(key)
                        unprocessed_keys.discard(key)
                        if is_folder_key: unprocessed_keys.discard('folders')

            # --- Handle 'folders' key if it exists but has no subkeys defined/found ---
            if 'folders' in self.current_config and 'folders' in unprocessed_keys:
                 processed_keys.add('folders')
                 unprocessed_keys.discard('folders')

            # --- Report any remaining unprocessed keys (instead of putting in 'Other') ---
            keys_to_ignore = {SELECTED_SOURCE_SERVER_KEY, SELECTED_TARGET_SERVER_KEY} | set(SERVER_DETAIL_KEYS)
            final_unprocessed = unprocessed_keys - keys_to_ignore - ADVANCED_KEYS
            if final_unprocessed:
                 print(f"Warning: The following keys from '{os.path.basename(self.settings_file_path)}' were not recognized or placed in any tab and will be ignored by the UI (but preserved on save): {sorted(list(final_unprocessed))}")


             # --- Set Server Dropdown Selections ---
            self._set_server_dropdown_selection(SELECTED_SOURCE_SERVER_KEY, self.source_server_combo)
            self._set_server_dropdown_selection(SELECTED_TARGET_SERVER_KEY, self.target_server_combo)
            # Add the dropdowns themselves to settings_widgets for saving
            self.settings_widgets[SELECTED_SOURCE_SERVER_KEY] = {'widget': self.source_server_combo, 'type': str, 'widget_type': 'server_combobox'}
            self.settings_widgets[SELECTED_TARGET_SERVER_KEY] = {'widget': self.target_server_combo, 'type': str, 'widget_type': 'server_combobox'}
            processed_keys.add(SELECTED_SOURCE_SERVER_KEY)
            processed_keys.add(SELECTED_TARGET_SERVER_KEY)


            # Set Initial Tab Enabled States (based on plan widgets created earlier)
            for key in MIGRATION_CONTROL_KEYS:
                 plan_widget = self.migration_plan_widgets.get(key)
                 is_enabled = plan_widget.isChecked() if plan_widget else False
                 self._update_tab_enabled_state(key, is_enabled)

            # Set initial visibility for dependent fields
            self._update_dashboard_include_visibility()
            self._update_oid_host_mapping_visibility()
            self._update_connection_fields_visibility()
            self._update_preflight_concurrency_enabled_state() # Set initial state for new field


            self.status_bar.showMessage(f"Settings loaded successfully from {self.settings_file_path}")

            # After loading settings, update advanced tab widgets (even if not visible)
            self.mongo_conn_string_edit.setText(self.current_config.get('target_mongo_connection_string', ''))
            self.use_mongo_checkbox.setChecked(self.current_config.get('use_mongo_for_target_dashboards', False))
            self.enable_runtime_analytics_checkbox.setChecked(self.current_config.get('enable_runtime_analytics', False))
            # Ensure enable_runtime_analytics is always present in config
            if 'enable_runtime_analytics' not in self.current_config:
                self.current_config['enable_runtime_analytics'] = False

        except yaml.YAMLError as e:
            self.status_bar.showMessage(f"Error parsing YAML: {e}")
            QMessageBox.critical(self, "YAML Error", f"Error parsing '{os.path.basename(self.settings_file_path)}':\n{e}")
            self.current_config = {}
        except IOError as e:
            self.status_bar.showMessage(f"Error reading file: {e}")
            QMessageBox.critical(self, "File Error", f"Error reading '{os.path.basename(self.settings_file_path)}':\n{e}")
            self.current_config = {}
        except Exception as e:
            traceback.print_exc()
            self.status_bar.showMessage(f"An unexpected error occurred during load: {e}")
            QMessageBox.critical(self, "Error", f"An unexpected error occurred during load:\n{e}\n\nSee console for details.")
            self.current_config = {}

    # --- Help Icon Helper ---
    def _create_label_with_help(self, key):
        """Creates a QWidget containing the setting label and a help button."""
        container_widget = QWidget()
        label_layout = QHBoxLayout(container_widget) # Set the layout on the container
        label_layout.setContentsMargins(0, 0, 0, 0)
        label_layout.setSpacing(5)

        key_label = QLabel(str(key) + ":")
        key_label.setFont(QApplication.font())

        help_button = QPushButton(HELP_ICON_TEXT)
        help_button.setFixedSize(25, 25) # Slightly larger for emoji
        help_button.setToolTip(f"Click for help on '{key}'") # Tooltip on hover
        # Use a lambda to pass the key to the slot
        help_button.clicked.connect(lambda checked=False, k=key: self._show_help_popup(k))
        # Set flat style for a less intrusive look
        help_button.setFlat(True)
        # Adjust font size if needed for emoji rendering
        font = help_button.font()
        font.setPointSize(font.pointSize() + 2) # Increase font size slightly
        help_button.setFont(font)
        help_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # Set cursor

        label_layout.addWidget(key_label)
        label_layout.addWidget(help_button)
        label_layout.addStretch() # Push label and button to the left

        # Return the container QWidget
        return container_widget


    def _add_setting_widget(self, key, value, parent_layout):
        """
        Adds a single setting widget (excluding server dropdowns and plan controls)
        to the specified layout, including a help icon next to the label.
        """
        # *** Skip server selection keys and migration control keys ***
        if key in [SELECTED_SOURCE_SERVER_KEY, SELECTED_TARGET_SERVER_KEY] or key in MIGRATION_CONTROL_KEYS_SET:
            return

        if key in self.settings_widgets:
             print(f"Warning: Widget for key '{key}' already exists in registry. Skipping re-creation.")
             return

        # Create the label + help icon container widget
        label_container_widget = self._create_label_with_help(key)

        widget = None # The main input widget or layout containing the widget
        widget_type_str = 'unknown'
        original_type = type(value)
        slider_value_label = None # Specific label for slider value
        field_widget_to_add = None # The actual widget/layout to add to the form layout

        # --- Widget Creation Logic ---
        if key in KEY_VALUE_EDITOR_KEYS:
             initial_data = value if isinstance(value, dict) else {}
             widget = KeyValueEditorWidget(initial_data=initial_data,
                                           key_label="Host" if key == 'oid_host_mapping' else "Key",
                                           value_label="OID Prefix" if key == 'oid_host_mapping' else "Value")
             widget_type_str = 'keyvalue_editor'
             field_widget_to_add = widget

        elif key in LIST_OF_DICTS_EDITOR_KEYS:
             field_config = LIST_OF_DICTS_EDITOR_KEYS[key]
             add_button_text = f"Add Entry"
             if key == 'update_connections': add_button_text = "Add Connection Update"
             initial_data = value if isinstance(value, list) else []
             widget = ListOfDictsEditorWidget(initial_data=initial_data,
                                              field_config=field_config,
                                              add_button_text=add_button_text)
             widget_type_str = 'listofdicts_editor'
             field_widget_to_add = widget

        elif key == 'dashboard_import_mode':
             widget = QComboBox()
             widget.addItems(DASHBOARD_IMPORT_MODES)
             if isinstance(value, str) and value in DASHBOARD_IMPORT_MODES:
                 widget.setCurrentText(value)
             else:
                 widget.setCurrentText('skip') # Default
             widget_type_str = 'dashboard_import_mode_combobox'
             field_widget_to_add = widget

        elif key == DASHBOARD_MIGRATION_MODE_KEY:
            widget = QComboBox()
            widget.addItems(DASHBOARD_MIGRATION_MODES)
            if isinstance(value, str) and value in DASHBOARD_MIGRATION_MODES:
                widget.setCurrentText(value)
            else:
                widget.setCurrentText('ALL') # Default
            try: widget.currentTextChanged.disconnect(self._update_dashboard_include_visibility)
            except (TypeError, RuntimeError): pass
            widget.currentTextChanged.connect(self._update_dashboard_include_visibility)
            widget_type_str = 'dashboard_mode_combobox'
            field_widget_to_add = widget

        elif key in COMMA_STRING_KEYS:
            widget = QTextEdit()
            widget.setAcceptRichText(False); widget.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
            widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.MinimumExpanding)
            widget.setMinimumHeight(80)
            display_text = ""
            if isinstance(value, list): display_text = "\n".join(str(item).strip() for item in value if str(item).strip())
            elif isinstance(value, str): items = [item.strip() for item in re.split(r'\s*,\s*', value) if item.strip()]; display_text = "\n".join(items)
            elif value is not None: display_text = str(value)
            widget.setPlainText(display_text)
            widget_type_str = 'textedit_comma_string'
            field_widget_to_add = widget

        elif isinstance(value, (list, Mapping)) and key != 'folders':
             widget = QTextEdit()
             widget.setAcceptRichText(False); widget.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
             widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.MinimumExpanding)
             widget.setMinimumHeight(80)
             try: yaml_str = yaml.dump(value, default_flow_style=None, sort_keys=False, indent=2, allow_unicode=True); widget.setPlainText(yaml_str)
             except Exception as dump_error: widget.setPlainText(f"# Error dumping value: {dump_error}\n{value}")
             widget_type_str = 'textedit_yaml'
             field_widget_to_add = widget

        elif key in ['file_logLevel', 'console_logLevel']:
            widget = QComboBox(); widget.addItems(LOGGING_LEVELS)
            current_value = str(value).upper()
            if current_value in LOGGING_LEVELS: widget.setCurrentText(current_value)
            else: print(f"Warning: Invalid log level '{value}' for '{key}'. Defaulting INFO."); widget.setCurrentText('INFO')
            widget_type_str = 'combobox'
            field_widget_to_add = widget

        elif isinstance(value, bool) or (isinstance(value, str) and value.lower() in ['true', 'false']):
            widget = QCheckBox()
            is_checked = False
            if isinstance(value, bool):
                is_checked = value
            elif isinstance(value, str):
                is_checked = value.lower() == 'true'
            widget.setChecked(is_checked)
            widget_type_str = 'checkbox'
            field_widget_to_add = widget
            # *** Connect signals for dependent fields ***
            if key == 'use_custom_dashboard_oid':
                try: widget.stateChanged.disconnect(self._update_oid_host_mapping_visibility)
                except (TypeError, RuntimeError): pass
                widget.stateChanged.connect(self._update_oid_host_mapping_visibility)
            elif key == 'enable_update_connections':
                try: widget.stateChanged.disconnect(self._update_connection_fields_visibility)
                except (TypeError, RuntimeError): pass
                widget.stateChanged.connect(self._update_connection_fields_visibility)
            elif key == DASHBOARD_SHARE_KEY: # Connect the share checkbox
                try: widget.stateChanged.disconnect(self._update_preflight_concurrency_enabled_state)
                except (TypeError, RuntimeError): pass
                widget.stateChanged.connect(self._update_preflight_concurrency_enabled_state)


        elif value is None:
            widget = QLineEdit();
            widget.setPlaceholderText("<None>")
            widget.setText("")
            widget_type_str = 'lineedit_none'
            field_widget_to_add = widget

        # *** HANDLE SLIDER for dashboard_share_concurrency ***
        elif key == 'dashboard_share_concurrency':
            widget_type_str = 'slider'
            slider_layout = QHBoxLayout()
            slider_layout.setContentsMargins(0,0,0,0)
            slider_layout.setSpacing(10)

            slider = QSlider(Qt.Orientation.Horizontal)
            slider.setRange(1, 20)
            slider.setSingleStep(1)
            slider.setTickPosition(QSlider.TickPosition.TicksBelow)
            slider.setTickInterval(1)
            slider.setValue(int(value) if isinstance(value, (int, str)) and str(value).isdigit() and 1 <= int(value) <= 20 else 10) # Default 10
            slider.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))

            slider_value_label = QLabel(f"[ {slider.value()} ]")
            slider_value_label.setObjectName("SliderValueLabel") # Set object name for styling
            slider_value_label.setMinimumWidth(40) # Ensure enough space
            slider_value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

            # Connect slider value changed to update label
            slider.valueChanged.connect(lambda val, lbl=slider_value_label: lbl.setText(f"[ {val} ]"))

            slider_layout.addWidget(slider, 1) # Slider takes most space
            slider_layout.addWidget(slider_value_label)

            widget = slider # Store the slider itself for value retrieval
            # The field widget added to the form layout will be the HBox layout
            field_widget_to_add = slider_layout # Add the layout, not just the slider


        else: # Default to QLineEdit
            widget = QLineEdit(str(value))
            widget_type_str = 'lineedit'
            field_widget_to_add = widget
            # Adjust width for specific fields
            if key in ['logFileName', 'post_import_update_connection_function', 'bearer_file_storage_file_name', 'mssql_connection', 'dashboard_migration_concurrency']:
                 widget.setMinimumWidth(300)
            if key in ['new_password_for_migrated_users']:
                 widget.setEchoMode(QLineEdit.EchoMode.Password)


        # --- Add Widget to Layout and Store Info ---
        if widget and field_widget_to_add: # Ensure both widget and the thing to add exist
            widget.setFont(QApplication.font())
            row_index = parent_layout.rowCount()

            # *** Check if adding a QLayout or QWidget ***
            if isinstance(field_widget_to_add, QLayout):
                 parent_layout.addRow(label_container_widget, field_widget_to_add)
            elif isinstance(field_widget_to_add, QWidget):
                 parent_layout.addRow(label_container_widget, field_widget_to_add)
            else:
                 print(f"Error: field_widget_to_add is neither QWidget nor QLayout for key '{key}'")
                 return # Skip adding if type is wrong

            # Store references
            self.settings_widgets[key] = {
                'widget': widget, # Store the primary control (Slider, QLineEdit, etc.)
                'label_widget': label_container_widget, # Store the container with label+help
                'slider_value_label': slider_value_label, # Store the slider label if it exists
                'layout': parent_layout,
                'row_index': parent_layout.rowCount() - 1, # Get the index of the added row
                'type': original_type,
                'widget_type': widget_type_str
            }
        else:
             if key != 'folders': # Don't warn about the 'folders' key itself
                 print(f"Warning: No widget created for key '{key}' with value type {original_type}")


    # --- Methods for Syncing Checkboxes and Tabs ---
    # _sync_migration_checkboxes is no longer needed

    def _update_tab_enabled_state(self, key, is_enabled):
        """Enables or disables the tab controlled by the given migration key."""
        controlled_tab_name = TAB_CONTROL_MAP.get(key)
        if controlled_tab_name:
            tab_index = self.tab_name_to_index.get(controlled_tab_name)
            if tab_index is not None:
                if controlled_tab_name == 'Users & Groups':
                    # Check the *current* state of both relevant checkboxes in the plan tab
                    users_plan_widget = self.migration_plan_widgets.get('migrate_users')
                    groups_plan_widget = self.migration_plan_widgets.get('migrate_groups')
                    users_checked = users_plan_widget.isChecked() if users_plan_widget else False
                    groups_checked = groups_plan_widget.isChecked() if groups_plan_widget else False
                    enable_tab = users_checked or groups_checked
                    self.tab_widget.setTabEnabled(tab_index, enable_tab)
                else:
                    self.tab_widget.setTabEnabled(tab_index, is_enabled)
            else:
                print(f"Warning: Could not find index for tab '{controlled_tab_name}' controlled by key '{key}'.")

    # --- Dashboard Mode Visibility ---
    @Slot()
    def _update_dashboard_include_visibility(self):
        """Shows/hides the dashboard include list based on the selected mode."""
        mode_widget_info = self.settings_widgets.get(DASHBOARD_MIGRATION_MODE_KEY)
        include_list_widget_info = self.settings_widgets.get(DASHBOARD_INCLUDE_LIST_KEY)

        if not mode_widget_info or not include_list_widget_info:
            return

        mode_combo = mode_widget_info['widget']
        include_list_widget = include_list_widget_info['widget']
        # Get the label container widget
        include_list_label_container = include_list_widget_info.get('label_widget')
        parent_layout = include_list_widget_info.get('layout')

        if not parent_layout:
             if include_list_widget_info:
                 print(f"Warning: Layout missing for widget '{DASHBOARD_INCLUDE_LIST_KEY}' in settings_widgets.")
             return

        current_mode = mode_combo.currentText()

        # Find the row index dynamically
        row_index = include_list_widget_info.get('row_index', -1) # Get stored row index

        if row_index != -1:
            if current_mode == 'ALL':
                parent_layout.setRowVisible(row_index, False)
            elif current_mode == 'By Name' or current_mode == 'By OID':
                parent_layout.setRowVisible(row_index, True)
                # Update label text (find the QLabel within the label_container)
                if include_list_label_container:
                    # Find the actual QLabel within the container widget's layout
                    label = include_list_label_container.findChild(QLabel)
                    if label:
                         label.setText(f"{DASHBOARD_INCLUDE_LIST_KEY} (Names):" if current_mode == 'By Name' else f"{DASHBOARD_INCLUDE_LIST_KEY} (OIDs):")
                if include_list_widget and isinstance(include_list_widget, QTextEdit):
                    include_list_widget.setPlaceholderText("Enter dashboard names, one per line" if current_mode == 'By Name' else "Enter dashboard OIDs, one per line")
        else:
             if include_list_widget_info:
                 print(f"Warning: Could not find row index for widget '{DASHBOARD_INCLUDE_LIST_KEY}' to update visibility.")


    # *** Slot to update oid_host_mapping visibility ***
    @Slot()
    def _update_oid_host_mapping_visibility(self):
        """Enables/disables the oid_host_mapping widget based on use_custom_dashboard_oid."""
        use_custom_oid_info = self.settings_widgets.get('use_custom_dashboard_oid')
        oid_mapping_info = self.settings_widgets.get('oid_host_mapping')

        if not use_custom_oid_info or not oid_mapping_info:
            return

        use_custom_oid_checkbox = use_custom_oid_info['widget']
        oid_mapping_widget = oid_mapping_info['widget']
        # Get the container widget containing the label and help icon
        oid_mapping_label_container = oid_mapping_info.get('label_widget')

        is_enabled = use_custom_oid_checkbox.isChecked()

        # Enable/disable the actual input widget
        if oid_mapping_widget:
            oid_mapping_widget.setEnabled(is_enabled)
        # Enable/disable the label container widget (which holds label + help)
        if oid_mapping_label_container:
            oid_mapping_label_container.setEnabled(is_enabled)

    # *** Slot to update connection fields visibility ***
    @Slot()
    def _update_connection_fields_visibility(self):
        """Shows/hides update_connections and post_import_update_connection_function widgets based on enable_update_connections."""
        enable_update_connections_info = self.settings_widgets.get('enable_update_connections')
        update_connections_info = self.settings_widgets.get('update_connections')
        post_import_function_info = self.settings_widgets.get('post_import_update_connection_function')

        if not enable_update_connections_info:
            return

        enable_update_connections_checkbox = enable_update_connections_info['widget']
        is_checked = enable_update_connections_checkbox.isChecked()

        # Get parent layout and row indices for visibility control
        if update_connections_info:
            update_connections_row_index = update_connections_info.get('row_index', -1)
            update_connections_layout = update_connections_info.get('layout')
            if update_connections_row_index != -1 and update_connections_layout:
                update_connections_layout.setRowVisible(update_connections_row_index, is_checked)

        if post_import_function_info:
            post_import_function_row_index = post_import_function_info.get('row_index', -1)
            post_import_function_layout = post_import_function_info.get('layout')
            if post_import_function_row_index != -1 and post_import_function_layout:
                post_import_function_layout.setRowVisible(post_import_function_row_index, is_checked)

    # *** Slot to update dashboard_share_concurrency enabled state ***
    @Slot()
    def _update_preflight_concurrency_enabled_state(self):
        """Enables/disables the dashboard_share_concurrency field based on dashboard_share_with_migration_user."""
        share_checkbox_info = self.settings_widgets.get(DASHBOARD_SHARE_KEY)
        concurrency_widget_info = self.settings_widgets.get('dashboard_share_concurrency')

        if not share_checkbox_info or not concurrency_widget_info:
            # Widgets might not be fully loaded yet
            return

        share_checkbox = share_checkbox_info['widget']
        # The main widget is the slider
        concurrency_slider = concurrency_widget_info['widget']
        # Get the label showing the slider value
        concurrency_value_label = concurrency_widget_info.get('slider_value_label')
        # Get the label container (with help icon)
        concurrency_label_container = concurrency_widget_info.get('label_widget')

        is_enabled = share_checkbox.isChecked()

        # Enable/disable slider, value label, and label container
        if concurrency_slider:
            concurrency_slider.setEnabled(is_enabled)
        if concurrency_value_label:
            concurrency_value_label.setEnabled(is_enabled)
        if concurrency_label_container:
            concurrency_label_container.setEnabled(is_enabled)


    # --- Help Popup Method ---
    @Slot(str)
    def _show_help_popup(self, key):
        """Displays a help message for the given settings key."""
        help_text = HELP_TEXTS.get(key, "No specific help available for this field.")
        QMessageBox.information(self, f"Help: {key}", help_text)

    # --- Method for Uploading Settings File ---
    @Slot()
    def _upload_settings_file(self):
        """Opens a file dialog to upload a settings.yaml file."""
        file_dialog = QFileDialog(self)
        file_dialog.setNameFilter("YAML files (*.yaml *.yml)")
        file_dialog.setWindowTitle("Upload Settings File")
        file_dialog.setFileMode(QFileDialog.FileMode.ExistingFile)

        if file_dialog.exec():
            selected_files = file_dialog.selectedFiles()
            if selected_files:
                source_file_path = selected_files[0]
                destination_file_path = self.settings_file_path
                # destination_filename = os.path.basename(destination_file_path) # Not needed for status message

                reply = QMessageBox.question(self, "Confirm Upload",
                                             f"Do you want to replace your current '{os.path.basename(destination_file_path)}' with the selected file?\n\n'{os.path.basename(source_file_path)}' -> '{os.path.basename(destination_file_path)}'\n\nA backup of the current settings will be made.",
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                             QMessageBox.StandardButton.No)

                if reply == QMessageBox.StandardButton.Yes:
                    try:
                        # --- Backup existing settings file before overwriting ---
                        if os.path.exists(destination_file_path):
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            backup_filename = f"{destination_file_path}.upload_backup_{timestamp}{BACKUP_SUFFIX}"
                            shutil.copy2(destination_file_path, backup_filename)
                            print(f"Backup of current settings created: {os.path.basename(backup_filename)}")
                            self.status_bar.showMessage(f"Backed up current settings to {os.path.basename(backup_filename)}")
                            QApplication.processEvents()


                        # Copy the new file
                        shutil.copy2(source_file_path, destination_file_path)
                        # Update status bar to show full path of the destination
                        self.status_bar.showMessage(f"Uploaded '{os.path.basename(source_file_path)}' to '{destination_file_path}'. Reloading...")
                        QApplication.processEvents()

                        # Reload settings from the new file
                        self.load_settings() # This will update self.current_config and UI
                        # The load_settings method already shows a success message.
                        # If load_settings fails, it will show its own error.

                    except FileNotFoundError:
                        QMessageBox.critical(self, "Upload Error", f"Error: The source file '{os.path.basename(source_file_path)}' was not found.")
                        self.status_bar.showMessage("Upload failed: Source file not found.")
                    except IOError as e:
                        QMessageBox.critical(self, "Upload Error", f"Error copying file: {e}")
                        self.status_bar.showMessage(f"Upload failed: Error copying file.")
                    except Exception as e:
                        traceback.print_exc()
                        QMessageBox.critical(self, "Upload Error", f"An unexpected error occurred during upload: {e}")
                        self.status_bar.showMessage(f"Upload failed: Unexpected error.")
                else:
                    self.status_bar.showMessage("Upload cancelled by user.")
            else:
                self.status_bar.showMessage("No file selected for upload.")
        else:
            self.status_bar.showMessage("Upload cancelled.")


    # --- Methods for Saving Settings and Preview ---
    def _get_settings_from_gui(self):
        """
        Retrieves settings from GUI, including selected server names and dashboard mode.
        Sets dashboard_include_list to 'ALL' if mode is 'ALL'.
        Reads migration control keys from the migration_plan_widgets.
        Preserves unhandled keys from the original settings.
        Validates dashboard_share_concurrency range.
        """
        current_gui_settings = {}
        errors = []
        folders_dict = {}

        # Get dashboard mode first to potentially override include list later
        dashboard_mode = 'ALL' # Default
        mode_info = self.settings_widgets.get(DASHBOARD_MIGRATION_MODE_KEY)
        if mode_info and mode_info['widget_type'] == 'dashboard_mode_combobox':
             dashboard_mode = mode_info['widget'].currentText()

        # --- Get values from standard widgets ---
        for key, info in self.settings_widgets.items():
            # Skip plan widgets here, handle them separately
            if key in MIGRATION_CONTROL_KEYS_SET: continue

            widget = info['widget']
            original_type = info['type']
            widget_type = info['widget_type']
            value_to_store = None

            try:
                # Handle Server Selection Dropdowns
                if widget_type == 'server_combobox':
                    selected_text = widget.currentText()
                    value_to_store = selected_text if selected_text != "<Select Server>" else None
                # Handle Dashboard Mode Dropdown
                elif widget_type == 'dashboard_mode_combobox':
                     value_to_store = widget.currentText()
                # Handle Dashboard Import Mode Dropdown
                elif widget_type == 'dashboard_import_mode_combobox':
                     value_to_store = widget.currentText()
                # Handle other widget types
                elif widget_type == 'checkbox':
                    value_to_store = bool(widget.isChecked())
                elif widget_type == 'combobox': # Standard combobox (e.g., log level)
                    value_to_store = widget.currentText()
                # *** HANDLE SLIDER ***
                elif widget_type == 'slider':
                    if isinstance(widget, QSlider):
                        value_to_store = widget.value()
                    else:
                        errors.append(f"Key '{key}': Expected QSlider.")
                        value_to_store = 10 # Default on error
                elif widget_type == 'lineedit':
                    value_str = widget.text()
                    # Try conversion only if original type wasn't str and input is not empty
                    # Exclude port as it's handled specially elsewhere (ServerEditorDialog)
                    if original_type != str and value_str.strip() and 'port' not in key:
                        try:
                            if original_type == int:
                                value_to_store = int(value_str)
                            elif original_type == float:
                                value_to_store = float(value_str)
                            # Add other potential conversions if needed
                            else: value_to_store = original_type(value_str) # Generic attempt
                        except (ValueError, TypeError) as e:
                            errors.append(f"Key '{key}': Cannot convert '{value_str}' to expected type. Saving as string. Err: {e}")
                            value_to_store = value_str # Save as string on error
                    else:
                        value_to_store = value_str # Keep as string
                elif widget_type == 'lineedit_none':
                    value_str = widget.text().strip()
                    value_to_store = None if not value_str or value_str == "<None>" else value_str
                elif widget_type == 'keyvalue_editor':
                    if isinstance(widget, KeyValueEditorWidget): value_to_store = widget.get_data()
                    else: errors.append(f"Key '{key}': Expected KeyValueEditorWidget."); value_to_store = {} # Default
                elif widget_type == 'listofdicts_editor':
                    if isinstance(widget, ListOfDictsEditorWidget): value_to_store = widget.get_data()
                    else: errors.append(f"Key '{key}': Expected ListOfDictsEditorWidget."); value_to_store = [] # Default
                elif widget_type == 'textedit_comma_string':
                    plain_text = widget.toPlainText()
                    items = [line.strip() for line in plain_text.splitlines() if line.strip()]
                    value_to_store = ", ".join(items) if items else ""
                    # Override if dashboard mode is ALL
                    if key == DASHBOARD_INCLUDE_LIST_KEY and dashboard_mode == 'ALL':
                        value_to_store = "ALL"
                elif widget_type == 'textedit_yaml':
                    yaml_str = widget.toPlainText().strip()
                    # Determine original value source (top-level or folders)
                    original_val = None
                    if key in FOLDER_SUB_KEYS_SET:
                        original_val = self.original_settings.get('folders', {}).get(key)
                    else:
                        original_val = self.original_settings.get(key)

                    default_empty = [] if isinstance(original_val, list) else {} if isinstance(original_val, Mapping) else None
                    if not yaml_str or yaml_str.startswith('# Error dumping value:'):
                         value_to_store = default_empty
                         if yaml_str: errors.append(f"Key '{key}': Invalid YAML, saving default empty value ({type(value_to_store).__name__}).")
                    else:
                        try:
                            loaded_yaml = yaml.safe_load(yaml_str)
                            # Check if loaded type matches original type (if original was list/dict)
                            if isinstance(original_type, (list, Mapping)) and not isinstance(loaded_yaml, original_type):
                                errors.append(f"Key '{key}': YAML loaded as {type(loaded_yaml).__name__}, expected {original_type.__name__}. Reverting.")
                                value_to_store = original_val # Revert to original
                            else:
                                value_to_store = loaded_yaml
                        except yaml.YAMLError as e:
                            errors.append(f"Key '{key}': Invalid YAML syntax. Reverting. Error: {e}")
                            value_to_store = original_val # Revert to original

                # Store the value
                if key in FOLDER_SUB_KEYS_SET:
                    folders_dict[key] = value_to_store
                elif key != 'folders': # Don't store 'folders' key itself here
                    current_gui_settings[key] = value_to_store

            except Exception as e:
                traceback.print_exc()
                errors.append(f"Error processing widget for key '{key}': {e}. Reverting. See console.")
                # Determine original value source on error
                original_val_on_error = None
                if key in FOLDER_SUB_KEYS_SET:
                    original_val_on_error = self.original_settings.get('folders', {}).get(key)
                else:
                    original_val_on_error = self.original_settings.get(key)

                # Store reverted value
                if key in FOLDER_SUB_KEYS_SET:
                    folders_dict[key] = original_val_on_error
                elif key != 'folders':
                    current_gui_settings[key] = original_val_on_error

        # --- Get values from Migration Plan widgets ---
        for key, plan_widget in self.migration_plan_widgets.items():
            if isinstance(plan_widget, QCheckBox):
                value_to_store = bool(plan_widget.isChecked())
                current_gui_settings[key] = value_to_store
            else:
                 errors.append(f"Unexpected widget type found in migration_plan_widgets for key '{key}'.")


        # --- Construct the final settings dictionary ---
        final_settings = {}

        # 1. Start with original settings to preserve order and unhandled keys
        final_settings = self.original_settings.copy() if self.original_settings else {}

        # 2. Update with values obtained from the GUI widgets
        final_settings.update(current_gui_settings)

        # 3. Handle the 'folders' section specifically
        if folders_dict or ('folders' in self.original_settings and isinstance(self.original_settings['folders'], dict)):
            original_folder_data = self.original_settings.get('folders', {}) if isinstance(self.original_settings.get('folders'), dict) else {}
            final_folders_sub_dict = {}
            # Use defined sub-keys for order within 'folders'
            for f_key in FOLDER_SUB_KEYS:
                if f_key in folders_dict:
                    final_folders_sub_dict[f_key] = folders_dict[f_key]
                elif f_key in original_folder_data: # Preserve original if not in GUI dict
                    final_folders_sub_dict[f_key] = original_folder_data[f_key]

            # Add any other keys that might have been in the original folders dict,
            # EXCLUDING 'migrate_folders' as it's handled top-level now.
            for orig_f_key, orig_f_val in original_folder_data.items():
                 # *** MODIFICATION START ***
                 # Explicitly skip 'migrate_folders' if found inside original folders dict
                 if orig_f_key == 'migrate_folders':
                     continue
                 # *** MODIFICATION END ***

                 if orig_f_key not in final_folders_sub_dict:
                     final_folders_sub_dict[orig_f_key] = orig_f_val
                     # Update warning message to be more specific
                     errors.append(f"Key '{orig_f_key}' (in folders): Preserved original value (key not defined in FOLDER_SUB_KEYS).")


            # Only add the 'folders' key back if it contains something or existed originally
            if final_folders_sub_dict or ('folders' in self.original_settings and isinstance(self.original_settings['folders'], dict)):
                 final_settings['folders'] = final_folders_sub_dict
            elif 'folders' in final_settings: # Remove if empty and wasn't originally a dict
                 del final_settings['folders']

        elif 'folders' in final_settings: # Remove folders section if it wasn't originally a dict and no subkeys were found in GUI
             del final_settings['folders']


        # 4. Ensure essential keys exist (even if None or default)
        # Server selection
        if SELECTED_SOURCE_SERVER_KEY not in final_settings:
            final_settings[SELECTED_SOURCE_SERVER_KEY] = None
        if SELECTED_TARGET_SERVER_KEY not in final_settings:
            final_settings[SELECTED_TARGET_SERVER_KEY] = None
        # Dashboard mode/lists
        if DASHBOARD_MIGRATION_MODE_KEY not in final_settings:
            final_settings[DASHBOARD_MIGRATION_MODE_KEY] = 'ALL'
        if DASHBOARD_INCLUDE_LIST_KEY not in final_settings:
            final_settings[DASHBOARD_INCLUDE_LIST_KEY] = "ALL" if final_settings.get(DASHBOARD_MIGRATION_MODE_KEY) == 'ALL' else ''
        if EXCLUDE_DASHBOARDS_BY_NAME_KEY not in final_settings:
            final_settings[EXCLUDE_DASHBOARDS_BY_NAME_KEY] = ''
        if EXCLUDE_DASHBOARDS_BY_OID_KEY not in final_settings:
            final_settings[EXCLUDE_DASHBOARDS_BY_OID_KEY] = ''
        if SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY not in final_settings: # New Key Ensure
            final_settings[SKIP_DASHBOARDS_WITH_MISSING_ANCESTOR_FOLDER_KEY] = False # New Key Default
        # Migration control keys
        for mkey in MIGRATION_CONTROL_KEYS:
            if mkey not in final_settings:
                final_settings[mkey] = False # Default to False
        # New preflight key
        if 'dashboard_share_concurrency' not in final_settings:
             final_settings['dashboard_share_concurrency'] = 10 # Default


        # 5. Remove individual server detail keys (they belong in servers.yaml)
        for detail_key in SERVER_DETAIL_KEYS:
            final_settings.pop(detail_key, None)


        # Re-apply original order where possible (best effort)
        ordered_final_settings = {}
        original_keys_order = list(self.original_settings.keys()) if self.original_settings else []

        # Add keys based on original order
        for key in original_keys_order:
            if key in final_settings:
                ordered_final_settings[key] = final_settings[key]

        # Add any keys present in final_settings but not in original order
        for key, value in final_settings.items():
            if key not in ordered_final_settings:
                ordered_final_settings[key] = value

        # --- Always include advanced tab values ---
        try:
            ordered_final_settings['target_mongo_connection_string'] = self.mongo_conn_string_edit.text()
        except Exception:
            ordered_final_settings['target_mongo_connection_string'] = ''
        try:
            ordered_final_settings['use_mongo_for_target_dashboards'] = self.use_mongo_checkbox.isChecked()
        except Exception:
            ordered_final_settings['use_mongo_for_target_dashboards'] = False
        try:
            ordered_final_settings['enable_runtime_analytics'] = self.enable_runtime_analytics_checkbox.isChecked()
        except Exception:
            ordered_final_settings['enable_runtime_analytics'] = False

        return ordered_final_settings, errors


    @Slot()
    def save_settings(self):
        """Saves the current GUI settings back to the YAML file, creating a backup first."""
        updated_settings, errors = self._get_settings_from_gui()
        # settings_dir_path defined in __init__

        # Filter out the specific 'migrate_folders' warning before displaying
        display_errors = [e for e in errors if not ("Key 'migrate_folders' (in folders):" in e and "Preserved original value" in e)]
        # Filter out other non-critical preservation warnings
        display_errors = [e for e in display_errors if "Preserving original value" not in e and "Added default" not in e and "Added (was not in original" not in e]
        # Filter out validation warnings handled by defaulting
        # display_errors = [e for e in display_errors if "is outside the valid range" not in e and "Cannot convert" not in e]


        if display_errors:
            error_message = "Potential issues found during save:\n\n" + "\n".join(display_errors) + "\n\nProceed with saving?"
            reply = QMessageBox.warning(self, "Save Warnings", error_message,
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                        QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.No:
                self.status_bar.showMessage("Save cancelled due to warnings.")
                return

        # --- Backup and Archiving Logic ---
        backup_made = False
        backup_filename = ""
        archive_filename = ""
        try:
            os.makedirs(self.settings_dir_path, exist_ok=True) # Ensure dist directory exists

            # Check for existing backups and archive if needed
            backup_pattern = os.path.join(self.settings_dir_path, f"{os.path.splitext(os.path.basename(self.settings_file_path))[0]}.*{BACKUP_SUFFIX}")
            existing_backups = glob.glob(backup_pattern)

            if len(existing_backups) >= MAX_BACKUP_FILES_BEFORE_ARCHIVE:
                timestamp_archive = datetime.now().strftime("%Y%m%d_%H%M%S")
                archive_filename = os.path.join(self.settings_dir_path, f"{BACKUP_ARCHIVE_PREFIX}{timestamp_archive}.tar.gz")
                print(f"Archiving {len(existing_backups)} backups to {os.path.basename(archive_filename)}...")
                try:
                    with tarfile.open(archive_filename, "w:gz") as tar:
                        for backup_file in existing_backups:
                            tar.add(backup_file, arcname=os.path.basename(backup_file))
                    print("Archive created successfully.")
                    # Delete original files AFTER successful archival
                    for backup_file in existing_backups:
                        try:
                            os.remove(backup_file)
                        except OSError as del_err:
                            print(f"Warning: Could not delete archived backup file '{os.path.basename(backup_file)}': {del_err}")
                except (tarfile.TarError, IOError, OSError) as archive_err:
                    print(f"Error creating archive '{os.path.basename(archive_filename)}': {archive_err}")
                    # Don't delete originals if archive failed
                    archive_filename = "" # Clear archive name so it's not mentioned in a success message

            # Create the new backup file for the current save
            if os.path.exists(self.settings_file_path):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_filename = f"{self.settings_file_path}.{timestamp}{BACKUP_SUFFIX}"
                shutil.copy2(self.settings_file_path, backup_filename)
                backup_made = True
                print(f"Backup created: {os.path.basename(backup_filename)}")

        except (IOError, OSError, shutil.Error) as backup_err:
            self.status_bar.showMessage(f"Error during backup/archive: {backup_err}")
            QMessageBox.critical(self, "Backup/Archive Error", f"Could not create backup or archive.\nError: {backup_err}\n\nSaving Aborted.")
            return
        # --- End Backup/Archive Logic ---


        # Proceed with saving the main settings file
        try:
            # os.makedirs(settings_dir, exist_ok=True) # Already ensured above
            with open(self.settings_file_path, 'w', encoding='utf-8') as f:
                # Use sort_keys=False to preserve order as much as possible from _get_settings_from_gui
                yaml.dump(updated_settings, f, default_flow_style=None, sort_keys=False, allow_unicode=True, indent=2)

            save_msg = f"Settings saved successfully to {os.path.basename(self.settings_file_path)}"
            if backup_made: save_msg += f" (Backup: {os.path.basename(backup_filename)})"
            if archive_filename: save_msg += f" (Archived old backups to {os.path.basename(archive_filename)})"

            # Update internal state AFTER successful save
            self.current_config = updated_settings
            # Update original_settings to reflect the saved state
            self.original_settings = yaml.safe_load(yaml.dump(updated_settings)) if updated_settings else {}

            # Reload GUI (will repopulate widgets based on saved settings)
            final_status_message = save_msg
            self._refresh_server_dropdowns() # Ensure dropdowns have latest servers
            self.load_settings() # Reload settings and apply selections
            self.status_bar.showMessage(final_status_message) # Show final status after reload

            # Always set advanced tab values before saving
            try:
                self.current_config['target_mongo_connection_string'] = self.mongo_conn_string_edit.text()
            except Exception:
                self.current_config['target_mongo_connection_string'] = ''
            try:
                self.current_config['use_mongo_for_target_dashboards'] = self.use_mongo_checkbox.isChecked()
            except Exception:
                self.current_config['use_mongo_for_target_dashboards'] = False
            try:
                self.current_config['enable_runtime_analytics'] = self.enable_runtime_analytics_checkbox.isChecked()
            except Exception:
                self.current_config['enable_runtime_analytics'] = False

        except IOError as e:
            self.status_bar.showMessage(f"Error writing file: {e}")
            QMessageBox.critical(self, "File Error", f"Error writing to '{os.path.basename(self.settings_file_path)}':\n{e}")
            # Attempt to restore original settings in memory if save failed
            self.current_config = self.original_settings.copy() if self.original_settings else {}
        except yaml.YAMLError as e:
            self.status_bar.showMessage(f"Error formatting YAML for save: {e}")
            QMessageBox.critical(self, "YAML Error", f"Error preparing data for saving:\n{e}")
            self.current_config = self.original_settings.copy() if self.original_settings else {}
        except Exception as e:
            traceback.print_exc()
            self.status_bar.showMessage(f"An unexpected error occurred during save: {e}")
            QMessageBox.critical(self, "Error", f"An unexpected error occurred during save:\n{e}. See console.")
            self.current_config = self.original_settings.copy() if self.original_settings else {}


    @Slot()
    def preview_yaml(self):
        """Gathers current settings and displays them as YAML in a dialog."""
        self.status_bar.showMessage("Generating YAML preview...")
        QApplication.processEvents()

        preview_settings, errors = self._get_settings_from_gui()

        # Filter out the specific 'migrate_folders' warning before displaying
        display_errors = [e for e in errors if not ("Key 'migrate_folders' (in folders):" in e and "Preserved original value" in e)]
        # Filter out other non-critical preservation warnings
        display_errors = [e for e in display_errors if "Preserving original value" not in e and "Added default" not in e and "Added (was not in original" not in e]
        # Filter out validation warnings handled by defaulting
        display_errors = [e for e in display_errors if "is outside the valid range" not in e and "Cannot convert" not in e]

        if display_errors:
            error_message = "Potential issues found while generating preview (these won't be saved unless you click Save):\n\n" + "\n".join(display_errors)
            QMessageBox.warning(self, "Preview Warnings", error_message)

        try:
            yaml_string = yaml.dump(
                preview_settings,
                default_flow_style=False,
                sort_keys=False, # Preserve order from _get_settings_from_gui
                allow_unicode=True,
                indent=2
            )
            dialog = YamlPreviewDialog(yaml_string, self)
            dialog.exec()
            self.status_bar.showMessage("Preview closed.")

        except yaml.YAMLError as e:
            self.status_bar.showMessage(f"Error formatting YAML for preview: {e}")
            QMessageBox.critical(self, "YAML Error", f"Error preparing data for preview:\n{e}")
        except Exception as e:
            traceback.print_exc()
            self.status_bar.showMessage(f"An unexpected error occurred during preview: {e}")
            QMessageBox.critical(self, "Error", f"An unexpected error occurred during preview:\n{e}. See console.")

    # --- Methods for Running External Script ---
    @Slot()
    def run_app_script(self):
        """Runs the user's separate Python application script using subprocess."""
        if not os.path.exists(self.settings_file_path):
            QMessageBox.warning(self, "Run Error", f"Settings file '{os.path.basename(self.settings_file_path)}' not found. Please load or save settings first.")
            self.status_bar.showMessage("Run aborted: Settings file missing.")
            return

        reply = QMessageBox.question(self, "Run Application",
                                     "Save current settings before running the migration script?",
                                     QMessageBox.StandardButton.Save | QMessageBox.StandardButton.Discard | QMessageBox.StandardButton.Cancel,
                                     QMessageBox.StandardButton.Save)

        if reply == QMessageBox.StandardButton.Save:
            self.save_settings()
            if "Save cancelled" in self.status_bar.currentMessage() or "Error" in self.status_bar.currentMessage():
                print("Run cancelled because settings save failed or was cancelled.")
                return
        elif reply == QMessageBox.StandardButton.Cancel:
            self.status_bar.showMessage("Run cancelled.")
            return

        self.status_bar.showMessage("Running migration script...")
        print("\n--- Attempting to Run Migration Script ---")
        QApplication.processEvents()

        app_script_path = resource_path(self.external_script_rel_path)
        app_script_name = os.path.basename(app_script_path)
        cwd = os.path.dirname(app_script_path) if getattr(sys, 'frozen', False) else os.path.dirname(resource_path('.'))

        if not os.path.exists(app_script_path):
             error_msg = f"Error: The migration script '{app_script_name}' was not found at '{app_script_path}'."
             print(error_msg)
             QMessageBox.critical(self, "Run Error", error_msg)
             self.status_bar.showMessage("Run failed: Migration script not found.")
             return

        command_list = [str(sys.executable), str(app_script_path), '--settings-file', str(self.settings_file_path)]

        try:
            print(f"Executing: {' '.join(command_list)}")
            print(f"Working Directory: {cwd}")
            self.run_button.setEnabled(False)

            if sys.platform == "win32":
                # On Windows, run in a new console window that stays open after execution.
                # The '/K' argument to cmd.exe tells it to run the command and then remain.
                final_command = ['cmd.exe', '/K'] + command_list
                process = subprocess.Popen(final_command, creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=cwd)
                self.status_bar.showMessage(f"Migration script '{app_script_name}' launched in a new window.")
                # We don't wait for the process, so the success message box is different
                QMessageBox.information(self, "Script Launched",
                                        f"The migration script '{app_script_name}' has been launched in a new window.\n"
                                        "The settings editor will remain open. Check the new window for script progress and prompts.")
            else:
                # On other platforms (macOS, Linux), run as before.
                # Interactive prompts from the script might still cause issues or not work as expected.
                # The GUI might appear to hang if the script waits for input in the background.
                # print("Note: On non-Windows platforms, interactive prompts from the script may not work correctly when run from the GUI.")
                result = subprocess.run(
                    command_list, check=True, capture_output=False, text=True, cwd=cwd, encoding='utf-8'
                )
                self.status_bar.showMessage("Migration script completed successfully.")
                msg_box = QMessageBox(self)
                msg_box.setWindowTitle("Run Successful")
                msg_box.setIcon(QMessageBox.Icon.Information)
                msg_box.setText(f"The migration script '{app_script_name}' finished successfully.")
                msg_box.setInformativeText("Check the console output window for details.\n\nDo you want to close the settings editor?")
                close_button = msg_box.addButton("Close Editor", QMessageBox.ButtonRole.AcceptRole)
                ok_button = msg_box.addButton(QMessageBox.StandardButton.Ok)
                msg_box.setDefaultButton(ok_button)
                msg_box.exec()
                if msg_box.clickedButton() == close_button:
                     self.close()

        except FileNotFoundError: # This applies if sys.executable or script path is wrong
            error_msg = f"Error: Could not find Python executable or script path.\nExecutable: {sys.executable}\nScript: {app_script_path}"
            print(error_msg)
            QMessageBox.critical(self, "Run Error", error_msg)
            self.status_bar.showMessage("Run failed: Could not find executable or script.")
        except subprocess.CalledProcessError as e: # Only for subprocess.run with check=True
            error_msg = f"Migration script '{app_script_name}' failed with exit code {e.returncode}."
            print(error_msg)
            QMessageBox.critical(self, "Run Error", f"{error_msg}\n\nCheck the console output window for specific errors reported by the script.")
            self.status_bar.showMessage("Run failed: Script returned an error.")
        except Exception as e:
             traceback.print_exc()
             error_msg = f"An unexpected error occurred while trying to run the migration script:\n{e}"
             QMessageBox.critical(self, "Run Error", f"{error_msg}\n\nSee console for details.")
             self.status_bar.showMessage(f"Run failed: Unexpected error.")
        finally:
            self.run_button.setEnabled(True) # Re-enable the run button
            print("--- End of Migration Script Run Attempt ---\n")


    # --- Server Editor Methods ---
    @Slot()
    def _open_server_editor(self):
        """Opens the Server Editor dialog."""
        dialog = ServerEditorDialog(self)
        dialog.servers_updated.connect(self._refresh_server_dropdowns_and_preserve_selection)
        dialog.exec()

    @Slot()
    def _refresh_server_dropdowns_and_preserve_selection(self):
        """Refreshes server dropdowns after editor closes, trying to preserve selection."""
        selected_source = self.source_server_combo.currentText()
        selected_target = self.target_server_combo.currentText()
        self._refresh_server_dropdowns()
        self._set_combo_box_text(self.source_server_combo, selected_source)
        self._set_combo_box_text(self.target_server_combo, selected_target)

    def _refresh_server_dropdowns(self):
        """Loads servers from file and populates the source/target dropdowns based on allowed roles."""
        # Ensure self.available_servers is a dictionary, as expected by subsequent logic.
        # load_servers() should return a dictionary. If it could return None or other types, handle here.
        loaded_servers_data = load_servers()
        if isinstance(loaded_servers_data, dict):
            self.available_servers = loaded_servers_data
        else:
            print(f"Warning: load_servers() returned type {type(loaded_servers_data)}, expected dict. Using empty dict.")
            self.available_servers = {}


        source_servers = ["<Select Server>"] + sorted([
            name for name, details in self.available_servers.items()
            if 'source' in details.get('allowed_roles', [])
        ])
        target_servers = ["<Select Server>"] + sorted([
            name for name, details in self.available_servers.items()
            if 'target' in details.get('allowed_roles', [])
        ])

        self.source_server_combo.blockSignals(True)
        self.target_server_combo.blockSignals(True)
        self.source_server_combo.clear()
        self.source_server_combo.addItems(source_servers)
        self.target_server_combo.clear()
        self.target_server_combo.addItems(target_servers)
        self.source_server_combo.blockSignals(False)
        self.target_server_combo.blockSignals(False)

    def _set_server_dropdown_selection(self, settings_key, combo_box):
        """Sets the combo box selection based on the value in current_config."""
        saved_server_name = self.current_config.get(settings_key)
        self._set_combo_box_text(combo_box, saved_server_name)


    def _set_combo_box_text(self, combo_box, text_to_select):
        """Helper to set combo box current item by text, handling None/missing."""
        if text_to_select and text_to_select != "<Select Server>":
            index = combo_box.findText(text_to_select, Qt.MatchFlag.MatchFixedString)
            if index >= 0:
                combo_box.setCurrentIndex(index)
            else:
                print(f"Warning: Saved server '{text_to_select}' not found or not allowed for this role. Setting dropdown to <Select Server>.")
                combo_box.setCurrentIndex(0)
        else:
            combo_box.setCurrentIndex(0)

    def toggle_advanced_tab(self):
        if not self.advanced_tab_visible:
            self.tab_widget.addTab(self.advanced_tab_widget, "Advanced")
            self.advanced_tab_visible = True
            self.tab_widget.setCurrentWidget(self.advanced_tab_widget)
            # Load current values from config
            self.mongo_conn_string_edit.setText(self.current_config.get('target_mongo_connection_string', ''))
            self.use_mongo_checkbox.setChecked(self.current_config.get('use_mongo_for_target_dashboards', False))
            self.enable_runtime_analytics_checkbox.setChecked(self.current_config.get('enable_runtime_analytics', False))
        else:
            idx = self.tab_widget.indexOf(self.advanced_tab_widget)
            if idx != -1:
                self.tab_widget.removeTab(idx)
            self.advanced_tab_visible = False

    def export_settings(self):
        # Open a file dialog to select export destination
        default_name = os.path.basename(self.settings_file_path)
        dest_path, _ = QFileDialog.getSaveFileName(self, "Export Settings File", default_name, "YAML Files (*.yaml *.yml);;All Files (*)")
        if dest_path:
            try:
                shutil.copy2(self.settings_file_path, dest_path)
                QMessageBox.information(self, "Export Successful", f"Settings file exported to:\n{dest_path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Failed", f"Failed to export settings file:\n{e}")