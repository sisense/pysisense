# frontend/dialogs.py

import os
import yaml
import shutil  # Added for file operations
from datetime import datetime  # Added for timestamping backups
import traceback  # Added for detailed error logging

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QPlainTextEdit, QDialogButtonBox, QSplitter,
    QWidget, QLabel, QListWidget, QListWidgetItem, QGroupBox, QFormLayout,
    QLineEdit, QComboBox, QCheckBox, QHBoxLayout, QPushButton, QMessageBox,
    QFileDialog, QApplication, QAbstractItemView, QStyle, QToolButton
)
from PySide6.QtCore import Qt, Slot, Signal, QSize  # QAbstractItemView removed from here, QSize added
from PySide6.QtGui import QFont, QCursor, QIcon, QPixmap
import PySide6.QtSvg  # Ensure SVG plugin is loaded

from .utils import load_servers, save_servers  # Use relative import

# --- Placeholder for config values if not available from a central config ---
# These would ideally be imported from a config file or passed appropriately.
try:
    # Attempt to import from a potential central config location if your project has one
    from config_loader import SERVERS_FILE as APP_SERVERS_FILE, APP_NAME as CURRENT_APP_NAME
    from .config import BACKUP_SUFFIX as APP_BACKUP_SUFFIX
except ImportError:
    # Fallbacks if not found
    APP_SERVERS_FILE = "servers.yaml"
    CURRENT_APP_NAME = "YourAppName"  # Replace if you have a specific app name
    APP_BACKUP_SUFFIX = ".backup"
    print("Warning: Using fallback SERVERS_FILE, APP_NAME, or BACKUP_SUFFIX in dialogs.py")


def get_user_config_path_dialogs(filename, app_name):
    """Helper to get user-specific config path, used locally in this dialog if main one isn't available."""
    # This is a simplified version. Ideally, use the one from your utils.utils if possible.
    user_config_dir = os.path.join(os.path.expanduser("~"), f".{app_name.lower()}")
    os.makedirs(user_config_dir, exist_ok=True)
    return os.path.join(user_config_dir, filename)


# --- YAML Preview Dialog ---
class YamlPreviewDialog(QDialog):
    """Dialog for previewing YAML content."""

    def __init__(self, yaml_string, parent=None):
        super().__init__(parent)
        self.setWindowTitle("YAML Preview")
        self.setMinimumSize(600, 500)

        layout = QVBoxLayout(self)
        text_edit = QPlainTextEdit()
        text_edit.setPlainText(yaml_string)
        text_edit.setReadOnly(True)
        text_edit.setFont(QFont("Courier New", 10))
        text_edit.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        layout.addWidget(text_edit)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        button_box.accepted.connect(self.accept)
        layout.addWidget(button_box)
        self.setLayout(layout)


# --- Server Editor Dialog ---
class ServerEditorDialog(QDialog):
    """Dialog for managing individual server connection details."""
    servers_updated = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Server Editor")
        self.setMinimumSize(700, 500)

        # Determine file paths using the helper or imported function
        try:
            from utils.utils import get_user_config_path as app_get_user_config_path
            self.servers_file_path = app_get_user_config_path(APP_SERVERS_FILE, CURRENT_APP_NAME)
        except ImportError:
            self.servers_file_path = get_user_config_path_dialogs(APP_SERVERS_FILE, CURRENT_APP_NAME)

        self.servers_dir_path = os.path.dirname(self.servers_file_path)

        self.servers = load_servers()

        main_layout = QVBoxLayout(self)
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        # Helper to load white SVG icons
        def load_svg_icon(filename):
            path = os.path.join(os.path.dirname(__file__), '../assets', filename)
            print("Loading SVG icon:", path)
            return QIcon(path)
        # Helper to load icons using absolute path and print debug info
        def load_png_icon_absolute(filename):
            path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../assets', filename))
            print(f"[DEBUG] Loading icon: {path} Exists: {os.path.exists(path)}")
            return QIcon(path)
        # --- Top row: Servers label + icon buttons ---
        top_row = QHBoxLayout()  # Restore to horizontal layout
        server_list_label = QLabel("Servers:")
        top_row.addWidget(server_list_label)
        top_row.addStretch()
        # Add new icon (first)
        self.add_new_icon = QToolButton()
        self.add_new_icon.setText("+")
        self.add_new_icon.setToolTip("Add New Server")
        self.add_new_icon.setFixedSize(28, 28)
        self.add_new_icon.setStyleSheet("QToolButton { border-radius: 14px; background-color: #94F5F0; font-size: 16px; font-weight: bold; color: white; } QToolButton:hover { background-color: #1DE4EB; color: white; }")
        self.add_new_icon.setIconSize(QSize(16, 16))
        self.add_new_icon.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.add_new_icon.setIcon(QIcon())
        top_row.addWidget(self.add_new_icon)
        # Upload icon (second)
        self.upload_icon = QToolButton()
        self.upload_icon.setText("↥")
        self.upload_icon.setToolTip("Upload Servers File")
        self.upload_icon.setFixedSize(28, 28)
        self.upload_icon.setStyleSheet("QToolButton { border-radius: 14px; background-color: #94F5F0; font-size: 16px; font-weight: bold; color: white; } QToolButton:hover { background-color: #1DE4EB; color: white; }")
        self.upload_icon.setIconSize(QSize(16, 16))
        self.upload_icon.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.upload_icon.setIcon(QIcon())
        top_row.addWidget(self.upload_icon)
        # Export icon (third)
        self.export_icon = QToolButton()
        self.export_icon.setText("↧")
        self.export_icon.setToolTip("Export Servers File")
        self.export_icon.setFixedSize(28, 28)
        self.export_icon.setStyleSheet("QToolButton { border-radius: 14px; background-color: #94F5F0; font-size: 16px; font-weight: bold; color: white; } QToolButton:hover { background-color: #1DE4EB; color: white; }")
        self.export_icon.setIconSize(QSize(16, 16))
        self.export_icon.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonIconOnly)
        self.export_icon.setIcon(QIcon())
        top_row.addWidget(self.export_icon)
        left_layout.addLayout(top_row)
        self.server_list_widget = QListWidget()
        self.server_list_widget.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.server_list_widget.currentItemChanged.connect(self._on_server_selected)
        left_layout.addWidget(self.server_list_widget)
        splitter.addWidget(left_widget)

        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        details_group = QGroupBox("Server Details")
        right_layout.addWidget(details_group)
        details_layout = QFormLayout(details_group)

        self.server_name_edit = QLineEdit()
        self.host_edit = QLineEdit()
        self.port_edit = QLineEdit()
        self.protocol_combo = QComboBox()
        self.protocol_combo.addItems(['https', 'http'])
        self.os_combo = QComboBox()
        self.os_combo.addItems(['Windows', 'Linux'])  # Assuming these are the relevant OS options
        self.api_token_edit = QLineEdit()
        self.api_token_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.server_name_edit.setMinimumWidth(250)
        self.host_edit.setMinimumWidth(250)

        self.allow_source_checkbox = QCheckBox("Allow as Source Role")
        self.allow_target_checkbox = QCheckBox("Allow as Target Role")
        roles_layout = QHBoxLayout()
        roles_layout.addWidget(self.allow_source_checkbox)
        roles_layout.addWidget(self.allow_target_checkbox)
        roles_layout.addStretch()

        details_layout.addRow("Server Name:", self.server_name_edit)
        details_layout.addRow("Host:", self.host_edit)
        details_layout.addRow("Port:", self.port_edit)
        details_layout.addRow("Protocol:", self.protocol_combo)
        details_layout.addRow("OS:", self.os_combo)
        details_layout.addRow("API Token:", self.api_token_edit)
        details_layout.addRow("Allowed Roles:", roles_layout)

        right_layout.addStretch()
        splitter.addWidget(right_widget)
        splitter.setSizes([200, 500])

        button_layout = QHBoxLayout()
        self.save_button = QPushButton("Save Server")
        self.delete_button = QPushButton("Delete Server")
        self.close_button = QPushButton("Close")
        self.save_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.delete_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        self.close_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        button_layout.addWidget(self.save_button)
        self.save_button.setDefault(True)
        button_layout.addWidget(self.delete_button)
        button_layout.addStretch()
        button_layout.addWidget(self.close_button)
        main_layout.addLayout(button_layout)

        self.add_new_icon.clicked.connect(self._add_new_server)
        self.upload_icon.clicked.connect(self._upload_servers_file)
        self.export_icon.clicked.connect(self._export_servers_file)
        self.save_button.clicked.connect(self._save_server)
        self.delete_button.clicked.connect(self._delete_server)
        self.close_button.clicked.connect(self.accept)

        self._populate_server_list()
        # self._clear_detail_fields() # Clear fields initially
        self._update_button_states()  # Update button states based on list

        # Automatically select and display the first server if the list is not empty
        if self.server_list_widget.count() > 0:
            self.server_list_widget.setCurrentRow(0)
            # _on_server_selected will be called due to currentItemChanged signal,
            # which will then populate the detail fields.
        else:
            # If list is empty, ensure fields are cleared (though _populate_server_list might leave them if it returns early)
            self._clear_detail_fields()

        # --- Track original server entry for change detection ---
        self._original_server_entry = None

        # Connect change signals for all fields
        self.server_name_edit.textChanged.connect(self._on_server_field_changed)
        self.host_edit.textChanged.connect(self._on_server_field_changed)
        self.port_edit.textChanged.connect(self._on_server_field_changed)
        self.protocol_combo.currentIndexChanged.connect(self._on_server_field_changed)
        self.os_combo.currentIndexChanged.connect(self._on_server_field_changed)
        self.api_token_edit.textChanged.connect(self._on_server_field_changed)
        self.allow_source_checkbox.stateChanged.connect(self._on_server_field_changed)
        self.allow_target_checkbox.stateChanged.connect(self._on_server_field_changed)

    def _populate_server_list(self):
        """Updates the QListWidget with current server names from self.servers."""
        current_selection_text = None
        if self.server_list_widget.currentItem():
            current_selection_text = self.server_list_widget.currentItem().text()

        self.server_list_widget.clear()

        server_names_to_display = []
        if isinstance(self.servers, list):
            server_names_to_display = [s.get('name') for s in self.servers if s.get('name')]
        elif isinstance(self.servers, dict):
            server_names_to_display = sorted(self.servers.keys())
        else:
            print("Warning: self.servers is not a list or dict in _populate_server_list.")
            # If self.servers is not what's expected (e.g. None after a failed load_servers),
            # ensure the list is empty and buttons are updated.
            self._update_button_states()
            return

        selected_item_to_restore = None
        for name in server_names_to_display:
            item = QListWidgetItem(name)
            self.server_list_widget.addItem(item)
            if name == current_selection_text:
                selected_item_to_restore = item

        if selected_item_to_restore:
            self.server_list_widget.setCurrentItem(selected_item_to_restore)
        # Do not automatically select first item here; __init__ will handle initial selection.

    def _on_server_selected(self, current_item, previous_item):
        """Handles selection change in the server list."""
        self._update_button_states()
        if current_item:
            server_name = current_item.text()
            server_data = {}
            if isinstance(self.servers, list):
                for s in self.servers:
                    if s.get('name') == server_name:
                        server_data = s
                        break
            elif isinstance(self.servers, dict):
                server_data = self.servers.get(server_name, {})

            self._populate_detail_fields(server_name, server_data)
            # Store a snapshot of the original entry for change detection
            self._original_server_entry = self._get_current_server_entry()
            self._on_server_field_changed()  # Update save button state
        else:
            # If selection is cleared (e.g., by _add_new_server), clear the fields.
            self._clear_detail_fields()
            self._original_server_entry = self._get_current_server_entry()
            self._on_server_field_changed()  # Update save button state

    def _populate_detail_fields(self, name, data):
        """Fills the detail fields with data from the selected server."""
        self.server_name_edit.setText(name)
        self.host_edit.setText(data.get('host', ''))
        self.port_edit.setText(str(data.get('port', '')))
        self.protocol_combo.setCurrentText(data.get('protocol', 'https'))
        self.os_combo.setCurrentText(data.get('os', 'Windows'))
        self.api_token_edit.setText(data.get('api_token', ''))

        allowed_roles = data.get('allowed_roles', ['source', 'target'])
        self.allow_source_checkbox.setChecked('source' in allowed_roles)
        self.allow_target_checkbox.setChecked('target' in allowed_roles)

    def _clear_detail_fields(self):
        """Clears all server detail fields."""
        self.server_name_edit.clear()
        self.host_edit.clear()
        self.port_edit.clear()
        self.protocol_combo.setCurrentIndex(0)
        self.os_combo.setCurrentIndex(0)
        self.api_token_edit.clear()
        self.allow_source_checkbox.setChecked(True)
        self.allow_target_checkbox.setChecked(True)

    def _update_button_states(self):
        """Enables/disables buttons based on selection."""
        has_selection = self.server_list_widget.currentItem() is not None
        self.delete_button.setEnabled(has_selection)
        # Save button is generally always enabled for adding new or updating selected
        # Add new button is always enabled

    def _get_current_server_entry(self):
        """Returns a dict of the current server entry fields for change detection."""
        return {
            'name': self.server_name_edit.text().strip(),
            'host': self.host_edit.text().strip(),
            'port': self.port_edit.text().strip(),
            'protocol': self.protocol_combo.currentText(),
            'os': self.os_combo.currentText(),
            'api_token': self.api_token_edit.text(),
            'allowed_roles': [role for role, cb in zip(['source', 'target'], [self.allow_source_checkbox, self.allow_target_checkbox]) if cb.isChecked()]
        }

    def _on_server_field_changed(self):
        """Enables the save button only if any field has changed from the original."""
        current = self._get_current_server_entry()
        orig = self._original_server_entry or {}
        changed = current != orig
        self.save_button.setEnabled(changed)

    @Slot()
    def _add_new_server(self):
        """Clears the fields and deselects list item to prepare for adding."""
        self.server_list_widget.setCurrentRow(-1)
        self.server_name_edit.setFocus()
        self._original_server_entry = self._get_current_server_entry()
        self._on_server_field_changed()  # Update save button state

    @Slot()
    def _save_server(self):
        """Saves the current details as a new server or updates an existing one."""
        server_name = self.server_name_edit.text().strip()
        if not server_name:
            QMessageBox.warning(self, "Input Error", "Server Name cannot be empty.")
            return

        allowed_roles = []
        if self.allow_source_checkbox.isChecked():
            allowed_roles.append('source')
        if self.allow_target_checkbox.isChecked():
            allowed_roles.append('target')

        if not allowed_roles:
            QMessageBox.warning(self, "Input Error", "Server must be allowed at least one role (Source or Target).")
            return

        server_data_entry = {
            'name': server_name,
            'host': self.host_edit.text().strip(),
            'port': self.port_edit.text().strip(),
            'protocol': self.protocol_combo.currentText(),
            'os': self.os_combo.currentText(),
            'api_token': self.api_token_edit.text(),
            'allowed_roles': allowed_roles
        }

        port_str = server_data_entry['port']
        port_val = None
        if port_str:
            try:
                port_val = int(port_str)
                if not (0 < port_val < 65536):
                    raise ValueError("Port out of range")
            except ValueError:
                QMessageBox.warning(self, "Input Error",
                                    f"Invalid port number: {port_str}. Must be between 1 and 65535.")
                return
        server_data_entry['port'] = port_val

        is_update = False
        if isinstance(self.servers, list):
            existing_server_index = -1
            for i, s in enumerate(self.servers):
                if s.get('name') == server_name:
                    existing_server_index = i
                    is_update = True
                    break
            if is_update:
                self.servers[existing_server_index] = server_data_entry
            else:
                self.servers.append(server_data_entry)
        elif isinstance(self.servers, dict):
            is_update = server_name in self.servers
            self.servers[server_name] = server_data_entry
        else:
            QMessageBox.critical(self, "Internal Error", "Server data is not in a recognized format (list or dict).")
            return

        action_text = "Updated" if is_update else "Added"

        if save_servers(self.servers):
            QMessageBox.information(self, "Success", f"Server '{server_name}' {action_text.lower()} successfully.")
            self.servers_updated.emit()
            self._populate_server_list()  # This will re-populate and try to maintain selection
            items = self.server_list_widget.findItems(server_name, Qt.MatchFlag.MatchExactly)
            if items:
                self.server_list_widget.setCurrentItem(items[0])
            self._original_server_entry = self._get_current_server_entry()
            self._on_server_field_changed()  # Reset save button state
            self._update_button_states()

    @Slot()
    def _delete_server(self):
        """Deletes the selected server."""
        current_item = self.server_list_widget.currentItem()
        if not current_item:
            return

        server_name = current_item.text()
        reply = QMessageBox.question(self, "Confirm Delete",
                                     f"Are you sure you want to delete the server '{server_name}'?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:
            deleted_from_memory = False
            if isinstance(self.servers, list):
                # Find the server to delete
                original_length = len(self.servers)
                self.servers = [s for s in self.servers if s.get('name') != server_name]
                if len(self.servers) < original_length:
                    deleted_from_memory = True
            elif isinstance(self.servers, dict):
                if server_name in self.servers:
                    del self.servers[server_name]
                    deleted_from_memory = True

            if deleted_from_memory:
                if save_servers(self.servers):
                    QMessageBox.information(self, "Success", f"Server '{server_name}' deleted.")
                    self.servers_updated.emit()
                    self._populate_server_list()  # Repopulate list
                    # After repopulating, if list is not empty, select first item, else clear fields
                    if self.server_list_widget.count() > 0:
                        self.server_list_widget.setCurrentRow(0)
                    else:
                        self._clear_detail_fields()
                    self._update_button_states()
                else:
                    # If save failed, it's crucial to revert self.servers to avoid inconsistency
                    self.servers = load_servers()
                    self._populate_server_list()
            else:
                QMessageBox.warning(self, "Error",
                                    f"Server '{server_name}' not found for deletion (this might indicate an issue).")

    @Slot()
    def _upload_servers_file(self):
        """Opens a file dialog to upload a servers.yaml file."""
        file_dialog = QFileDialog(self)
        file_dialog.setNameFilter("YAML files (*.yaml *.yml)")
        file_dialog.setWindowTitle(f"Upload {os.path.basename(self.servers_file_path)}")
        file_dialog.setFileMode(QFileDialog.FileMode.ExistingFile)

        if file_dialog.exec():
            selected_files = file_dialog.selectedFiles()
            if selected_files:
                source_file_path = selected_files[0]

                reply = QMessageBox.question(self, "Confirm Upload",
                                             f"This will replace your current '{os.path.basename(self.servers_file_path)}' with the selected file.\n\n'{os.path.basename(source_file_path)}' -> '{os.path.basename(self.servers_file_path)}'\n\nA backup of the current server settings will be made.\n\nProceed?",
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                             QMessageBox.StandardButton.No)

                if reply == QMessageBox.StandardButton.Yes:
                    try:
                        if os.path.exists(self.servers_file_path):
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            backup_filename = f"{self.servers_file_path}.upload_backup_{timestamp}{APP_BACKUP_SUFFIX}"
                            shutil.copy2(self.servers_file_path, backup_filename)
                            print(
                                f"Backup of current '{os.path.basename(self.servers_file_path)}' created: {os.path.basename(backup_filename)}")

                        shutil.copy2(source_file_path, self.servers_file_path)
                        QApplication.processEvents()

                        self.servers = load_servers()
                        self._populate_server_list()
                        # After populating, if list is not empty, select first item, else clear fields
                        if self.server_list_widget.count() > 0:
                            self.server_list_widget.setCurrentRow(0)
                        else:
                            self._clear_detail_fields()  # Ensure fields are clear if new file is empty/invalid

                        self._update_button_states()
                        self.servers_updated.emit()

                        QMessageBox.information(self, "Upload Successful",
                                                f"'{os.path.basename(source_file_path)}' was successfully uploaded and applied.\nServers have been reloaded.")

                    except FileNotFoundError:
                        QMessageBox.critical(self, "Upload Error",
                                             f"Error: The source file '{os.path.basename(source_file_path)}' was not found.")
                    except IOError as e:
                        QMessageBox.critical(self, "Upload Error", f"Error copying or reading file: {e}")
                    except Exception as e:
                        traceback.print_exc()
                        QMessageBox.critical(self, "Upload Error", f"An unexpected error occurred during upload: {e}")
                else:
                    QMessageBox.information(self, "Upload Cancelled", "Server file upload was cancelled.")
            else:
                QMessageBox.information(self, "Upload Info", "No file selected for upload.")

    def _export_servers_file(self):
        """Opens a file dialog to export the servers.yaml file."""
        file_dialog = QFileDialog(self)
        file_dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptSave)
        file_dialog.setNameFilter("YAML files (*.yaml *.yml)")
        file_dialog.setWindowTitle(f"Export {os.path.basename(self.servers_file_path)}")
        file_dialog.selectFile(os.path.basename(self.servers_file_path))
        if file_dialog.exec():
            selected_files = file_dialog.selectedFiles()
            if selected_files:
                dest_file_path = selected_files[0]
                try:
                    shutil.copy2(self.servers_file_path, dest_file_path)
                    QMessageBox.information(self, "Export Successful", f"Servers file exported to:\n{dest_file_path}")
                except Exception as e:
                    QMessageBox.critical(self, "Export Failed", f"Failed to export servers file:\n{e}")
