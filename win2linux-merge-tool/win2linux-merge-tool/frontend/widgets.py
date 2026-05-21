# frontend/widgets.py

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
    QSizePolicy, QScrollArea # Import QScrollArea
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QCursor

# --- Custom Key-Value Editor Widget (for simple dicts) ---
class KeyValueEditorWidget(QWidget):
    """A widget for editing simple key-value pairs (like string->string dictionaries)."""
    def __init__(self, initial_data=None, key_label="Key", value_label="Value", parent=None): # Changed default labels
        super().__init__(parent)
        self.key_label_text = key_label
        self.value_label_text = value_label

        # REMOVED self.setMinimumWidth(450)

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0) # No margins for the main container
        self.main_layout.setSpacing(5)

        # --- Scroll Area Setup ---
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True) # Allows the inner widget to resize
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded) # Horizontal scroll if needed (better default)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded) # Vertical scroll as needed
        self.scroll_area.setFrameShape(QScrollArea.Shape.NoFrame) # Remove scroll area border

        # Widget to contain the rows within the scroll area
        self.rows_container_widget = QWidget()
        self.rows_layout = QVBoxLayout(self.rows_container_widget) # Layout for the container
        self.rows_layout.setSpacing(3)
        self.rows_layout.addStretch(1) # Add stretch to push rows to the top within the container

        self.scroll_area.setWidget(self.rows_container_widget)
        # --- End Scroll Area Setup ---

        # Add scroll area to the main layout
        self.main_layout.addWidget(self.scroll_area)

        # Add button (outside the scroll area)
        self.add_button = QPushButton(f"Add Mapping") # Generic add button
        self.add_button.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.add_button.clicked.connect(lambda: self._add_row()) # Use lambda
        self.add_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # Set cursor
        self.main_layout.addWidget(self.add_button, alignment=Qt.AlignmentFlag.AlignLeft)

        # Set a reasonable minimum height for the scroll area (for vertical scrolling)
        self.scroll_area.setMinimumHeight(100) # Adjust as needed

        # Populate with initial data if provided
        self.setData(initial_data)

    def setData(self, data):
        """Clears existing rows and populates with new data."""
        # Clear existing rows first (iterate backwards for safe removal)
        for i in reversed(range(self.rows_layout.count())):
            item = self.rows_layout.itemAt(i)
            if item and item.widget():
                 item.widget().deleteLater()
            elif item and item.spacerItem(): # Also remove the stretch item
                 self.rows_layout.removeItem(item)


        # Populate with new data
        if isinstance(data, dict):
            for key, value in data.items():
                self._add_row(key, value)

        # Re-add stretch item at the end
        self.rows_layout.addStretch(1)


    def _add_row(self, key="", value=""):
        """Adds a new row for editing a key-value pair."""
        row_widget = QWidget() # Use a widget container for easier removal
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(5)

        key_edit = QLineEdit(str(key))
        key_edit.setPlaceholderText(self.key_label_text)
        # *** Use Expanding size policy for LineEdits ***
        key_edit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        value_edit = QLineEdit(str(value))
        value_edit.setPlaceholderText(self.value_label_text)
        value_edit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        remove_button = QPushButton("Remove")
        remove_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed) # Don't let button expand
        remove_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # Set cursor

        row_layout.addWidget(QLabel(f"{self.key_label_text}:"))
        row_layout.addWidget(key_edit) # No stretch factor needed with SizePolicy
        row_layout.addWidget(QLabel(f"{self.value_label_text}:"))
        row_layout.addWidget(value_edit) # No stretch factor needed with SizePolicy
        row_layout.addWidget(remove_button)

        # Store references for data retrieval
        row_widget.setProperty("key_edit", key_edit)
        row_widget.setProperty("value_edit", value_edit)

        # Connect the remove button's clicked signal to a lambda
        remove_button.clicked.connect(lambda: self._remove_row(row_widget))

        # Insert the new row widget into the rows_layout *before* the stretch item
        self.rows_layout.insertWidget(self.rows_layout.count() - 1, row_widget)

    def _remove_row(self, row_widget):
        """Removes the specified row widget."""
        self.rows_layout.removeWidget(row_widget)
        row_widget.deleteLater() # Ensure the widget is properly deleted

    def get_data(self):
        """Retrieves the current key-value pairs as a dictionary."""
        data = {}
        # Iterate through items in rows_layout, skipping the stretch item at the end
        for i in range(self.rows_layout.count() - 1):
            item = self.rows_layout.itemAt(i)
            if item and item.widget():
                row_widget = item.widget()
                key_edit = row_widget.property("key_edit")
                value_edit = row_widget.property("value_edit")
                if key_edit and value_edit:
                    key = key_edit.text().strip()
                    value = value_edit.text().strip()
                    if key: # Only add if key is not empty
                        data[key] = value
        return data

# --- Custom List-of-Dicts Editor Widget ---
class ListOfDictsEditorWidget(QWidget):
    """A widget for editing a list of dictionaries with predefined keys."""
    def __init__(self, initial_data=None, field_config=None, add_button_text="Add Item", parent=None):
        super().__init__(parent)
        # field_config should be a dict like {'internal_key1': 'UI Label 1', 'internal_key2': 'UI Label 2'}
        self.field_config = field_config if field_config else {}
        # Store the internal keys in a fixed order for consistent UI generation
        self.internal_keys = list(self.field_config.keys())

        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(5)

        # Layout to hold the rows of dictionaries
        self.rows_layout = QVBoxLayout()
        self.rows_layout.setSpacing(3)
        self.main_layout.addLayout(self.rows_layout)

        # Add button
        self.add_button = QPushButton(add_button_text)
        self.add_button.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.add_button.clicked.connect(lambda: self._add_row()) # Use lambda
        self.add_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # Set cursor
        self.main_layout.addWidget(self.add_button, alignment=Qt.AlignmentFlag.AlignLeft)

        self.main_layout.addStretch(1) # Push rows and button to the top

        # Populate with initial data if provided
        self.setData(initial_data)

    def setData(self, data):
        """Clears existing rows and populates with new list data."""
        # Clear existing rows first
        while self.rows_layout.count():
            item = self.rows_layout.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()

        # Populate with new data
        if isinstance(data, list):
            for item_dict in data:
                if isinstance(item_dict, dict):
                    # Pass the dictionary for the current item to _add_row
                    self._add_row(item_data=item_dict)
                else:
                    print(f"Warning: Item in list is not a dictionary: {item_dict}")
        elif data is not None: # Handle cases where data might be something other than a list (e.g., None, string)
             print(f"Warning: Initial data for ListOfDictsEditorWidget is not a list: {type(data)}. Initializing empty.")


    def _add_row(self, item_data=None):
        """Adds a new row for editing a dictionary item.
           item_data is a dictionary containing the data for the row, or None for a new empty row."""
        if item_data is None:
            item_data = {} # Ensure item_data is a dict for .get() calls

        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        row_layout.setContentsMargins(0, 0, 0, 0)
        row_layout.setSpacing(5)

        editors = {} # Store references to line edits for this row
        # Create labels and line edits based on self.internal_keys and self.field_config
        for internal_key in self.internal_keys:
            ui_label = self.field_config.get(internal_key, internal_key) # Use internal key as fallback label
            # Get initial value from item_data for this specific internal_key
            initial_value = item_data.get(internal_key, "") # Default to empty string if key not in item_data
            line_edit = QLineEdit(str(initial_value))
            line_edit.setPlaceholderText(ui_label)
            # *** Use Expanding size policy for LineEdits ***
            line_edit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

            row_layout.addWidget(QLabel(f"{ui_label}:"))
            row_layout.addWidget(line_edit) # No stretch factor needed
            editors[internal_key] = line_edit # Store editor reference using the internal key

        remove_button = QPushButton("Remove")
        remove_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed) # Don't let button expand
        remove_button.setCursor(QCursor(Qt.CursorShape.PointingHandCursor)) # Set cursor
        row_layout.addWidget(remove_button)

        # Store editor references in the row widget itself for easy retrieval
        row_widget.setProperty("editors", editors)

        # Connect the remove button
        remove_button.clicked.connect(lambda: self._remove_row(row_widget))

        # Add the row to the layout
        self.rows_layout.addWidget(row_widget)

    def _remove_row(self, row_widget):
        """Removes the specified row widget."""
        self.rows_layout.removeWidget(row_widget)
        row_widget.deleteLater()

    def get_data(self):
        """Retrieves the current data as a list of dictionaries."""
        data_list = []
        for i in range(self.rows_layout.count()):
            item = self.rows_layout.itemAt(i)
            if item and item.widget():
                row_widget = item.widget()
                editors = row_widget.property("editors") # Retrieve the dict of editors
                if isinstance(editors, dict):
                    item_dict = {}
                    is_valid_item = False # Only add if at least one field has value
                    # Iterate through the expected internal keys to build the dict
                    for internal_key in self.internal_keys:
                        line_edit = editors.get(internal_key)
                        if line_edit:
                            value = line_edit.text().strip()
                            item_dict[internal_key] = value
                            if value: # Check if any value was entered in this row
                                 is_valid_item = True
                        else: # Should not happen if _add_row is correct
                             item_dict[internal_key] = ""

                    if is_valid_item: # Only add rows where at least one field is non-empty
                        data_list.append(item_dict)
        return data_list
