# frontend/app.py
import multiprocessing
import sys
import os
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QIcon
import uuid
from flask import Flask, request, jsonify

# Relative imports (These stay the same)
from frontend.config import STYLESHEET
from frontend.main_window import SettingsApp

# Function to launch the application
def launch(icon_resource_path=None): # Added optional argument for icon path
    """Initializes and runs the PySide application."""
    app = QApplication(sys.argv)

    # --- Set Application Icon ---
    if icon_resource_path and os.path.exists(icon_resource_path):
        app.setWindowIcon(QIcon(icon_resource_path))
        print(f"Debug: Using icon path: {icon_resource_path}") # Added debug print
    else:
        print(f"Warning: Icon file not found or path not provided ('{icon_resource_path}'). Using default icon.")
    # --------------------------

    # Apply the stylesheet to the entire application
    app.setStyleSheet(STYLESHEET)

    # Create and show the main window
    window = SettingsApp()
    window.show()

    # Start the application event loop
    sys.exit(app.exec())

app = Flask(__name__)

@app.route('/start_migration', methods=['POST'])
def start_migration():
    # Generate a unique job_id for this migration
    job_id = str(uuid.uuid4())
    # Extract migration parameters from request if needed
    # params = request.json or {}
    # Start the migration in a background thread or async task
    from sisense_migration_and_merge_tool import run_parallel_migrations
    import threading
    import asyncio
    # Example: you may need to adapt arguments to your actual migration function
    def run_migration():
        # Replace with actual arguments for your migration
        dash_list = []  # Populate with actual dashboards
        folders_map = {}
        dashboard_oid_map = {}
        concurrency_limit = 5
        asyncio.run(run_parallel_migrations(dash_list, folders_map, dashboard_oid_map, concurrency_limit, job_id=job_id))
    threading.Thread(target=run_migration, daemon=True).start()
    return jsonify({'job_id': job_id})

