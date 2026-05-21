# server.py

import sys
import warnings

# Suppress pkg_resources deprecation warning (from setuptools/PyInstaller) so it does not appear in validation logs
warnings.filterwarnings("ignore", message=".*pkg_resources is deprecated.*", category=UserWarning)

from flask import Flask, jsonify, request, send_from_directory, abort, Response, stream_with_context, send_file
import os
import yaml

# Load .env from project root (AI API keys: OPENAI_API_KEY, GOOGLE_API_KEY)
try:
    from dotenv import load_dotenv
    _env_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(_env_dir, '.env'))
except ImportError:
    pass
import subprocess
import sys
import shutil
from datetime import datetime
import time
from utils.progress_reporter import ProgressReporter
import uuid
import threading
from queue import Queue, Empty
from utils.utils import get_user_config_path, fetch_all_dashboards
import multiprocessing
import argparse
import traceback
import json
import webbrowser
from waitress import serve
# Import the main function from your migration script
from sisense_migration_and_merge_tool import main as run_migration_logic

# --- Frozen app: support --run-validation-fetch and --run-validation-run so the bundle can run validation in subprocesses ---
if getattr(sys, "frozen", False):
    # Dashboard fetch: argv is [executable, --run-validation-fetch, config_path, result_path]
    try:
        idx = sys.argv.index("--run-validation-fetch")
    except ValueError:
        idx = -1
    if idx >= 0 and idx + 2 < len(sys.argv):
        import validation_fetch_standalone
        sys.argv = [sys.argv[0], sys.argv[idx + 1], sys.argv[idx + 2]]
        validation_fetch_standalone.main()
        sys.exit(0)

    # Validation run: argv is [executable, --run-validation-run, config_path, dashboards_path, progress_path, optional --include-jaql, --jaql-mode=..., --show-jaql-in-results]
    try:
        idx_run = sys.argv.index("--run-validation-run")
    except ValueError:
        idx_run = -1
    if idx_run >= 0 and idx_run + 3 <= len(sys.argv):
        config_path = sys.argv[idx_run + 1]
        dashboards_path = sys.argv[idx_run + 2]
        progress_path = sys.argv[idx_run + 3]
        extra = [a for a in sys.argv[idx_run + 4:] if a.startswith("--")]
        sys.argv = [sys.argv[0], config_path, dashboards_path, progress_path] + extra
        import validation_run_standalone
        validation_run_standalone.main()
        sys.exit(0)

# Application version
APP_VERSION = "6.0.1"


# --- Helper function to handle bundled paths ---
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # Not running as a bundled app, use the normal path
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


# --- Configuration ---
APP_NAME = "sisense_merge_and_migration"
SETTINGS_FILE = "settings.yaml"
SERVERS_FILE = "servers.yaml"

# Chat context limits to stay under model token caps (e.g. Gemini 1M). Sliding window + per-message cap.
CHAT_MAX_HISTORY_MESSAGES = 20
CHAT_MAX_MESSAGE_CHARS = 12000
# SETTINGS_TEMPLATE_FILE is no longer needed


# --- Globals for Process Management ---
running_process = None
migration_process = None

# --- Validation jobs (background validation with polling) ---
_validation_jobs = {}
_validation_jobs_lock = threading.Lock()

# Dashboard compare job: long-running compare runs in background; clients poll for result (avoids proxy/SDK timeouts).
_compare_dashboards_job = {'status': 'idle', 'result': None, 'error': None}
_compare_dashboards_lock = threading.Lock()


# --- Utility Functions ---
def get_config_path(filename):
    """Gets the absolute path for a configuration file."""
    # For user-specific, mutable files, use the user's home directory.
    if filename in [SETTINGS_FILE, SERVERS_FILE]:
        path = str(get_user_config_path(filename, APP_NAME))
        print(f"DEBUG: Using config path: {path}")
        return path

    # For any other case, assume it's a bundled resource.
    return resource_path(filename)


def _resolve_verify(server_config, request_verify=None):
    """Resolve SSL verify: request > server config > global settings.yaml > False (permissive for missing)."""
    if request_verify is not None:
        return bool(request_verify)
    if server_config and 'verify' in server_config:
        return bool(server_config['verify'])
    try:
        settings_path = get_config_path(SETTINGS_FILE)
        if os.path.exists(settings_path):
            with open(settings_path, 'r', encoding='utf-8') as f:
                global_settings = yaml.safe_load(f) or {}
            return bool(global_settings.get('verify', False))
    except Exception:
        pass
    return False


def get_default_settings():
    """Returns a dictionary of default settings for a new settings.yaml file."""
    return {
        'selected_source_server': None,
        'selected_target_server': None,
        'verify': False,
        'file_logLevel': 'INFO',
        'console_logLevel': 'INFO',
        'logFileName': 'migration.log',
        'override_logs': True,
        'write_migration_reports': True,
        'migrate_users': False,
        'migrate_groups': False,
        'migrate_folders': False,
        'migrate_dashboards': False,
        'migrate_datamodels': False,
        'migrate_datasecurity': False,
        'migrate_saved_formulas': True,
        'migrate_saved_filters': False,
        'folders': {
            'share_source_dashboards_with_migration_user': True,
            'update_target_folders_owner': True,
            'run_folder_migration_if_missing': False
        },
        'dashboard_share_with_migration_user': False,
        'dashboard_share_concurrency': 10,
        'dashboard_migration_mode': 'ALL',
        'dashboard_include_list': 'ALL',
        'exclude_dashboards_by_name': '',
        'exclude_dashboards_by_oid': '',
        'skip_dashboards_with_missing_ancestor_folder': True,
        'dashboard_import_mode': 'skip',
        'dashboard_migration_concurrency': 20,
        'wait_between_chunks': 5,
        'wait_chunk_size_threshold': 500,
        'dashboard_fetch_chunk_size': 1000,
        'use_custom_dashboard_oid': True,
        'oid_host_mapping': {},
        'update_connections': [],
        'include_datamodels': 'ALL',
        'exclude_datamodels': 'Usage Analytics Model',
        'include_datamodels_datasecurity': 'ALL',
        'exclude_datamodels_datasecurity': '',
        'enable_runtime_analytics': False,
        'target_mongo_connection_string': '',
        'use_mongo_for_target_dashboards': False,
        'datamodel_overwrite': False,
        'enable_update_connections': False,
        'auto_migrate_missing_custom_code_notebooks': False,
        'exact_match_in_dashboard_search': True,
        'migrate_blox_actions': False,
        'migrate_custom_code': False,
        'notebook_import_mode': 'skip',
        'notebook_include_list': 'ALL',
        'migrate_datasecurity_chunk_size': -1,
        'migrate_users_chunk_size': 1000,
        'new_password_for_migrated_users': None,
        'overwrite_existing_blox_actions': False,
        'post_import_update_connection_function': '',
        'skip_dashboards_with_missing_owner': True,
        'update_users_password': False,
        'ignore_custom_roles': False,
        'validate_dashboards_migration': False,
        # Optional: per-migration-item log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL, or omit to inherit global)
        'migration_log_levels': {},
    }


def _deep_merge(current, partial):
    """Deep merge partial into current. Nested dicts are merged recursively; lists and other values are replaced."""
    if not isinstance(partial, dict):
        return partial
    result = dict(current) if isinstance(current, dict) else {}
    for k, v in partial.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _get_migration_logs_dir():
    """Return the logs directory for migration (same base as _get_migration_log_path)."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, 'logs')


def _get_migration_log_path():
    """Return path to the legacy single migration log file from settings."""
    log_dir = _get_migration_logs_dir()
    settings_path = get_config_path(SETTINGS_FILE)
    log_file_name = 'migration.log'
    if os.path.exists(settings_path):
        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                s = yaml.safe_load(f)
                if s and isinstance(s.get('logFileName'), str):
                    log_file_name = s['logFileName']
        except Exception:
            pass
    return os.path.join(log_dir, log_file_name)


def _get_latest_migration_log_path():
    """Return path to the most recent per-run log (migration_YYYYMMDD_HHMMSS.log), or legacy single file if none."""
    log_dir = _get_migration_logs_dir()
    if not os.path.isdir(log_dir):
        return _get_migration_log_path()
    import glob
    pattern = os.path.join(log_dir, 'migration_*.log')
    run_logs = glob.glob(pattern)
    if not run_logs:
        return _get_migration_log_path()
    run_logs.sort(key=os.path.getmtime, reverse=True)
    return run_logs[0]


def _build_migration_subprocess():
    """Build and return the migration subprocess (Popen). Caller must manage stdout and migration_process global.
    Each run writes to a per-run log file (migration_YYYYMMDD_HHMMSS.log) via MIGRATION_LOG_FILE env."""
    settings_path = get_config_path(SETTINGS_FILE)
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        command = [sys.executable, '--run-migration', '--settings-file', settings_path]
    else:
        command = [sys.executable, __file__, '--run-migration', '--settings-file', settings_path]
    log_dir = _get_migration_logs_dir()
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError:
        pass
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    per_run_log_name = f'migration_{timestamp}.log'
    per_run_log_path = os.path.abspath(os.path.join(log_dir, per_run_log_name))
    proc_env = os.environ.copy()
    proc_env['PYTHONIOENCODING'] = 'utf-8'
    proc_env['MIGRATION_LOG_FILE'] = per_run_log_path
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=proc_env,
    )


def _load_settings_and_servers():
    """Load settings and servers from disk. Returns (settings_dict, servers_dict) or (None, None) on error."""
    settings_path = get_config_path(SETTINGS_FILE)
    servers_path = get_config_path(SERVERS_FILE)
    settings = None
    servers = None
    if os.path.exists(settings_path):
        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                settings = yaml.safe_load(f) or {}
        except Exception:
            pass
    if os.path.exists(servers_path):
        try:
            with open(servers_path, 'r', encoding='utf-8') as f:
                servers = yaml.safe_load(f) or {}
        except Exception:
            pass
    return settings or {}, servers or {}


def _get_api_client_for_server(server_name, servers_dict, timeout=60):
    """Build SisenseRestApiClient for the given server name. Returns (client, error_message)."""
    if not server_name or server_name not in (servers_dict or {}):
        return None, f'Server "{server_name}" not found'
    server_config = servers_dict[server_name]
    for key in ('host', 'port', 'protocol', 'os', 'api_token'):
        if key not in server_config:
            return None, f'Server "{server_name}" is missing required key: {key}'
    import logging
    from SisenseRESTAPIClientClass import SisenseRestApiClient
    logger = logging.getLogger(__name__)
    verify_ssl = _resolve_verify(server_config, None)
    client = SisenseRestApiClient(
        server_domain=server_config['host'],
        port=server_config['port'],
        protocol=server_config['protocol'],
        operating_system=server_config['os'],
        api_token=server_config['api_token'],
        verify=verify_ssl,
        timeout=timeout,
        logger=logger,
    )
    return client, None


# --- Flask App Initialization ---
static_folder_path = resource_path('frontend')
app = Flask(__name__, static_folder=static_folder_path, static_url_path='')
chat_dist_path = resource_path('frontend/chat/dist')


# --- API Endpoints ---

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/chat')
@app.route('/chat/')
def chat_index():
    """Serve the React chat app (built from frontend/chat). Run `npm run build` in frontend/chat first."""
    if not os.path.isdir(chat_dist_path):
        return (
            "<p>Chat app not built. Run in repo root: <code>cd frontend/chat && npm install && npm run build</code></p>",
            503,
            {"Content-Type": "text/html"},
        )
    return send_from_directory(chat_dist_path, "index.html")


@app.route('/chat/<path:filename>')
def chat_assets(filename):
    """Serve React chat app static assets (JS, CSS, etc.)."""
    if not os.path.isdir(chat_dist_path):
        abort(503, description="Chat app not built. Run: cd frontend/chat && npm run build")
    return send_from_directory(chat_dist_path, filename)


@app.route('/api/settings', methods=['GET'])
def get_settings():
    settings_path = get_config_path(SETTINGS_FILE)
    try:
        # If the settings file doesn't exist, create it with defaults.
        if not os.path.exists(settings_path):
            print(f"INFO: Settings file not found at {settings_path}. Creating with defaults.")
            default_settings = get_default_settings()
            with open(settings_path, 'w', encoding='utf-8') as f:
                yaml.dump(default_settings, f, default_flow_style=False, sort_keys=False, indent=2)
            return jsonify(default_settings)

        # If it exists, read and return it.
        with open(settings_path, 'r', encoding='utf-8') as f:
            settings = yaml.safe_load(f)
            return jsonify(settings or {})
    except Exception as e:
        # Log the full traceback to the console for debugging
        print(f"ERROR in get_settings: {traceback.format_exc()}")
        abort(500, description=f"Error loading or creating settings: {e}")


@app.route('/api/settings', methods=['POST'])
def save_settings():
    settings_path = get_config_path(SETTINGS_FILE)
    data = request.json
    if not data:
        abort(400, description="No data provided.")
    try:
        if os.path.exists(settings_path):
            backup_path = f"{settings_path}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            shutil.copy2(settings_path, backup_path)
        with open(settings_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, indent=2)
        return jsonify({"message": "Settings saved successfully."})
    except Exception as e:
        abort(500, description=f"Error saving settings: {e}")


@app.route('/api/settings', methods=['PATCH'])
def patch_settings():
    """Partial update: merge JSON body with current settings and save (deep merge)."""
    settings_path = get_config_path(SETTINGS_FILE)
    partial = request.get_json()
    if not partial or not isinstance(partial, dict):
        abort(400, description='JSON body must be a non-empty object.')
    try:
        if os.path.exists(settings_path):
            with open(settings_path, 'r', encoding='utf-8') as f:
                current = yaml.safe_load(f) or {}
        else:
            current = get_default_settings()
        merged = _deep_merge(current, partial)
        if os.path.exists(settings_path):
            backup_path = f"{settings_path}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            shutil.copy2(settings_path, backup_path)
        with open(settings_path, 'w', encoding='utf-8') as f:
            yaml.dump(merged, f, default_flow_style=False, sort_keys=False, indent=2)
        return jsonify({"message": "Settings updated successfully."})
    except Exception as e:
        abort(500, description=str(e))


@app.route('/api/settings/preview', methods=['POST'])
def settings_preview():
    """Plan-only: merge JSON body with current settings and return merged settings without saving."""
    partial = request.get_json()
    if partial is not None and not isinstance(partial, dict):
        abort(400, description='JSON body must be an object.')
    partial = partial or {}
    settings_path = get_config_path(SETTINGS_FILE)
    try:
        if os.path.exists(settings_path):
            with open(settings_path, 'r', encoding='utf-8') as f:
                current = yaml.safe_load(f) or {}
        else:
            current = get_default_settings()
        merged = _deep_merge(current, partial)
        return jsonify(merged)
    except Exception as e:
        abort(500, description=str(e))


@app.route('/api/export_settings', methods=['GET'])
def export_settings():
    settings_path = get_config_path(SETTINGS_FILE)
    if not os.path.exists(settings_path):
        abort(404, description="Settings file not found.")
    return send_file(settings_path, as_attachment=True, download_name=os.path.basename(settings_path))


@app.route('/api/servers', methods=['GET', 'POST'])
def handle_servers():
    servers_path = get_config_path(SERVERS_FILE)
    if request.method == 'GET':
        try:
            if not os.path.exists(servers_path):
                return jsonify({})
            with open(servers_path, 'r', encoding='utf-8') as f:
                return jsonify(yaml.safe_load(f) or {})
        except Exception as e:
            abort(500, description=f"Error loading servers: {e}")

    if request.method == 'POST':
        data = request.json
        if not data:
            abort(400, description="No data provided.")
        try:
            with open(servers_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=True, indent=2)
            return jsonify({"message": "Servers saved successfully."})
        except Exception as e:
            abort(500, description=f"Error saving servers: {e}")

    return abort(405, description="Method Not Allowed")


@app.route('/api/export_servers', methods=['GET'])
def export_servers():
    servers_path = get_config_path(SERVERS_FILE)
    if not os.path.exists(servers_path):
        abort(404, description="Servers file not found.")
    return send_file(servers_path, as_attachment=True, download_name=os.path.basename(servers_path))


@app.route('/api/upload/<filename>', methods=['POST'])
def upload_file(filename):
    if filename not in [SETTINGS_FILE, SERVERS_FILE]:
        abort(400, description="Invalid filename.")
    file = request.files.get('file')
    if not file:
        abort(400, description="No file part.")
    try:
        file_path = get_config_path(filename)
        if os.path.exists(file_path):
            backup_path = f"{file_path}.upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            shutil.copy2(file_path, backup_path)
        file.save(file_path)
        return jsonify({"message": f"'{filename}' uploaded successfully."})
    except Exception as e:
        abort(500, description=f"Error uploading file: {e}")


@app.route('/api/preview', methods=['POST'])
def get_preview():
    data = request.json
    if not data:
        abort(400, description="No data for preview.")
    try:
        yaml_string = yaml.dump(data, default_flow_style=False, sort_keys=False, indent=2)
        return jsonify({"preview": yaml_string})
    except Exception as e:
        abort(500, description=f"Error generating preview: {e}")


@app.route('/api/cancel', methods=['POST'])
def cancel_script():
    """Endpoint to cancel the running script."""
    global running_process
    if running_process and running_process.poll() is None:
        try:
            running_process.terminate()  # Send SIGTERM
            running_process = None
            return jsonify({"message": "Script cancellation requested."})
        except Exception as e:
            return jsonify({"error": f"Failed to terminate script: {e}"}), 500
    return jsonify({"message": "No script running."})


@app.route('/progress/<job_id>')
def progress_stream(job_id):
    def generate():
        last = (-1, -1)
        while True:
            current, total = ProgressReporter.get_progress(job_id)
            if (current, total) != last:
                yield f"data: {{\"current\": {current}, \"total\": {total}}}\n\n"
                last = (current, total)
            if total > 0 and current >= total:
                break
            time.sleep(0.2)

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


@app.route('/start_migration', methods=['POST'])
def start_migration():
    job_id = str(uuid.uuid4())

    # Start a demo migration in a background thread for demonstration
    def run_migration():
        total = 100
        with ProgressReporter(total, job_id=job_id, title='Demo Migration') as progress:
            for _ in range(total):
                time.sleep(0.05)
                progress.update()

    threading.Thread(target=run_migration, daemon=True).start()
    return jsonify({'job_id': job_id})


@app.route('/run_migration_stream')
def run_migration_stream():
    global migration_process
    if migration_process is not None and migration_process.poll() is None:
        return jsonify({'error': 'Migration already in progress.', 'status': 'conflict'}), 409

    def generate():
        global migration_process
        migration_process = _build_migration_subprocess()
        if migration_process.stdout is not None:
            for line in iter(migration_process.stdout.readline, ''):
                clean_line = line.strip()  # Strip leading/trailing whitespace

                if clean_line.startswith("MIGRATION_PROGRESS::"):
                    try:
                        # Extract the JSON part of the progress message
                        json_payload = clean_line.split("::", 1)[1]
                        # Yield a named 'progress' event to the client
                        yield f"event: progress\ndata: {json_payload}\n\n"
                    except (IndexError, json.JSONDecodeError):
                        # If parsing fails, yield it as a normal log line to not lose info
                        yield f"data: [SERVER_WARNING] Failed to parse progress line: {clean_line}\n\n"
                elif clean_line.startswith("SHARE_PROGRESS::"):
                    try:
                        # Extract the JSON part of the progress message
                        json_payload = clean_line.split("::", 1)[1]
                        # Yield a named 'progress' event to the client
                        yield f"event: share_progress\ndata: {json_payload}\n\n"
                    except (IndexError, json.JSONDecodeError):
                        # If parsing fails, yield it as a normal log line to not lose info
                        yield f"data: [SERVER_WARNING] Failed to parse progress line: {clean_line}\n\n"
                elif clean_line:  # Don't send empty lines
                    # Yield a standard, unnamed log message
                    yield f"data: {clean_line}\n\n"

            migration_process.stdout.close()
        migration_process.wait()
        migration_process = None

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/stop_migration', methods=['POST'])
def stop_migration():
    global migration_process
    if migration_process and migration_process.poll() is None:
        migration_process.terminate()
        return jsonify({'status': 'terminated'})
    return jsonify({'status': 'no_process'})


@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """Simple test connection endpoint"""
    try:
        data = request.get_json()
        server_name = data.get('serverName')
        temp_server = data.get('tempServer')
        
        if not server_name:
            return jsonify({'success': False, 'message': 'No server name provided'}), 400
        
        # If tempServer is provided (from server manager), use it directly
        if temp_server:
            server_config = temp_server
        else:
            # Load servers (always reload to get latest configuration including updated tokens)
            servers_path = get_config_path(SERVERS_FILE)
            if os.path.exists(servers_path):
                with open(servers_path, 'r', encoding='utf-8') as f:
                    servers = yaml.safe_load(f)
            else:
                servers = {}
            
            if server_name not in servers:
                return jsonify({'success': False, 'message': f'Server "{server_name}" not found'}), 404
            
            server_config = servers[server_name]
        
        verify_ssl = _resolve_verify(server_config, data.get('verify') if 'verify' in data else None)
        
        # Create API client for testing
        from SisenseRESTAPIClientClass import SisenseRestApiClient
        import logging
        
        logger = logging.getLogger(__name__)
        api_client = SisenseRestApiClient(
            server_domain=server_config['host'],
            port=server_config['port'],
            protocol=server_config['protocol'],
            operating_system=server_config['os'],
            api_token=server_config['api_token'],
            verify=verify_ssl,
            timeout=30,
            logger=logger
        )
        
        # Test the connection
        connection_success = api_client.test_http_connection()
        
        if not connection_success:
            return jsonify({'success': False, 'message': f'Connection to {server_name} failed'})
        
        # Test the API token by getting user info
        try:
            user_response = api_client.get_my_user()
            if user_response and isinstance(user_response, dict):
                # Check for various possible user name fields
                user_name = (user_response.get('userName') or 
                           user_response.get('name') or 
                           user_response.get('email') or 
                           user_response.get('id') or 
                           'Unknown')
                return jsonify({
                    'success': True, 
                    'message': f'Connection to {server_name} successful. User: {user_name}'
                })
            else:
                return jsonify({
                    'success': False, 
                    'message': f'Connection successful but API token invalid for {server_name}'
                })
        except Exception as token_error:
            # Check if it's an authentication error
            error_str = str(token_error)
            if '401' in error_str or 'Unauthorized' in error_str:
                return jsonify({
                    'success': False, 
                    'message': f'Connection successful but API token is invalid for {server_name}'
                })
            else:
                return jsonify({
                    'success': False, 
                    'message': f'Connection successful but API token test failed: {error_str}'
                })
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'Connection test error: {str(e)}'}), 500


def _api_users_list(server_role):
    """GET /api/source/users or /api/target/users: list users on selected source or target server."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return jsonify({'error': f'No {server_role} server selected'}), 400
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return jsonify({'error': err}), 404
    try:
        raw = client.get_users()
        users = raw if isinstance(raw, list) else (raw.get('items', raw.get('users', [])) if isinstance(raw, dict) else [])
        return jsonify({'users': users, 'server': server_name})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/source/users', methods=['GET'])
def get_source_users():
    return _api_users_list('source')


@app.route('/api/target/users', methods=['GET'])
def get_target_users():
    return _api_users_list('target')


@app.route('/api/compare/users-missing-on-target', methods=['GET'])
def get_users_missing_on_target():
    """Users present on source but not on target (by email). Stateless; does not use migration script globals."""
    settings, servers = _load_settings_and_servers()
    src_name = (settings or {}).get('selected_source_server')
    tgt_name = (settings or {}).get('selected_target_server')
    if not src_name or not tgt_name:
        return jsonify({'error': 'Both source and target server must be selected in settings'}), 400
    src_client, err1 = _get_api_client_for_server(src_name, servers)
    tgt_client, err2 = _get_api_client_for_server(tgt_name, servers)
    if err1:
        return jsonify({'error': err1}), 404
    if err2:
        return jsonify({'error': err2}), 404
    try:
        raw_tgt = tgt_client.get_users()
        tgt_users = raw_tgt if isinstance(raw_tgt, list) else (raw_tgt.get('items', raw_tgt.get('users', [])) if isinstance(raw_tgt, dict) else [])
        target_emails = {x.get('email', '').lower() for x in tgt_users if isinstance(x, dict) and x.get('email')}
        raw_src = src_client.get_users()
        src_users = raw_src if isinstance(raw_src, list) else (raw_src.get('items', raw_src.get('users', [])) if isinstance(raw_src, dict) else [])
        missing = [u for u in src_users if isinstance(u, dict) and u.get('email', '').lower() not in target_emails]
        return jsonify({'users_missing_on_target': missing, 'count': len(missing)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/compare/roles-missing-on-target', methods=['GET'])
def get_roles_missing_on_target():
    """Roles present on source but not on target (by role name)."""
    settings, servers = _load_settings_and_servers()
    src_name = (settings or {}).get('selected_source_server')
    tgt_name = (settings or {}).get('selected_target_server')
    if not src_name or not tgt_name:
        return jsonify({'error': 'Both source and target server must be selected in settings'}), 400
    src_client, err1 = _get_api_client_for_server(src_name, servers)
    tgt_client, err2 = _get_api_client_for_server(tgt_name, servers)
    if err1:
        return jsonify({'error': err1}), 404
    if err2:
        return jsonify({'error': err2}), 404
    try:
        raw_tgt = tgt_client.get_roles()
        tgt_roles = raw_tgt if isinstance(raw_tgt, list) else (raw_tgt.get('items', raw_tgt.get('roles', [])) if isinstance(raw_tgt, dict) else [])
        target_names = {str(x.get('name', x.get('_id', ''))).strip() for x in tgt_roles if isinstance(x, dict)}
        raw_src = src_client.get_roles()
        src_roles = raw_src if isinstance(raw_src, list) else (raw_src.get('items', raw_src.get('roles', [])) if isinstance(raw_src, dict) else [])
        missing = [r for r in src_roles if isinstance(r, dict) and str(r.get('name', r.get('_id', ''))).strip() not in target_names]
        return jsonify({'roles_missing_on_target': missing, 'count': len(missing)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _api_dashboards_list(server_role, owner_id=None, owner_email=None, title=None, oids=None, limit=1000):
    """List dashboards on source or target. Optional filters: owner_id, owner_email, title (search), oids (comma-separated).
    When title or oids are provided they take precedence over owner. When neither is set, behavior is unchanged (owner only).
    limit=0 means no cap (return full list); for the default path this uses fetch_all_dashboards."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return None, None, 'No {0} server selected'.format(server_role)
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return None, None, err
    try:
        items = []
        no_cap = limit == 0
        # By OID(s): comma-separated list (same as migration "By OID")
        if oids and oids.strip():
            oid_list = [x.strip() for x in oids.split(',') if x.strip()]
            seen_oids = set()
            for oid in oid_list:
                raw = client.get_dashboards_admin({"id": oid, "ownershipType": "allRoot"})
                chunk = _normalize_dashboard_list(raw)
                for d in chunk:
                    d_oid = d.get('oid') or d.get('_id') or d.get('id')
                    if d_oid and d_oid not in seen_oids:
                        seen_oids.add(d_oid)
                        items.append(d)
        # By title (search): same as migration "By Name"
        elif title and title.strip():
            raw = client.search_get_dashboards_by_title(title.strip())
            items = _normalize_dashboard_list(raw)
        # Default: by owner or all (use fetch_all_dashboards when no_cap for full list)
        else:
            # Sisense /v1/dashboards/admin filters by owner email via search=owner: email (not owner=uid)
            if owner_email and owner_email.strip():
                qs = {
                    'search': 'owner: ' + owner_email.strip(),
                    'dashboardType': 'owner',
                    'ownerInfo': 'true',
                    'ownershipType': 'allRoot',
                }
                raw = client.get_dashboards_admin(qs)
                items = _normalize_dashboard_list(raw)
                if limit and limit > 0 and len(items) > limit:
                    items = items[:limit]
                return items, server_name, None
            if no_cap:
                import logging
                items = fetch_all_dashboards(client, logging.getLogger(__name__))
                if owner_id:
                    items = [d for d in items if d.get('owner') == owner_id]
            else:
                qs = {}
                if owner_id:
                    qs['owner'] = owner_id
                raw = client.get_dashboards_admin(qs) if qs else client.get_dashboards_admin()
                items = _normalize_dashboard_list(raw)
        if limit and limit > 0 and len(items) > limit:
            items = items[:limit]
        return items, server_name, None
    except Exception as e:
        return None, None, str(e)


@app.route('/api/source/dashboards', methods=['GET'])
def get_source_dashboards():
    owner_id = request.args.get('owner_id')
    owner_email = request.args.get('owner_email')
    title = request.args.get('title') or request.args.get('search')
    oids = request.args.get('oids')
    limit = request.args.get('limit', type=int)
    if limit is None:
        limit = 1000
    items, server, err = _api_dashboards_list(
        'source', owner_id=owner_id, owner_email=owner_email, title=title, oids=oids, limit=limit
    )
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'dashboards': items, 'server': server})


@app.route('/api/target/dashboards', methods=['GET'])
def get_target_dashboards():
    owner_id = request.args.get('owner_id')
    owner_email = request.args.get('owner_email')
    title = request.args.get('title') or request.args.get('search')
    oids = request.args.get('oids')
    limit = request.args.get('limit', type=int)
    if limit is None:
        limit = 1000
    items, server, err = _api_dashboards_list(
        'target', owner_id=owner_id, owner_email=owner_email, title=title, oids=oids, limit=limit
    )
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'dashboards': items, 'server': server})


@app.route('/api/source/dashboards/count', methods=['GET'])
def get_source_dashboard_count():
    """Return total count of dashboards on the source server (no limit). Use for 'how many dashboards on source'."""
    import logging
    settings, servers = _load_settings_and_servers()
    server_name = (settings or {}).get('selected_source_server')
    if not server_name:
        return jsonify({'error': 'No source server selected'}), 400
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return jsonify({'error': err}), 500
    try:
        all_dashboards = fetch_all_dashboards(client, logging.getLogger(__name__))
        return jsonify({'count': len(all_dashboards), 'server': server_name})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/target/dashboards/count', methods=['GET'])
def get_target_dashboard_count():
    """Return total count of dashboards on the target server (no limit). Use for 'how many dashboards on target'."""
    import logging
    settings, servers = _load_settings_and_servers()
    server_name = (settings or {}).get('selected_target_server')
    if not server_name:
        return jsonify({'error': 'No target server selected'}), 400
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return jsonify({'error': err}), 500
    try:
        all_dashboards = fetch_all_dashboards(client, logging.getLogger(__name__))
        return jsonify({'count': len(all_dashboards), 'server': server_name})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _run_compare_dashboards():
    """Run dashboard compare and store result in _compare_dashboards_job. Called from a daemon thread."""
    import logging
    logger = logging.getLogger(__name__)
    with _compare_dashboards_lock:
        if _compare_dashboards_job['status'] != 'running':
            return
    settings, servers = _load_settings_and_servers()
    src_name = (settings or {}).get('selected_source_server')
    tgt_name = (settings or {}).get('selected_target_server')
    if not src_name or not tgt_name:
        with _compare_dashboards_lock:
            _compare_dashboards_job['status'] = 'done'
            _compare_dashboards_job['error'] = 'No source or target server selected'
        return
    compare_client_timeout = 180
    src_client, err_src = _get_api_client_for_server(src_name, servers, timeout=compare_client_timeout)
    tgt_client, err_tgt = _get_api_client_for_server(tgt_name, servers, timeout=compare_client_timeout)
    if err_src or err_tgt:
        with _compare_dashboards_lock:
            _compare_dashboards_job['status'] = 'done'
            _compare_dashboards_job['error'] = err_src or err_tgt
        return
    try:
        src_list = fetch_all_dashboards(src_client, logger)
        tgt_list = fetch_all_dashboards(tgt_client, logger)
    except Exception as e:
        with _compare_dashboards_lock:
            _compare_dashboards_job['status'] = 'done'
            _compare_dashboards_job['error'] = str(e)
        return
    src_oids = {d.get('oid') or d.get('_id') or d.get('id'): d for d in src_list if d.get('oid') or d.get('_id') or d.get('id')}
    tgt_oids = {d.get('oid') or d.get('_id') or d.get('id'): d for d in tgt_list if d.get('oid') or d.get('_id') or d.get('id')}
    src_set = set(src_oids.keys())
    tgt_set = set(tgt_oids.keys())
    only_on_source = [{"oid": oid, "title": (src_oids[oid].get('title') or src_oids[oid].get('name') or '')} for oid in (src_set - tgt_set)]
    only_on_target = [{"oid": oid, "title": (tgt_oids[oid].get('title') or tgt_oids[oid].get('name') or '')} for oid in (tgt_set - src_set)]
    on_both = [{"oid": oid, "title": (src_oids[oid].get('title') or src_oids[oid].get('name') or '')} for oid in (src_set & tgt_set)]
    with _compare_dashboards_lock:
        _compare_dashboards_job['status'] = 'done'
        _compare_dashboards_job['result'] = {
            "only_on_source": only_on_source,
            "only_on_target": only_on_target,
            "on_both": on_both,
        }
        _compare_dashboards_job['error'] = None


@app.route('/api/compare/dashboards/start', methods=['POST'])
def start_compare_dashboards():
    """Start dashboard compare in background. Poll GET /api/compare/dashboards/result for the result (avoids long-running request timeouts)."""
    with _compare_dashboards_lock:
        if _compare_dashboards_job['status'] == 'running':
            return jsonify({'status': 'running', 'message': 'Compare already in progress.'}), 409
        _compare_dashboards_job['status'] = 'running'
        _compare_dashboards_job['result'] = None
        _compare_dashboards_job['error'] = None
    t = threading.Thread(target=_run_compare_dashboards, daemon=True)
    t.start()
    return jsonify({'status': 'started', 'message': 'Compare started. Poll /api/compare/dashboards/result for the result.'}), 202


@app.route('/api/compare/dashboards/result', methods=['GET'])
def get_compare_dashboards_result():
    """Return dashboard compare result if ready. Optional ?wait=30 to block up to 30 seconds for the result."""
    wait = request.args.get('wait', type=int)
    if wait is not None and wait > 0:
        wait = min(wait, 60)
    deadline = (time.time() + wait) if wait else None
    while True:
        with _compare_dashboards_lock:
            status = _compare_dashboards_job['status']
            result = _compare_dashboards_job['result']
            error = _compare_dashboards_job['error']
        if status == 'done':
            if error:
                return jsonify({'status': 'done', 'error': error}), 200
            return jsonify({'status': 'done', **result}), 200
        if status == 'idle':
            return jsonify({'status': 'idle', 'message': 'No compare run. POST to /api/compare/dashboards/start first.'}), 404
        if deadline is None or time.time() >= deadline:
            return jsonify({'status': 'running', 'message': 'Compare still in progress.'}), 202
        time.sleep(1)


@app.route('/api/compare/dashboards', methods=['GET'])
def get_compare_dashboards():
    """Compare all dashboards on source vs target. No limit; returns only_on_source, only_on_target, on_both (by OID). Long-running; for agent use prefer start + poll result to avoid timeouts."""
    import logging
    settings, servers = _load_settings_and_servers()
    src_name = (settings or {}).get('selected_source_server')
    tgt_name = (settings or {}).get('selected_target_server')
    if not src_name:
        return jsonify({'error': 'No source server selected'}), 400
    if not tgt_name:
        return jsonify({'error': 'No target server selected'}), 400
    compare_client_timeout = 180
    src_client, err_src = _get_api_client_for_server(src_name, servers, timeout=compare_client_timeout)
    tgt_client, err_tgt = _get_api_client_for_server(tgt_name, servers, timeout=compare_client_timeout)
    if err_src:
        return jsonify({'error': err_src}), 500
    if err_tgt:
        return jsonify({'error': err_tgt}), 500
    logger = logging.getLogger(__name__)
    try:
        src_list = fetch_all_dashboards(src_client, logger)
        tgt_list = fetch_all_dashboards(tgt_client, logger)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    src_oids = {d.get('oid') or d.get('_id') or d.get('id'): d for d in src_list if d.get('oid') or d.get('_id') or d.get('id')}
    tgt_oids = {d.get('oid') or d.get('_id') or d.get('id'): d for d in tgt_list if d.get('oid') or d.get('_id') or d.get('id')}
    src_set = set(src_oids.keys())
    tgt_set = set(tgt_oids.keys())
    only_on_source = [{"oid": oid, "title": (src_oids[oid].get('title') or src_oids[oid].get('name') or '')} for oid in (src_set - tgt_set)]
    only_on_target = [{"oid": oid, "title": (tgt_oids[oid].get('title') or tgt_oids[oid].get('name') or '')} for oid in (tgt_set - src_set)]
    on_both = [{"oid": oid, "title": (src_oids[oid].get('title') or src_oids[oid].get('name') or '')} for oid in (src_set & tgt_set)]
    return jsonify({
        "only_on_source": only_on_source,
        "only_on_target": only_on_target,
        "on_both": on_both,
    })


def _api_datamodels_list(server_role):
    """List datamodels (elasticubes) on source or target server. Returns (list, server_name, error)."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return None, None, 'No {0} server selected'.format(server_role)
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return None, None, err
    try:
        raw = client.get_datamodels_metadata()
        items = []
        if isinstance(raw, dict) and raw.get('data') and isinstance(raw['data'].get('elasticubesMetadata'), list):
            items = raw['data']['elasticubesMetadata']
        elif isinstance(raw, list):
            items = raw
        return items, server_name, None
    except Exception as e:
        return None, None, str(e)


@app.route('/api/source/datamodels', methods=['GET'])
def get_source_datamodels():
    items, server, err = _api_datamodels_list('source')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'datamodels': items, 'server': server})


@app.route('/api/target/datamodels', methods=['GET'])
def get_target_datamodels():
    items, server, err = _api_datamodels_list('target')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'datamodels': items, 'server': server})


def _api_folders_list(server_role):
    """List folders on source or target server (flat structure). Returns (list, server_name, error)."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return None, None, 'No {0} server selected'.format(server_role)
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return None, None, err
    try:
        raw = client.get_folders('flat')
        items = raw if isinstance(raw, list) else []
        return items, server_name, None
    except Exception as e:
        return None, None, str(e)


@app.route('/api/source/folders', methods=['GET'])
def get_source_folders():
    items, server, err = _api_folders_list('source')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'folders': items, 'server': server})


@app.route('/api/target/folders', methods=['GET'])
def get_target_folders():
    items, server, err = _api_folders_list('target')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'folders': items, 'server': server})


def _api_groups_list(server_role):
    """List groups on source or target server. Returns (list, server_name, error)."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return None, None, 'No {0} server selected'.format(server_role)
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return None, None, err
    try:
        raw = client.get_groups()
        items = raw if isinstance(raw, list) else (raw.get('items', raw.get('groups', [])) if isinstance(raw, dict) else [])
        return items, server_name, None
    except Exception as e:
        return None, None, str(e)


@app.route('/api/source/groups', methods=['GET'])
def get_source_groups():
    items, server, err = _api_groups_list('source')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'groups': items, 'server': server})


@app.route('/api/target/groups', methods=['GET'])
def get_target_groups():
    items, server, err = _api_groups_list('target')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'groups': items, 'server': server})


def _api_roles_list(server_role):
    """List roles on source or target server. Returns (list, server_name, error)."""
    settings, servers = _load_settings_and_servers()
    key = 'selected_source_server' if server_role == 'source' else 'selected_target_server'
    server_name = (settings or {}).get(key)
    if not server_name:
        return None, None, 'No {0} server selected'.format(server_role)
    client, err = _get_api_client_for_server(server_name, servers)
    if err:
        return None, None, err
    try:
        raw = client.get_roles()
        items = raw if isinstance(raw, list) else (raw.get('items', raw.get('roles', [])) if isinstance(raw, dict) else [])
        return items, server_name, None
    except Exception as e:
        return None, None, str(e)


@app.route('/api/source/roles', methods=['GET'])
def get_source_roles():
    items, server, err = _api_roles_list('source')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'roles': items, 'server': server})


@app.route('/api/target/roles', methods=['GET'])
def get_target_roles():
    items, server, err = _api_roles_list('target')
    if err:
        return jsonify({'error': err}), 400 if 'selected' in err else 500
    return jsonify({'roles': items, 'server': server})


@app.route('/api/migration-log', methods=['GET'])
def get_migration_log():
    """Return last N lines of migration log. Uses latest per-run log (migration_YYYYMMDD_HHMMSS.log) or legacy single file. Query param: lines=500."""
    lines = request.args.get('lines', type=int) or 500
    log_path = _get_latest_migration_log_path()
    if not os.path.exists(log_path):
        return jsonify({'content': '', 'path': log_path, 'message': 'Log file not created yet.'})
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()
        last_n = all_lines[-lines:] if len(all_lines) > lines else all_lines
        content = ''.join(last_n)
        return jsonify({'content': content, 'path': log_path, 'lines': len(last_n)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/migration-log/download', methods=['GET'])
def get_migration_log_download():
    """Stream the latest migration log file as an attachment. Uses same resolution as get_migration_log."""
    log_path = _get_latest_migration_log_path()
    if not os.path.exists(log_path):
        return jsonify({'error': 'Log file not found.', 'path': log_path}), 404
    try:
        return send_file(log_path, as_attachment=True, download_name=os.path.basename(log_path), mimetype='text/plain')
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/migration/status', methods=['GET'])
def get_migration_status():
    """Current run state: single source of truth for migration in progress."""
    global migration_process
    running = migration_process is not None and migration_process.poll() is None
    return jsonify({
        'running': running,
        'message': 'Migration in progress.' if running else 'No migration running.',
    })


@app.route('/api/run_migration', methods=['POST'])
def api_run_migration():
    """Start migration in background; returns 202 or 409. Shares migration gate with run_migration_stream."""
    global migration_process
    if migration_process is not None and migration_process.poll() is None:
        return jsonify({
            'status': 'conflict',
            'message': 'Migration already in progress.',
        }), 409
    def drain_stdout(proc):
        if proc.stdout:
            try:
                for _ in iter(proc.stdout.readline, ''):
                    pass
            except Exception:
                pass
            try:
                proc.stdout.close()
            except Exception:
                pass
        proc.wait()
        global migration_process
        migration_process = None
    try:
        migration_process = _build_migration_subprocess()
        t = threading.Thread(target=drain_stdout, args=(migration_process,), daemon=True)
        t.start()
        return jsonify({
            'status': 'started',
            'message': 'Migration started. You can ask me to show the migration log to monitor progress.',
            'migration_output_url': '/migration_output.html',
        }), 202
    except Exception as e:
        migration_process = None
        return jsonify({'status': 'error', 'message': str(e)}), 500


def _normalize_dashboard_list(raw):
    """Return a list of dashboard items from API response (list or dict with items)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return raw.get('items', raw.get('dashboards', []))
    return []


@app.route('/api/validate-dashboards', methods=['POST'])
def validate_dashboards():
    """Start a background validation job; returns job_id for polling."""
    import logging
    from SisenseRESTAPIClientClass import SisenseRestApiClient, SisenseRestAPIError

    logger = logging.getLogger(__name__)
    try:
        data = request.get_json() or {}
        server_name = (data.get('serverName') or '').strip()
        include_jaql = data.get('includeJaql') is True
        show_jaql_in_results = data.get('showJaqlInResults') is True
        jaql_mode = (data.get('jaqlMode') or 'widget').strip().lower()
        if jaql_mode not in ('widget', 'item'):
            jaql_mode = 'widget'

        if not server_name:
            return jsonify({'error': 'Server name is required'}), 400

        servers_path = get_config_path(SERVERS_FILE)
        if not os.path.exists(servers_path):
            return jsonify({'error': 'Servers file not found'}), 404
        with open(servers_path, 'r', encoding='utf-8') as f:
            servers = yaml.safe_load(f)
        if server_name not in (servers or {}):
            return jsonify({'error': f'Server "{server_name}" not found'}), 404
        server_config = servers[server_name]
        for key in ('host', 'port', 'protocol', 'os', 'api_token'):
            if key not in server_config:
                return jsonify({'error': f'Server "{server_name}" is missing required key: {key}'}), 400

        job_id = str(uuid.uuid4())
        # Fetch dashboard list in the main request thread (avoids thread/subprocess hangs under IDE)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        import tempfile
        config_for_fetch = dict(server_config)
        config_for_fetch['verify'] = _resolve_verify(server_config, None)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_for_fetch, f)
            config_file = f.name
        result_file = tempfile.mktemp(suffix=".json")
        # When frozen (PyInstaller), we cannot run a .py script; use --run-validation-fetch mode
        if getattr(sys, "frozen", False):
            fetch_cmd = [sys.executable, "--run-validation-fetch", config_file, result_file]
        else:
            script_path = os.path.join(script_dir, "validation_fetch_standalone.py")
            fetch_cmd = [sys.executable, script_path, config_file, result_file]
        try:
            fetch_env = os.environ.copy()
            if getattr(sys, "frozen", False):
                fetch_env["PYTHONWARNINGS"] = "ignore::UserWarning"
            proc = subprocess.Popen(
                fetch_cmd,
                cwd=script_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
                env=fetch_env,
            )
            proc.wait(timeout=90)
            if proc.returncode != 0:
                err = proc.stderr.read() if proc.stderr else ""
                try:
                    os.unlink(config_file)
                    os.unlink(result_file)
                except Exception:
                    pass
                return jsonify({'error': f'Dashboard fetch failed (exit {proc.returncode}): {err[:300]}'}), 500
            if not os.path.exists(result_file):
                try:
                    os.unlink(config_file)
                except Exception:
                    pass
                return jsonify({'error': 'Dashboard fetch produced no result file'}), 500
            with open(result_file, "r") as f:
                out = json.load(f)
            if not out.get("ok"):
                try:
                    os.unlink(config_file)
                    os.unlink(result_file)
                except Exception:
                    pass
                return jsonify({'error': out.get('error', 'Fetch failed')}), 500
            dashboards_list = out.get("data", [])
        except subprocess.TimeoutExpired as e:
            try:
                if e.process:
                    e.process.kill()
            except Exception:
                pass
            try:
                os.unlink(config_file)
                os.unlink(result_file)
            except Exception:
                pass
            return jsonify({'error': 'Dashboard fetch timed out after 90 seconds'}), 504
        finally:
            try:
                os.unlink(config_file)
            except Exception:
                pass
            try:
                os.unlink(result_file)
            except Exception:
                pass

        # Run widget (+ optional JAQL) checks in a standalone process; it writes progress to a file.
        # Use a file under script_dir so the subprocess (cwd=script_dir) can always write to it.
        progress_file = os.path.join(script_dir, f".validation_progress_{job_id}.json")
        dashboards_file = tempfile.mktemp(suffix=".json")
        config_file_run = tempfile.mktemp(suffix=".json")
        try:
            config_for_run = dict(server_config)
            config_for_run['verify'] = _resolve_verify(server_config, None)
            with open(config_file_run, "w") as f:
                json.dump(config_for_run, f)
            with open(dashboards_file, "w") as f:
                json.dump(dashboards_list, f)
            initial = {
                "status": "running",
                "log": [
                    f"Starting validation for server: {server_name}",
                    "Loading server config...",
                    "Server config loaded.",
                    f"Connecting to: {server_config['host']}:{server_config['port']}",
                    f"Dashboard list fetched ({len(dashboards_list)} dashboards).",
                    f"Checking {len(dashboards_list)} dashboard(s)...",
                ],
                "total_dashboards": len(dashboards_list),
                "dashboards_checked": 0,
                "widgets_checked": 0,
                "current_dashboard_name": "",
            }
            with open(progress_file, "w") as f:
                json.dump(initial, f)
        except Exception as e:
            try:
                os.unlink(dashboards_file)
                os.unlink(progress_file)
                os.unlink(config_file_run)
            except Exception:
                pass
            return jsonify({'error': f'Failed to prepare validation: {str(e)}'}), 500

        # When frozen (PyInstaller), run in-worker mode; otherwise run the .py script
        if getattr(sys, "frozen", False):
            cmd = [sys.executable, "--run-validation-run", config_file_run, dashboards_file, progress_file]
        else:
            run_script = os.path.join(script_dir, "validation_run_standalone.py")
            cmd = [sys.executable, "-u", run_script, config_file_run, dashboards_file, progress_file]
        if include_jaql:
            cmd.append("--include-jaql")
            cmd.append(f"--jaql-mode={jaql_mode}")
            if show_jaql_in_results:
                cmd.append("--show-jaql-in-results")
        stderr_file = progress_file + ".stderr"

        # In frozen app, suppress pkg_resources UserWarning in validation subprocess stderr
        proc_env = os.environ.copy()
        if getattr(sys, "frozen", False):
            proc_env["PYTHONWARNINGS"] = "ignore::UserWarning"

        # Avoid start_new_session so child isn't detached; some environments (e.g. Cursor/IDE)
        # may not run detached processes or may delay them.
        proc = subprocess.Popen(
            cmd,
            cwd=script_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=False,
            env=proc_env,
        )

        def _drain_stderr(process, path):
            try:
                with open(path, "w") as f:
                    for line in process.stderr:
                        f.write(line)
                        f.flush()
            except Exception:
                pass
            finally:
                try:
                    process.stderr.close()
                except Exception:
                    pass

        drain_thread = threading.Thread(target=_drain_stderr, args=(proc, stderr_file), daemon=True)
        drain_thread.start()
        logger.info("Validation job %s: started subprocess (progress=%s)", job_id[:8], progress_file)
        # So you can watch progress in the terminal: tail -f <progress_file>
        print(f"Validation job {job_id[:8]}: progress file (tail -f to watch in terminal): {progress_file}", flush=True)

        control_file = progress_file + ".control"
        with _validation_jobs_lock:
            _validation_jobs[job_id] = {
                'progress_file': progress_file,
                'dashboards_file': dashboards_file,
                'config_file': config_file_run,
                'stderr_file': stderr_file,
                'control_file': control_file,
                'proc': proc,
            }
        return jsonify({'jobId': job_id})
    except Exception as e:
        logger.exception('validate-dashboards start failed')
        return jsonify({'error': str(e)}), 500


def _is_valid_uuid(s):
    """Return True if s is a valid UUID string."""
    if not s or not isinstance(s, str):
        return False
    try:
        uuid.UUID(s)
        return True
    except ValueError:
        return False


@app.route('/api/validate-dashboards/control/<job_id>', methods=['POST'])
def validate_dashboards_control(job_id):
    """Pause, resume, or stop a running validation job."""
    if not _is_valid_uuid(job_id):
        return jsonify({'error': 'Invalid job ID'}), 400
    with _validation_jobs_lock:
        job = _validation_jobs.get(job_id)
    if job is None:
        return jsonify({'error': 'Job not found', 'status': 'not_found'}), 404
    data = request.get_json() or {}
    action = (data.get('action') or '').strip().lower()
    if action not in ('pause', 'resume', 'stop'):
        return jsonify({'error': 'Invalid action; use pause, resume, or stop'}), 400
    control_file = job.get('control_file')
    if control_file:
        try:
            with open(control_file, 'w') as f:
                f.write(action)
                f.flush()
        except Exception as e:
            return jsonify({'error': f'Failed to write control file: {e}'}), 500
    if action == 'stop':
        proc = job.get('proc')
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
    return jsonify({'ok': True, 'action': action})


@app.route('/api/validate-dashboards/status/<job_id>', methods=['GET'])
def validate_dashboards_status(job_id):
    """Return current progress and final result for a validation job."""
    if not _is_valid_uuid(job_id):
        return jsonify({'error': 'Invalid job ID'}), 400
    with _validation_jobs_lock:
        job = _validation_jobs.get(job_id)
    if job is None:
        return jsonify({'error': 'Job not found', 'status': 'not_found'}), 404
    # If progress file was already cleaned up, return stored final result
    progress_file = job.get('progress_file')
    if progress_file and not os.path.exists(progress_file) and job.get('final_result'):
        resp = jsonify(job['final_result'])
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
        resp.headers['Pragma'] = 'no-cache'
        return resp
    # If job uses progress file (standalone script), read from it
    if progress_file and os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                out = json.load(f)
            stderr_file = job.get('stderr_file')
            if stderr_file and os.path.exists(stderr_file):
                try:
                    with open(stderr_file, 'r') as ef:
                        out['stderr'] = ef.read()[-8192:]
                except Exception:
                    pass
            # If process exited, reflect that in status (do not overwrite "completed")
            proc = job.get('proc')
            if proc is not None and out.get('status') != 'completed':
                exit_code = proc.poll()
                if exit_code is not None:
                    # Process ended (Stop button / SIGTERM / crash). Treat as stopped so UI shows partial results.
                    out['status'] = 'stopped'
            # When run is finished, store result and delete progress + temp files (including config/dashboards with token)
            if out.get('status') in ('completed', 'stopped', 'failed'):
                with _validation_jobs_lock:
                    j = _validation_jobs.get(job_id)
                    if j is not None:
                        j['final_result'] = dict(out)
                for p in (progress_file, job.get('stderr_file'), job.get('control_file'), progress_file + '.tmp' if progress_file else None, job.get('dashboards_file'), job.get('config_file')):
                    if p and os.path.exists(p):
                        try:
                            os.unlink(p)
                        except Exception:
                            pass
            resp = jsonify(out)
            resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
            resp.headers['Pragma'] = 'no-cache'
            return resp
        except Exception:
            pass
    # Legacy: build from job dict
    out = {
        'status': job.get('status', 'pending'),
        'total_dashboards': job.get('total_dashboards', 0),
        'dashboards_checked': job.get('dashboards_checked', 0),
        'widgets_checked': job.get('widgets_checked', 0),
        'current_dashboard_name': job.get('current_dashboard_name', ''),
        'log': job.get('log') or [],
    }
    if job.get('total_jaql') is not None:
        out['total_jaql'] = job['total_jaql']
        out['jaql_checked'] = job.get('jaql_checked', 0)
    if job.get('status') == 'completed':
        out['dashboardsOk'] = job.get('dashboardsOk', 0)
        out['dashboardsFailed'] = job.get('dashboardsFailed', 0)
        out['widgetsOk'] = job.get('widgetsOk', 0)
        out['widgetsFailed'] = job.get('widgetsFailed', 0)
        out['errors'] = job.get('errors', [])
        out['jaqlOk'] = job.get('jaqlOk')
        out['jaqlFailed'] = job.get('jaqlFailed')
    if job.get('status') == 'failed':
        out['error'] = job.get('error', 'Unknown error')
    return jsonify(out)


@app.route('/api/notebooks', methods=['GET'])
def get_notebooks():
    """Fetch notebooks from the source server"""
    try:
        # Load settings to get the selected source server
        settings_path = get_config_path(SETTINGS_FILE)
        if not os.path.exists(settings_path):
            return jsonify({'error': 'Settings file not found', 'description': 'Settings file not found'}), 404
        
        with open(settings_path, 'r', encoding='utf-8') as f:
            settings = yaml.safe_load(f)
        
        source_server_name = settings.get('selected_source_server')
        if not source_server_name:
            return jsonify({'error': 'No source server selected', 'description': 'No source server selected'}), 400
        
        # Load servers to get source server config
        servers_path = get_config_path(SERVERS_FILE)
        if not os.path.exists(servers_path):
            return jsonify({'error': 'Servers file not found', 'description': 'Servers file not found'}), 404
        
        with open(servers_path, 'r', encoding='utf-8') as f:
            servers = yaml.safe_load(f)
        
        if source_server_name not in servers:
            error_msg = f'Source server "{source_server_name}" not found'
            return jsonify({'error': error_msg, 'description': error_msg}), 404
        
        server_config = servers[source_server_name]
        
        # Create API client for source server
        from SisenseRESTAPIClientClass import SisenseRestApiClient
        import logging
        
        logger = logging.getLogger(__name__)
        
        verify_ssl = _resolve_verify(server_config, None)
        api_client = SisenseRestApiClient(
            server_domain=server_config['host'],
            port=server_config['port'],
            protocol=server_config['protocol'],
            operating_system=server_config['os'],
            api_token=server_config['api_token'],
            verify=verify_ssl,
            timeout=30,
            logger=logger
        )
        
        # Fetch notebooks
        try:
            notebooks_response = api_client.get_notebooks()
            # Log the response for debugging
            if logger:
                logger.debug(f"Notebooks API response type: {type(notebooks_response)}, value: {notebooks_response}")
        except Exception as api_error:
            import traceback
            error_trace = traceback.format_exc()
            error_msg = str(api_error)
            
            # Log the full error for debugging
            if logger:
                logger.error(f"Error fetching notebooks: {error_msg}")
                logger.error(f"Traceback: {error_trace}")
            
            # Check if it's a SisenseRestAPIError (has response attribute)
            if hasattr(api_error, 'result') and hasattr(api_error.result, 'status_code'):
                status_code = api_error.result.status_code
                try:
                    error_text = api_error.result.text[:500] if hasattr(api_error.result, 'text') else str(api_error)
                except:
                    error_text = str(api_error)
                
                if status_code == 401 or status_code == 403:
                    error_response = f'Authentication failed (HTTP {status_code}): {error_text}'
                    return jsonify({
                        'error': error_response,
                        'description': error_response,
                        'details': 'Please verify the API token for the source server.'
                    }), 500
                elif status_code == 404:
                    error_response = f'Notebooks endpoint not found (HTTP {status_code}): {error_text}'
                    return jsonify({
                        'error': error_response,
                        'description': error_response,
                        'details': 'The notebooks API endpoint may not be available on this server version.'
                    }), 500
                else:
                    error_response = f'API error (HTTP {status_code}): {error_text}'
                    return jsonify({
                        'error': error_response,
                        'description': error_response
                    }), 500
            # Check if it's a connection error
            elif 'connection' in error_msg.lower() or 'timeout' in error_msg.lower():
                error_response = f'Failed to connect to source server "{source_server_name}": {error_msg}'
                return jsonify({
                    'error': error_response,
                    'description': error_response,
                    'details': 'Please verify the server connection settings and API token.'
                }), 500
            else:
                error_response = f'Error fetching notebooks from source server: {error_msg}'
                return jsonify({
                    'error': error_response,
                    'description': error_response
                }), 500
        
        # Handle different response formats
        if isinstance(notebooks_response, list):
            notebooks = notebooks_response
        elif isinstance(notebooks_response, dict):
            notebooks = notebooks_response.get('data', notebooks_response.get('notebooks', notebooks_response.get('items', [])))
            # If still empty, check if the response itself is the list (some APIs return dict with list directly)
            if not notebooks and isinstance(notebooks_response, dict):
                # Check if any value in the dict is a list
                for key, value in notebooks_response.items():
                    if isinstance(value, list):
                        notebooks = value
                        break
        elif notebooks_response is None:
            notebooks = []
            if logger:
                logger.warning("Notebooks API returned None")
        else:
            notebooks = []
            if logger:
                logger.warning(f"Unexpected notebooks response type: {type(notebooks_response)}, value: {notebooks_response}")
        
        if logger:
            logger.info(f"Returning {len(notebooks)} notebooks")
        
        return jsonify({'notebooks': notebooks})
    
    except KeyError as e:
        missing_key = str(e).strip("'")
        error_response = f'Missing required server configuration: {missing_key}'
        return jsonify({
            'error': error_response,
            'description': error_response,
            'details': f'Please check the server configuration for "{source_server_name if "source_server_name" in locals() else "unknown"}"'
        }), 500
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        error_response = f'Error fetching notebooks: {str(e)}'
        return jsonify({
            'error': error_response,
            'description': error_response,
            'details': error_trace if app.debug else 'Check server logs for details'
        }), 500


@app.route('/assets/<path:filename>')
def serve_assets(filename):
    # Use the resource_path helper to find the assets folder
    assets_dir = resource_path('assets')
    return send_from_directory(assets_dir, filename)


@app.route('/api/shutdown', methods=['POST'])
def shutdown_server():
    """Gracefully shutdown the Waitress server"""
    def shutdown():
        import time
        import os
        time.sleep(0.5)  # Give the response time to be sent
        print("\n--- Server shutdown requested via web UI ---")
        os._exit(0)  # Force exit the process
    
    # Start shutdown in a separate thread to allow response to be sent
    shutdown_thread = threading.Thread(target=shutdown)
    shutdown_thread.daemon = True
    shutdown_thread.start()
    
    return jsonify({'message': 'Server shutdown initiated', 'status': 'shutting_down'})


@app.route('/api/version', methods=['GET'])
def get_version():
    """Get the application version"""
    return jsonify({'version': APP_VERSION})


@app.route('/api/chat/env-api-keys', methods=['GET'])
def get_chat_env_api_keys():
    """Report whether OPENAI_API_KEY / GOOGLE_API_KEY are set in env (no values)."""
    return jsonify({
        'openai': bool((os.environ.get('OPENAI_API_KEY') or '').strip()),
        'google': bool((os.environ.get('GOOGLE_API_KEY') or '').strip()),
    })


def _chat_request_data():
    """Parse and return chat params from request JSON. Returns (message, history, mode, settings_path, provider, api_key, model, llm_client)."""
    data = request.get_json() or {}
    message = (data.get('message') or '').strip()
    history = data.get('history')
    if history is not None and not isinstance(history, list):
        history = None
    mode = data.get('mode') or 'tool_only'
    settings_path = (data.get('settings_path') or '').strip() or None
    provider = (data.get('provider') or 'OpenAI').strip()
    api_key = (data.get('api_key') or '').strip() or None
    if not api_key and mode in ('llm', 'llm_only'):
        if provider.lower() == 'google':
            api_key = (os.environ.get('GOOGLE_API_KEY') or '').strip() or None
        else:
            api_key = (os.environ.get('OPENAI_API_KEY') or '').strip() or None
    model = (data.get('model') or '').strip() or None
    llm_client = None
    if mode in ('llm', 'llm_only') and api_key:
        llm_client = {
            'provider': 'google' if provider.lower() == 'google' else 'openai',
            'api_key': api_key,
            'model': model or ('gpt-4o-mini' if provider.lower() != 'google' else 'gemini-2.5-flash'),
        }
    return message, history, mode, settings_path, provider, api_key, model, llm_client


def _patch_async_openai_client_for_gemini(async_client):
    """Gemini's OpenAI-compatible Chat Completions API rejects several OpenAI-only body fields.
    The agents SDK may pass them (e.g. frequency_penalty=0, which is not omitted). Strip them
    before each request so tool-calling agent runs succeed."""
    from openai import omit

    completions = async_client.chat.completions
    _orig_create = completions.create
    _strip = (
        'frequency_penalty',
        'presence_penalty',
        'verbosity',
        'metadata',
        'store',
        'prompt_cache_retention',
        'top_logprobs',
    )

    async def _create(*args, **kwargs):
        kwargs = dict(kwargs)
        for key in _strip:
            kwargs[key] = omit
        return await _orig_create(*args, **kwargs)

    completions.create = _create


def _run_agent_stream(message: str, llm_client: dict, queue: Queue, history=None):
    """Run the agent with MCP in a dedicated event loop (called from a thread). Puts ('delta', text) and ('done', {reply, model}) into queue.
    history: optional list of {role, content} for conversation context (user/assistant turns). SDK accepts easy input format."""
    import asyncio
    try:
        from dotenv import load_dotenv
        _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
        load_dotenv(_env_path)
    except ImportError:
        pass
    # Ensure API key is in env for the agents SDK (it reads OPENAI_API_KEY / GOOGLE_API_KEY)
    api_key = (llm_client or {}).get('api_key') or ''
    if not api_key:
        api_key = os.environ.get('OPENAI_API_KEY') or os.environ.get('GOOGLE_API_KEY') or ''
    if api_key:
        if (llm_client or {}).get('provider') == 'google':
            os.environ['GOOGLE_API_KEY'] = api_key
        else:
            os.environ['OPENAI_API_KEY'] = api_key
    try:
        from agents import Agent, Runner, set_default_openai_key, set_default_openai_client, set_default_openai_api
        from agents.mcp import MCPServerStdio
    except ImportError as e:
        queue.put(('done', {'reply': f'Agent SDK not available: {e}. Install openai-agents and mcp.', 'model': None}))
        return
    provider = (llm_client or {}).get('provider') or 'openai'
    if provider == 'google' and api_key:
        # Use Gemini via OpenAI-compatible API. Gemini only supports Chat Completions, not the
        # default Responses API, so we must switch the SDK to chat_completions.
        from openai import AsyncOpenAI
        set_default_openai_api('chat_completions')
        gemini_client = AsyncOpenAI(
            api_key=api_key,
            base_url='https://generativelanguage.googleapis.com/v1beta/openai/',
        )
        _patch_async_openai_client_for_gemini(gemini_client)
        set_default_openai_client(gemini_client, use_for_tracing=False)
    else:
        if api_key:
            set_default_openai_key(api_key)
    migration_url = os.environ.get('MIGRATION_SERVER_URL', 'http://localhost:5001')
    mcp_env = os.environ.copy()
    mcp_env['MIGRATION_SERVER_URL'] = migration_url
    if api_key:
        if (llm_client or {}).get('provider') == 'google':
            mcp_env['GOOGLE_API_KEY'] = api_key
        else:
            mcp_env['OPENAI_API_KEY'] = api_key
    model_name = (llm_client or {}).get('model') or 'gpt-4o-mini'
    instructions = (
        'You help users run Sisense migrations, configure settings, and answer questions about source and target servers. '
        'Use the MCP tools to read and update the full migration settings schema, run migrations, list and compare users, dashboards, datamodels, folders, groups, and roles on source and target, and read migration logs. '
        'Respond only to the user\'s most recent message (the current turn). If the conversation history shows one or more user messages without an assistant reply in between, ignore those earlier user messages and answer only the latest one. Do not combine or echo answers to previous unanswered questions. '
        'When the user asks for server-specific data (e.g. dashboards, users, datamodels, folders, groups, roles owned by or on a server) without saying "source" or "target", ask which server they mean (source or target) before calling list_source_* or list_target_* tools. '
        'When the user asks to validate or test connection to the "source" or "target", use test_connection_to_source() or test_connection_to_target() (do not pass the literal word "source" or "target" to test_connection—those tools use the currently selected server from settings). '
        'When the user asks how many dashboards (or the count of dashboards) on the source or target, use get_source_dashboard_count() or get_target_dashboard_count(); do not use the list tools and count items—list results are capped and would give a wrong total. '
        'When the user asks "how many did you find?" or "how many did you list?" after you showed a dashboard list, call get_source_dashboard_count() or get_target_dashboard_count() and report the actual total; clarify that the list you showed was capped (e.g. first 200) so the number in the list is not the total. '
        'When the user asks which dashboards from source are missing on target (or to compare source vs target), call compare_dashboards(). It may take 5–10 minutes with many dashboards; wait for the result and then report only_on_source (titles only if they asked for names). If the user says they got a timeout or to try again, call get_compare_dashboards_result() to retrieve the result if the compare has finished. '
        'When the user asks for the full list of dashboards (e.g. "list all dashboards", "show me every dashboard", "complete list"), use list_source_dashboards or list_target_dashboards with full_list=True so they get every dashboard, not a capped list. '
        'When the user says "migrate all dashboards", "migrate every dashboard", or "migrate everything" for dashboards without naming an owner or a list, use set_migrate_all_dashboards() then run_migration. Do NOT use prepare_dashboard_migration(owner_email=...) for that—do not pass owner_email "*" or "all". For "migrate all dashboards owned by X" or "migrate dashboards for user Y", use prepare_dashboard_migration(owner_email=X) then run_migration; for a specific list by name or OID use prepare_dashboard_migration(titles=...) or prepare_dashboard_migration(oids=...) then run_migration. '
        'When the user says "update the configuration to migrate these dashboards" or "migrate these dashboards", you MUST use the dashboard list from the current conversation (the list you just showed from compare_dashboards, list_source_dashboards, or similar) and call prepare_dashboard_migration with that exact list: use titles= with comma-separated dashboard titles, or oids= with comma-separated OIDs. Do not report that the configuration was updated without actually calling prepare_dashboard_migration with the specific list. If the user wants overwrite mode, pass import_mode="overwrite" to prepare_dashboard_migration. '
        'If the user asks for a plan or preview first, use get_settings_preview to show the planned configuration and do not save or run until they confirm. '
        'When the user asks to run a specific migration type (e.g. "run user migration", "only migrate users", "just user migration"), you MUST first call update_settings to enable only that type (e.g. migrate_users: true) and set other migrate_* options to false (migrate_dashboards, migrate_folders, migrate_groups, migrate_datamodels, migrate_datasecurity, etc.), then call run_migration. Never run_migration when the user requested a single type without updating settings first.'
        'When the user asks for migration results, outcome, or how the migration went: call get_migration_log to read the log, then summarize it in a short report (e.g. success or failure, key steps completed, counts, any errors); do not paste the entire raw log. Only show the full raw log when the user explicitly asks for it (e.g. "show me the full log", "entire log", "raw log", "display the log"). '
        'When the user asks for a specific count or fact from the migration (e.g. "how many dashboards migrated successfully", "exact number of failures"): get the migration log (use a large lines value if needed, e.g. 2000 or more) and parse it to extract the exact number; if the first portion does not contain the answer, fetch more of the log. Always give an exact answer (e.g. "X dashboards migrated successfully"), not a generic estimate or "difficult to provide". '
        'When you report that a migration has been started, tell the user they can open the migration output page to follow progress and include a markdown link: [Open migration output](/migration_output.html). '
        'When the user asks for the migration log or after discussing migration results, you may offer a download link: [Download migration log](/api/migration-log/download). '
        'Format your responses in markdown by default: use headings, lists, and bold where they make the answer clearer. Only use plain text if the user explicitly asks for it (e.g. "plain text", "no markdown"). '
        'When returning migration log content or other multi-line tool output (e.g. from read_migration_log or long lists), always wrap it in a markdown code block (triple backticks) so it displays with correct line breaks and monospace in the chat.'
    )

    async def _async_run():
        try:
            params = {
                'command': sys.executable,
                'args': ['-m', 'migration_mcp'],
                'env': mcp_env,
            }
            # Allow long-running MCP tool calls (e.g. dashboard compare can take 5–10 minutes).
            async with MCPServerStdio(
                name='Migration MCP',
                params=params,
                client_session_timeout_seconds=900,
            ) as server:
                agent = Agent(
                    name='Migration Assistant',
                    instructions=instructions,
                    mcp_servers=[server],
                    model=model_name,
                )
                # Pass conversation history so the agent has context (e.g. "show me again").
                # Sliding-window cap to avoid exceeding model context (e.g. Gemini 1M tokens); per-message cap for long tool output.
                if history and isinstance(history, list):
                    input_items = []
                    for h in history:
                        if isinstance(h, dict) and h.get('role') in ('user', 'assistant', 'system', 'developer') and 'content' in h:
                            content = (h['content'] or '') if isinstance(h['content'], str) else str(h['content'])
                            if len(content) > CHAT_MAX_MESSAGE_CHARS:
                                content = content[:CHAT_MAX_MESSAGE_CHARS] + '\n[... truncated for context limit ...]'
                            input_items.append({'role': h['role'], 'content': content})
                    if len(input_items) > CHAT_MAX_HISTORY_MESSAGES:
                        input_items = input_items[-CHAT_MAX_HISTORY_MESSAGES:]
                        input_items.insert(0, {'role': 'user', 'content': '(Earlier messages in this conversation were omitted to stay within context limits. Below is the recent context.)'})
                    input_items.append({'role': 'user', 'content': message})
                    agent_input = input_items
                else:
                    agent_input = message
                result = Runner.run_streamed(agent, input=agent_input)
                try:
                    from openai.types.responses import ResponseTextDeltaEvent
                except ImportError:
                    ResponseTextDeltaEvent = None
                # Use only one source of text to avoid duplication: token deltas (raw_response_event)
                # or full message (message_output_item). Prefer deltas when present.
                used_raw_delta = False
                async for event in result.stream_events():
                    if event.type == 'raw_response_event' and ResponseTextDeltaEvent and isinstance(event.data, ResponseTextDeltaEvent):
                        delta = getattr(event.data, 'delta', None) or getattr(event.data, 'text', '')
                        if delta:
                            used_raw_delta = True
                            queue.put(('delta', delta))
                    elif event.type == 'run_item_stream_event' and getattr(event, 'item', None) and not used_raw_delta:
                        item = event.item
                        if getattr(item, 'type', None) == 'message_output_item':
                            try:
                                from agents import ItemHelpers
                                text = ItemHelpers.text_message_output(item)
                                if text:
                                    queue.put(('delta', text))
                            except Exception:
                                pass
                final = (getattr(result, 'final_output', None) or '') if hasattr(result, 'final_output') else ''
                if not final and hasattr(result, 'output') and result.output:
                    final = result.output if isinstance(result.output, str) else str(result.output)
                queue.put(('done', {'reply': final or '', 'model': model_name}))
        except Exception as e:
            err_msg = str(e)
            try:
                import logging
                _log = logging.getLogger(__name__)
                _log.exception('Chat agent run failed: %s', err_msg)
                resp = getattr(e, 'response', None)
                if resp is not None:
                    try:
                        _log.error('LLM HTTP %s body: %s', getattr(resp, 'status_code', '?'), (resp.text or '')[:4000])
                    except Exception:
                        pass
            except Exception:
                traceback.print_exc()
            if 'ReadError' in type(e).__name__ or 'ReadError' in err_msg:
                reply = (
                    'The connection to the LLM was interrupted while receiving the response. '
                    'This can be due to network issues, timeouts, or the provider closing the stream. Please try again.'
                )
            elif '400' in err_msg and 'INVALID_ARGUMENT' in err_msg:
                reply = (
                    'The assistant hit a request limit or invalid argument from the LLM provider. '
                    'If you just changed your API key or token, start a **new chat** and try again. '
                    'Otherwise try rephrasing or a shorter request; check server logs for details.'
                )
            else:
                reply = f'Error: {err_msg}'
            queue.put(('done', {'reply': reply, 'model': model_name}))
        finally:
            queue.put(None)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_async_run())
    finally:
        loop.close()


@app.route('/api/chat/stream', methods=['POST'])
def api_chat_stream():
    """Stream chat reply as Server-Sent Events. Uses Agent + MCP migration tools when mode is llm or llm_only."""
    message, history, mode, settings_path, provider, api_key, model, llm_client = _chat_request_data()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    if mode == 'tool_only' or not llm_client:
        def generate_stub():
            yield "data: " + json.dumps({
                "reply": "Use LLM mode (and set an API key) to talk to the migration assistant.",
                "source": "tool_only",
                "model": None,
            }) + "\n\n"
        return Response(
            stream_with_context(generate_stub()),
            mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
        )

    q = Queue()
    thread = threading.Thread(target=_run_agent_stream, args=(message, llm_client, q), kwargs={'history': history}, daemon=True)
    thread.start()

    def generate():
        model_used = None
        full_reply = []
        while True:
            try:
                item = q.get(timeout=120)
            except Empty:
                break
            if item is None:
                break
            kind, data = item
            if kind == 'delta':
                full_reply.append(data)
                yield "data: " + json.dumps({"delta": data}) + "\n\n"
            elif kind == 'done':
                reply = data.get('reply', '') if isinstance(data, dict) else ''
                model_used = data.get('model') if isinstance(data, dict) else None
                final_text = reply if reply else "".join(full_reply)
                yield "data: " + json.dumps({"reply": final_text, "source": "llm", "model": model_used or model}) + "\n\n"
                break

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


@app.route('/api/chat', methods=['POST'])
def api_chat():
    """Non-streaming chat. Uses Agent + MCP when mode is llm or llm_only."""
    message, history, mode, settings_path, provider, api_key, model, llm_client = _chat_request_data()
    if not message:
        return jsonify({'error': 'message is required'}), 400

    if mode == 'tool_only' or not llm_client:
        return jsonify({
            'reply': 'Use LLM mode (and set an API key) to talk to the migration assistant.',
            'source': 'tool_only',
            'model': None,
        })

    q = Queue()
    thread = threading.Thread(target=_run_agent_stream, args=(message, llm_client, q), kwargs={'history': history}, daemon=True)
    thread.start()
    thread.join(timeout=120)
    reply = ''
    model_used = None
    while True:
        try:
            item = q.get_nowait()
        except Empty:
            break
        if item is None:
            break
        kind, data = item
        if kind == 'done' and isinstance(data, dict):
            reply = data.get('reply', '')
            model_used = data.get('model')
    if not reply:
        reply = 'No response from assistant.'
    return jsonify({'reply': reply, 'source': 'llm', 'model': model_used})


if __name__ == '__main__':
    multiprocessing.freeze_support()

    # This simple parser determines if we are running as a server or a worker.
    # It doesn't interfere with the more complex parser in config_loader.
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-migration', action='store_true')

    # Use parse_known_args() to avoid errors about other flags (like --settings-file)
    args, unknown = parser.parse_known_args()

    if args.run_migration:
        # WORKER MODE: The subprocess calls this branch.
        print("--- Running in Worker Mode ---")
        # The migration script will handle its own argument parsing for --settings-file.
        run_migration_logic()
    else:
        # SERVER MODE (default): This is what runs when you double-click the .exe.
        print("--- Running in Server Mode ---")
        print("--- Starting production server with Waitress ---")
        print("--- Server will be available at http://localhost:5001 ---")
        print("--- Press Ctrl+C or use the 'Stop Server' button in the web UI to stop ---")
        
        # Open browser automatically after a short delay
        def open_browser():
            time.sleep(1.5)  # Wait for server to be ready
            url = 'http://localhost:5001'
            print(f"--- Opening browser to {url} ---")
            try:
                webbrowser.open(url)
            except Exception as e:
                print(f"--- Could not open browser automatically: {e} ---")
                print(f"--- Please manually open: {url} ---")
        
        browser_thread = threading.Thread(target=open_browser)
        browser_thread.daemon = True
        browser_thread.start()
        
        try:
            # Run Waitress server (blocking call)
            serve(app, host='0.0.0.0', port=5001, threads=8)
        except KeyboardInterrupt:
            print("\n--- Shutting down server... ---")
            sys.exit(0)