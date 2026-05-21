#!/usr/bin/env python3
"""
Independent test script to verify notebook update permissions (401 error).

This script tests whether the API token has permission to update notebooks
using the PATCH endpoint, which is failing with 401 errors during migration.

Usage:
    python test_notebook_update_permissions.py [target_server_name] [notebook_id]
    
If no arguments are provided, it will:
1. Load the target server from settings.yaml
2. Get the first notebook from the target server
3. Attempt to update it
"""

import sys
import os
import yaml
import json
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from SisenseRESTAPIClientClass import SisenseRestApiClient, SisenseRestAPIError
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_user_config_path(filename, app_name="sisense_merge_and_migration"):
    """Get the path to a user configuration file (same logic as main app)"""
    import platform
    if platform.system() == 'Windows':
        base_dir = os.environ.get('LOCALAPPDATA')
        if not base_dir:
            base_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local')
        config_dir = os.path.join(base_dir, app_name)
    elif platform.system() == 'Darwin':  # macOS
        config_dir = os.path.join(os.path.expanduser('~/Library/Application Support'), app_name)
    else:  # Linux
        base_dir = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
        config_dir = os.path.join(base_dir, app_name)
    
    # Create directory if it doesn't exist
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, filename)


def load_servers():
    """Load server configuration from servers.yaml"""
    # Try user config directory first (same as main app)
    servers_file = get_user_config_path('servers.yaml')
    if not os.path.exists(servers_file):
        # Fallback to project root
        servers_file = project_root / 'servers.yaml'
        if not servers_file.exists():
            servers_file = project_root / 'config' / 'servers.yaml'
    
    if not os.path.exists(servers_file):
        raise FileNotFoundError(f"Servers file not found. Looked in: {get_user_config_path('servers.yaml')} and {project_root}")
    
    with open(servers_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_settings():
    """Load settings to get target server name"""
    # Try user config directory first (same as main app)
    settings_file = get_user_config_path('settings.yaml')
    if not os.path.exists(settings_file):
        # Fallback to project root
        settings_file = project_root / 'settings.yaml'
        if not settings_file.exists():
            settings_file = project_root / 'config' / 'settings.yaml'
    
    if not os.path.exists(settings_file):
        return {}
    
    with open(settings_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def test_notebook_update(server_name=None, notebook_id=None):
    """
    Test notebook update permissions on the target server.
    
    Args:
        server_name: Name of the target server (from servers.yaml)
        notebook_id: ID of the notebook to test (if None, uses first notebook found)
    """
    # Load configuration
    servers = load_servers()
    settings = load_settings()
    
    # Determine target server
    if not server_name:
        server_name = settings.get('selected_target_server')
        if not server_name:
            print("ERROR: No target server specified and 'selected_target_server' not found in settings.yaml")
            print(f"Available servers: {list(servers.keys())}")
            return False
    
    if server_name not in servers:
        print(f"ERROR: Server '{server_name}' not found in servers.yaml")
        print(f"Available servers: {list(servers.keys())}")
        return False
    
    server_config = servers[server_name]
    print(f"\n{'='*60}")
    print(f"Testing notebook update permissions on: {server_name}")
    print(f"Host: {server_config['host']}:{server_config['port']}")
    print(f"{'='*60}\n")
    
    # Create API client
    api_client = SisenseRestApiClient(
        server_domain=server_config['host'],
        port=server_config['port'],
        protocol=server_config['protocol'],
        operating_system=server_config['os'],
        api_token=server_config['api_token'],
        verify=True,
        timeout=30,
        logger=logger
    )
    
    # Test 1: Get notebooks (should work)
    print("Test 1: Getting notebooks from target server...")
    try:
        notebooks_response = api_client.get_notebooks()
        if isinstance(notebooks_response, dict):
            notebooks = notebooks_response.get('data', [])
        elif isinstance(notebooks_response, list):
            notebooks = notebooks_response
        else:
            notebooks = []
        
        print(f"✓ Successfully retrieved {len(notebooks)} notebooks")
        
        if not notebooks:
            print("ERROR: No notebooks found on target server. Cannot test update.")
            return False
        
        # Select notebook to test
        if notebook_id:
            # Find notebook by ID (using prefix matching like the migration tool)
            test_notebook = None
            for nb in notebooks:
                nb_id = nb.get('id', '')
                if nb_id == notebook_id or nb_id.startswith(notebook_id):
                    test_notebook = nb
                    break
            
            if not test_notebook:
                print(f"ERROR: Notebook with ID '{notebook_id}' not found")
                print(f"Available notebook IDs: {[nb.get('id') for nb in notebooks[:10]]}")
                return False
        else:
            # Use first notebook
            test_notebook = notebooks[0]
        
        notebook_uuid = test_notebook.get('uuid') or test_notebook.get('oid')
        notebook_display_name = test_notebook.get('displayName', test_notebook.get('id', 'Unknown'))
        
        print(f"\nSelected notebook for testing:")
        print(f"  ID: {test_notebook.get('id')}")
        print(f"  Display Name: {notebook_display_name}")
        print(f"  UUID: {notebook_uuid}")
        
    except Exception as e:
        print(f"✗ Failed to get notebooks: {e}")
        return False
    
    # Test 2: Try to update notebook ID (this should fail with 401)
    print(f"\n{'='*60}")
    print("Test 2: Attempting to update notebook ID (PATCH /v1/notebooks/{uuid})")
    print(f"{'='*60}")
    
    update_identifier = notebook_uuid if notebook_uuid else test_notebook.get('id')
    update_payload = {'id': test_notebook.get('id')}  # Try to set the same ID
    
    print(f"Endpoint: PATCH /v1/notebooks/{update_identifier}")
    print(f"Payload: {json.dumps(update_payload, indent=2)}")
    print()
    
    try:
        result = api_client.update_notebook(update_identifier, update_payload)
        print(f"✓ SUCCESS: Notebook ID update succeeded!")
        print(f"Response: {result}")
        return True
    except SisenseRestAPIError as e:
        status_code = e.result.status_code if hasattr(e, 'result') and hasattr(e.result, 'status_code') else None
        response_text = e.result.text if hasattr(e, 'result') and hasattr(e.result, 'text') else str(e)
        
        print(f"✗ FAILED: Notebook ID update failed")
        print(f"Status Code: {status_code}")
        print(f"Response: {response_text}")
        
        if status_code == 401:
            print(f"\n{'!'*60}")
            print("CONFIRMED: 401 Unauthorized error - API token does NOT have permission to update notebooks")
            print(f"{'!'*60}")
            return False
        else:
            print(f"\nUnexpected error code: {status_code}")
            return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Try to update notebook codePath (this should also fail with 401)
    print(f"\n{'='*60}")
    print("Test 3: Attempting to update notebook codePath (PATCH /v1/notebooks/{uuid})")
    print(f"{'='*60}")
    
    code_path = test_notebook.get('codePath', '')
    if code_path:
        update_code_path_payload = {'codePath': code_path}  # Try to set the same codePath
        
        print(f"Endpoint: PATCH /v1/notebooks/{update_identifier}")
        print(f"Payload: {json.dumps(update_code_path_payload, indent=2)}")
        print()
        
        try:
            result = api_client.update_notebook(update_identifier, update_code_path_payload)
            print(f"✓ SUCCESS: Notebook codePath update succeeded!")
            print(f"Response: {result}")
        except SisenseRestAPIError as e:
            status_code = e.result.status_code if hasattr(e, 'result') and hasattr(e.result, 'status_code') else None
            response_text = e.result.text if hasattr(e, 'result') and hasattr(e.result, 'text') else str(e)
            
            print(f"✗ FAILED: Notebook codePath update failed")
            print(f"Status Code: {status_code}")
            print(f"Response: {response_text}")
            
            if status_code == 401:
                print(f"\n{'!'*60}")
                print("CONFIRMED: 401 Unauthorized error - API token does NOT have permission to update notebooks")
                print(f"{'!'*60}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")
    else:
        print("Skipping codePath update test (notebook has no codePath)")
    
    return False


if __name__ == '__main__':
    server_name = sys.argv[1] if len(sys.argv) > 1 else None
    notebook_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        success = test_notebook_update(server_name, notebook_id)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

