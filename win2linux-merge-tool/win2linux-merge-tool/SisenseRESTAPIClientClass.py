import urllib
from urllib.parse import quote

import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re


class SisenseRestApiClient:

    def __init__(self, api_token=None, operating_system='linux', protocol='http', server_domain='localhost',
                 port=443,
                 verify=True, timeout=30, logger=None, session=None):
        # Handle multi-tenancy paths in the server domain
        path_parts = server_domain.split('/', 1)
        host = path_parts[0]
        self.tenant_path = ''
        if len(path_parts) > 1:
            # Ensure tenant_path starts with a slash and doesn't have extra slashes
            self.tenant_path = '/' + path_parts[1].strip('/')

        self.server_base_url = f'{protocol}://{host}:{port}{self.tenant_path}/api'
        # self.api_token = api_token
        self.logger = logger
        self.session = requests.Session()

        self.operating_system = operating_system
        self.api_token = api_token

        if session is None:
            self.session = requests.Session()
        else:
            self.session = session
        self._set_retry_adapter()  # Apply the adapter to the session
        self.session.verify = verify
        self.timeout = timeout
        if api_token is None:
            print(f'Enter username for {server_domain}:')
            u = input()
            self.username = u

            print(f'Enter password for {server_domain}')

            p = input()

            # Let SisenseRestAPIError from __create_token propagate
            self.api_token = self.__create_token(u, p)
            print(f"{server_domain} api+key={self.api_token}")

    def _handle_response(self, response: requests.Response):
        """
        Handles a requests.Response object, parsing JSON if appropriate
        and raising consistent errors.
        """
        if not response.ok:
            
            raise SisenseRestAPIError(response)

        # If there's no content, it's a success (e.g., 204 No Content)
        if not response.text:
            return 'OK'

        content_type = response.headers.get('Content-Type', '')
        # Some valid responses might be plain text 'OK' or '200 OK'
        if 'application/json' not in content_type:
            response_text = response.text.strip()
            # Accept plain text success responses like "OK", "200 OK", etc.
            if response_text == 'OK' or response_text == '200 OK' or (response.status_code == 200 and response_text.upper().endswith('OK')):
                return 'OK'
            msg = f"Unexpected content type '{content_type}' from server. Expected JSON. Raw response: {response.text[:500]}..."
            if self.logger:
                self.logger.error(msg)
            raise SisenseRestAPIError(response)

        try:
            return response.json()
        except json.JSONDecodeError as e:
            msg = f"Failed to decode JSON response. HTTP {response.status_code}. Raw response: {response.text}"
            if self.logger:
                self.logger.error(msg)
            raise SisenseRestAPIError(response) from e

    def _set_retry_adapter(self):
        """
        Sets up a retry strategy with exponential backoff and jitter using HTTPAdapter.
        """
        retry_strategy = Retry(
            total=3,  # Maximum number of retries
            connect=0, # Do not retry on connection timeouts or connection errors.
            read=0,    # Do not retry on read timeouts.
            backoff_factor=1,  # Base for exponential backoff (e.g., 1, 2, 4, 8...)
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)  # Mount for HTTPS
        self.session.mount("http://", adapter)  # Mount for HTTP

    def __create_token(self, username, password):

        payload = "username={0}&password={1}".format(
            username, password)

        url = self.server_base_url + "/v1/authentication/login"

        headers = {
            'content-type': "application/x-www-form-urlencoded",
            'accept': "application/json"
        }

        response = requests.request(
            "POST", url, data=payload, headers=headers, verify=self.session.verify)

        if response.ok:
            try:
                res_json = response.json()
                self.api_token = res_json['access_token']
                return self.api_token
            except (json.JSONDecodeError, KeyError) as e:
                raise SisenseRestAPIError(response) from e
        else:
            raise SisenseRestAPIError(response)

    def __get_token(self):
        return self.api_token or self.__create_token()

    def __request_post(self, endpoint, payload, querystring=None):
        if querystring is None:
            querystring = {}
        token = self.__get_token()
        headers = {
            "accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            'authorization': "Bearer " + token
        }
        # Add Internal header only for notebook operations (required for creates/updates)
        has_internal_header = False
        if '/notebooks' in endpoint:
            headers['Internal'] = 'true'
            has_internal_header = True

        url = self.server_base_url + endpoint

        # Ensure payload is in the correct format
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        # Handle specific format issues for folders
        if 'folders' in url:
            payload = payload.replace('null', '\"\"')

        

        # Add retry logic
        session = requests.Session()
        retries = Retry(
            total=3,  # Attempt 3 retries
            backoff_factor=1,  # Wait 1s, 2s, 4s between retries
            status_forcelist=[502, 503, 504],  # Retry on specific server errors
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        try:
            response = self.session.post(url, headers=headers, data=payload, params=querystring)
            
            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Request failed: {e}")

    def __request_put(self, endpoint, payload, querystring=None):

        if querystring is None:
            querystring = {}
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            'authorization': "Bearer " + self.__get_token()
        }

        url = self.server_base_url + endpoint

        # Ensure payload is in the correct format
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        response = requests.request("PUT", url, headers=headers, data=payload, params=querystring,
                                    verify=self.session.verify, timeout=self.timeout)

        return self._handle_response(response)

    def __request_patch(self, endpoint, payload, querystring=None):

        if querystring is None:
            querystring = {}
        token = self.__get_token()
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            'authorization': "Bearer " + token
        }
        # Add Internal header only for notebook operations (required for updates)
        has_internal_header = False
        if '/notebooks' in endpoint:
            headers['Internal'] = 'true'
            has_internal_header = True

        url = self.server_base_url + endpoint

        # Ensure payload is in the correct format
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        

        response = requests.request("PATCH", url, headers=headers, data=payload, params=querystring,
                                    verify=self.session.verify, timeout=self.timeout)

        

        return self._handle_response(response)

    def __request_get(self, endpoint, querystring=None, timeout=None):

        if querystring is None:
            querystring = {}
        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }

        url = self.server_base_url + endpoint

        

        try:
            response = self.session.get(url, headers=headers, params=querystring, verify=self.session.verify, timeout=timeout if timeout else self.timeout)
            
            

            # --- FIX: Handle export endpoints that send JSON with an incorrect Content-Type header ---
            is_export_endpoint = '/export/' in endpoint or '/datamodel-exports/' in endpoint
            if is_export_endpoint:
                if response.ok:
                    try:
                        return response.json()
                    except json.JSONDecodeError as e:
                        msg = f"Failed to decode JSON from an export endpoint. URL: {url}, Status: {response.status_code}, Raw response: {response.text[:500]}..."
                        if self.logger:
                            self.logger.error(msg)
                        raise SisenseRestAPIError(response) from e
                else:
                    # If the export endpoint call itself fails (e.g., 404), raise the error normally.
                    raise SisenseRestAPIError(response)

            # Special handling for non-standard endpoints
            if 'encryption/encrypt' in endpoint:
                if response.ok: return response.text
                else: raise SisenseRestAPIError(response)
            elif 'encryption/decrypt' in endpoint:
                if not response.ok: raise SisenseRestAPIError(response)
                def add_quotes_to_json_values(json_string):
                    json_string = json_string.replace("'", '"')
                    def replace_values(match):
                        key, value = match.groups()
                        if not (value.startswith('"') and value.endswith('"')):
                            return f'"{key}": "{value}"'
                        else:
                            return match.group(0)
                    pattern = r'"([^"]+)": ([^",}]+)'
                    return re.sub(pattern, replace_values, json_string)
                fixed_json = add_quotes_to_json_values(response.text)
                return json.loads(fixed_json)

            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise SisenseRestAPIError(
                e.response if hasattr(e, 'response') and e.response is not None else requests.Response())
        except requests.exceptions.ReadTimeout as e:
            raise ConnectionError(f"Read timeout error: {e}")


    def test_http_connection(self) -> bool:
        """
        Tests the HTTP connection to the Sisense server. Uses a different endpoint
        for multi-tenant systems to ensure a valid test.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        if self.tenant_path:
            test_endpoint_url = f"{self.server_base_url}/v1/application/status"
        else:
            test_endpoint_url = f"{self.server_base_url}/test"

        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }
        try:
            # Use self.timeout (e.g. 30s); avoid hardcoded 10s which can be too short on Windows (proxy/DNS/TLS)
            response = self.session.get(test_endpoint_url, headers=headers, timeout=self.timeout, verify=self.session.verify)
            if response.ok:
                self.logger.info(f"--> SUCCESS: Connection test OK.")
                return True
            else:
                self.logger.error(f"--> FAILED: Connection test. URL: {test_endpoint_url}, Status: {response.status_code}, Response: {response.text[:200]}...")
                return False
        except Exception as e:
            self.logger.error(f"--> FAILED: Exception during connection test for URL {test_endpoint_url}: {str(e)}")
            return False

    def decrypt(self, value):

        endpoint = "/v1/encryption/decrypt"

        querystring = {"value": value}

        return self.__request_get(endpoint, querystring)

    def encrypt(self, value):

        # value = json.dumps(value)
        # value = quote(value)  # Apply URL encoding to the value

        endpoint = f"/v1/encryption/encrypt"

        querystring = {"value": json.dumps(value)}
        # querystring = {}
        return self.__request_get(endpoint, querystring)

    def can_be_owned(self, dash_oid, email):

        endpoint = f'/v1/dashboards/{dash_oid}/can_be_owned'

        querystring = {"email": email}

        return self.__request_get(endpoint, querystring)

    def check_elasticube_exists(self, ec_title):

        endpoint = '/v2/ecm'

        payload = {
            "query": "query doesCubeExists($title: String!) {\n  elasticubeExists(title: $title)\n}\n",
            "variables": {"title": ec_title},
            "operationName": "doesCubeExists"
        }

        return self.__request_post(endpoint, json.dumps(payload))
    def get_live_datasecurity(self, elasticube_name):
        """
        Get datasecurity rules for a live model Elasticube.

        Args:
            elasticube_name (str): The name of the Elasticube (live model).
        
        Returns:
            Response object from the API.
        """
        endpoint = f"/v1/elasticubes/live/{elasticube_name}/datasecurity"
        querystring = {}
        return self.__request_get(endpoint, querystring)

    def get_datasecurity(self, elasticube_name):

        endpoint = f'/elasticubes/localhost/{elasticube_name}/datasecurity'

        querystring = {}

        return self.__request_get(endpoint, querystring)
    def set_live_datasecurity_add_many(self, elasticube_name, datasecurity_payload):
        """
        Set datasecurity rules for a live model Elasticube.

        Args:
            elasticube_name (str): The name of the Elasticube (live model).
            datasecurity_payload (dict): The datasecurity rule payload (as dict).

        Returns:
            Response object from the API.
        """
        endpoint = f"/v1/elasticubes/live/{elasticube_name}/datasecurity/addMany"
        headers = {'Content-Type': 'application/json'}
 
        return self.__request_post(endpoint, json.dumps(datasecurity_payload))

    def update_datasecurity(self, elasticube_name, ds_rules):

        endpoint = f'/elasticubes/localhost/{elasticube_name}/datasecurity'

        return self.__request_post(endpoint, json.dumps(ds_rules))

    def get_system_settings(self):
        """
        Get system settings from the API.

        Returns:
            Response object from the API containing system settings.
        """
        
        endpoint = '/v1/settings/system'
        querystring = {}
        response = self.__request_get(endpoint, querystring)
        
        return response

    def is_custom_code_enabled(self):
        """
        Check if custom code feature is enabled on the server.

        Returns:
            bool: True if custom code is enabled, False otherwise.
            Returns False if the check fails or settings are unavailable.
        """
        try:
            settings_response = self.get_system_settings()
            if isinstance(settings_response, dict):
                custom_code = settings_response.get('customCode', {})
                if isinstance(custom_code, dict):
                    enabled = custom_code.get('enabled', False)
                    
                    return enabled
            
            return False
        except Exception as e:
            # If we can't check, assume it's disabled to be safe
            
            return False

    def get_notebooks(self):
        """
        Get all notebooks.

        Returns:
            Response object from the API containing the list of notebooks.
        """
        
        endpoint = '/v1/notebooks'
        querystring = {}
        return self.__request_get(endpoint, querystring)

    def export_notebook(self, notebook_id):
        """
        Export a notebook by ID.

        Args:
            notebook_id (str): The ID of the notebook to export.

        Returns:
            Response object from the API containing the exported notebook data.
        """
        endpoint = f'/v1/notebooks/{notebook_id}/export'
        querystring = {}
        return self.__request_get(endpoint, querystring)

    def create_notebook(self, notebook_payload):
        """
        Create a new notebook.

        Args:
            notebook_payload (dict): The notebook configuration payload containing:
                - id: Notebook ID
                - displayName: Display name
                - codePath: Path to the notebook code
                - description: Description (optional)
                - group: Group (optional)
                - icon: Icon (optional)
                - cellsDisable: List of disabled cell indices
                - additionalParameters: Additional parameters
                - columns: List of column definitions
                - isActive: Whether notebook is active
                - isSystem: Whether notebook is system notebook
                - language: Programming language (e.g., "Python")
                - mode: Execution mode (e.g., "Full")
                - serverUrl: Server URL
                - timeout: Timeout value
                - isTableDiscoveryEnabled: Whether table discovery is enabled
                - notebookCode: Notebook code structure with cells

        Returns:
            Response object from the API containing the created notebook data.
        """
        endpoint = '/v1/notebooks'
        return self.__request_post(endpoint, json.dumps(notebook_payload))

    def update_notebook(self, notebook_id, notebook_payload):
        """
        Update an existing notebook by ID.

        Args:
            notebook_id (str): The ID of the notebook to update.
            notebook_payload (dict): The notebook configuration payload containing fields to update.
                Can include any notebook fields such as:
                - id: Notebook ID
                - displayName: Display name
                - description: Description
                - isActive: Whether notebook is active
                - And other notebook configuration fields

        Returns:
            Response object from the API containing the updated notebook data.
        """
        endpoint = f'/v1/notebooks/{notebook_id}'
        
        return self.__request_patch(endpoint, json.dumps(notebook_payload))

    def delete_notebook(self, notebook_id):
        """
        Delete a notebook by ID (UUID).

        Args:
            notebook_id (str): The UUID or ID of the notebook to delete.

        Returns:
            Response object from the API.
        """
        endpoint = f'/v1/notebooks/{notebook_id}'
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'authorization': 'Bearer ' + self.__get_token()
        }
        url = self.server_base_url + endpoint
        # Send empty JSON object in body as shown in curl example
        response = self.session.delete(url, headers=headers, data='{}', verify=self.session.verify, timeout=self.timeout)
        return self._handle_response(response)

    def list_notebook_folder_contents(self, notebook_id):
        """
        List contents of a notebook folder.

        Args:
            notebook_id (str): The notebook ID.

        Returns:
            dict: Response containing items list with file information.
        """
        # The explore API uses a different base path
        explore_base_url = self.server_base_url.replace('/api', '/app/explore/api')
        
        # Construct the endpoint
        endpoint = f'/resources/notebooks/custom_code_notebooks/notebooks/{notebook_id}/'
        
        headers = {
            'accept': '*/*',
            'authorization': 'Bearer ' + self.__get_token()
        }
        
        url = explore_base_url + endpoint
        response = self.session.get(url, headers=headers, verify=self.session.verify, timeout=self.timeout)
        return self._handle_response(response)

    def rename_notebook_file(self, old_file_path, new_file_path):
        """
        Rename a notebook file (e.g., .ipynb or .json file).

        Args:
            old_file_path (str): The current file path (e.g., /notebooks/custom_code_notebooks/notebooks/oldId/oldId.ipynb).
            new_file_path (str): The new file path (e.g., /notebooks/custom_code_notebooks/notebooks/newId/newId.ipynb).

        Returns:
            Response object from the API.
        """
        from urllib.parse import quote
        
        # The explore API uses a different base path
        explore_base_url = self.server_base_url.replace('/api', '/app/explore/api')
        
        # Construct the endpoint with old file path (remove leading slash if present)
        endpoint_path = old_file_path.lstrip('/')
        endpoint = f'/resources/{endpoint_path}'
        
        # Build query parameters
        querystring = {
            'action': 'rename',
            'destination': new_file_path,  # The API expects the unencoded path in the query param
            'override': 'false',
            'rename': 'false'
        }
        
        headers = {
            'accept': '*/*',
            'authorization': 'Bearer ' + self.__get_token()
        }
        
        url = explore_base_url + endpoint
        response = self.session.patch(url, headers=headers, params=querystring, verify=self.session.verify, timeout=self.timeout)
        return self._handle_response(response)

    def rename_notebook_folder(self, old_notebook_id, new_notebook_id):
        """
        Rename notebook folder and files to match the new notebook ID.

        Args:
            old_notebook_id (str): The current notebook ID (before rename).
            new_notebook_id (str): The new notebook ID (after ID update).

        Returns:
            Response object from the API.
        """
        from urllib.parse import quote
        
        # The explore API uses a different base path
        explore_base_url = self.server_base_url.replace('/api', '/app/explore/api')
        
        # Construct the endpoint with old notebook ID
        endpoint = f'/resources/notebooks/custom_code_notebooks/notebooks/{old_notebook_id}/'
        
        # Construct destination path with new notebook ID and URL encode it
        destination_path = f'/notebooks/custom_code_notebooks/notebooks/{new_notebook_id}'
        encoded_destination = quote(destination_path, safe='')
        
        # Build query parameters
        querystring = {
            'action': 'rename',
            'destination': destination_path,  # The API expects the unencoded path in the query param
            'override': 'false',
            'rename': 'false'
        }
        
        headers = {
            'accept': '*/*',
            'authorization': 'Bearer ' + self.__get_token()
        }
        
        url = explore_base_url + endpoint
        response = self.session.patch(url, headers=headers, params=querystring, verify=self.session.verify, timeout=self.timeout)
        return self._handle_response(response)

    def create_shared_formula(self, formula_obj):

        endpoint = '/v1/formulas'

        return self.__request_post(endpoint, json.dumps(formula_obj))

    def get_shared_formulas(self, datasource_name, flat=True):

        endpoint = '/v1/formulas'

        querystring = {"datasource": datasource_name, "flat": flat}

        return self.__request_get(endpoint, querystring)

    def elasticube_run_sql_query(self, elasticube_name, query, result_format=None):

        endpoint = "/elasticubes/DataTransformer_test/Sql"

        querystring = {}

        if result_format:
            querystring["format"] = result_format

        querystring["query"] = query

        url = self.server_base_url + endpoint

        headers = {
            'accept': "text/csv",
            'authorization': "Bearer " + self.__get_token()
        }

        response = None
        response = requests.request(
            "GET", url, headers=headers, params=querystring, verify=self.session.verify)

        return response.text

    def elasticube_run_jaql_query(self, elasticube_name, jaql_query, result_format=None):

        self.logger.debug("jaql query to run in api: " + jaql_query)

        endpoint = "/datasources/{0}/jaql".format(elasticube_name)
        # querystring = jaql_query

        url = self.server_base_url + endpoint

        headers = {
            'content-type': "application/json",
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token()
        }

        response = None
        response = requests.request(
            "POST", url, data=jaql_query, headers=headers, verify=self.session.verify)

        if response.reason == 'OK':

            return response.text

        else:
            raise SisenseRestAPIError(response)

    def get_users(self):
        """
        Fetch all users in the Sisense system.

        Returns:
            dict: JSON response with user data.
        """
        endpoint = "/v1/users"
        return self.__request_get(endpoint)

    def change_user_password(self, user_id, password):
        endpoint = f'/users/{user_id}'

        payload = {
            "password": password,
        }

        return self.__request_put(endpoint, payload)

    def get_user_by_id(self, userid):

        endpoint = f"/v1/users/{userid}"
        return self.__request_get(endpoint)

    def get_my_user(self):
        endpoint = "/users/loggedin"
        return self.__request_get(endpoint)

    def get_roles(self):

        endpoint = "/roles"
        return self.__request_get(endpoint)

    def get_groups(self):

        endpoint = "/groups"
        return self.__request_get(endpoint)

    def get_folders(self, struct, fields = None):

        endpoint = '/v1/folders'
        querystring = {"structure": struct}

        if fields:
            querystring["fields"] = fields

        return self.__request_get(endpoint, querystring)

    def get_navver(self):

        endpoint = '/v1/navver'
        return self.__request_get(endpoint)

    def get_folder_ancestors(self, folder_id, struct):

        endpoint = '/v1/folders'
        querystring = {"structure": struct}
        return self.__request_get(endpoint, querystring)

    def get_dashboards(self, fields=None, timeout=None):
        """
        Retrieve a list of dashboards with optional fields filtering.

        Args:
            fields (list, optional): A list of fields to include in the response. If None, fetches all fields.

        Returns:
            dict: JSON response containing dashboards data.
        """
        endpoint = "/v1/dashboards"
        querystring = {"fields": fields} if fields else {}
        return self.__request_get(endpoint, querystring, timeout=timeout)

    def get_dashboards_admin(self, querystring=None):

        endpoint = "/v1/dashboards/admin"
        # querystring = {}
        return self.__request_get(endpoint, querystring)

    def export_dash_by_id(self, dash_id):

        endpoint = f'/dashboards/{dash_id}/export/'

        querystring = {"adminAccess": "true"}

        return self.__request_get(endpoint, querystring)

    def import_dashboards_bulk(self, payload, action=None):

        endpoint = '/v1/dashboards/import/bulk'

        if action:
            querystring = {"action": action}
        else:
            querystring = {}

        # payload = payload.replace('\"previewLayout\": \"\"', '\"previewLayout\": null')

        if '\"previewLayout\": null' in payload:
            payload = payload.replace('\"previewLayout\": null', '\"previewLayout\": []')

        return self.__request_post(endpoint, payload, querystring)

    def change_dashboard_owner(self, dash_id, owner_id):

        endpoint = f"/v1/dashboards/{dash_id}/change_owner"

        payload = {
            "ownerId": owner_id,
            "originalOwnerRule": "edit"
        }

        querystring = ''
        # querystring = {"adminAccess": "true"}

        return self.__request_post(endpoint, json.dumps(payload), querystring)

    def change_folder_owner(self, folder_id, owner_id):

        endpoint = f"/v1/folders/{folder_id}"

        payload = {
            "owner": owner_id
        }

        querystring = ''

        return self.__request_patch(endpoint, json.dumps(payload), querystring)

    def share_datamodel(self, server, dm_name, payload):

        endpoint = f'/elasticubes/{server}/{dm_name}/permissions'

        return self.__request_put(endpoint, json.dumps(payload))

    def get_datamodels_metadata(self):

        endpoint = "/v2/ecm"

        # For newer Sisense versions use the Payload
        #     payload = {
        # "query": "query _ {\n  elasticubesMetadata {\n    ...ecMetaData\n    __typename\n  }\n}\n\nfragment ecMetaData on ElasticubeMetadata {\n  oid\n  title\n  server\n  tenant {\n    _id\n    name\n    systemManagement\n    __typename\n  }\n  fiscal\n  serverId\n  hasDatasets\n  type\n  relationType\n  provider\n  importTime\n  hasDatasetsWithoutConnectionParameters\n  datasets {\n    oid\n    database\n    schemaName\n    connection {\n      id\n      __typename\n    }\n    __typename\n  }\n  set {\n    title\n    __typename\n  }\n  status\n  lastBuildTime\n  lastBuildStatus\n  lastSuccessfulBuildStartTime\n  lastSuccessfulManualBuildStartTime\n  lastSuccessfulBuildTime\n  lastPublishTime\n  lastUpdated\n  hasPendingChanges\n  nextBuildTime\n  creator {\n    id\n    firstName\n    lastName\n    __typename\n  }\n  sizeInMb\n  shares {\n    partyId\n    type\n    ... on ShareUserInfo {\n      firstName\n      lastName\n      email\n      __typename\n    }\n    ... on ShareGroupInfo {\n      name\n      ad\n      objectSid\n      everyone\n      tenantEveryone\n      admins\n      __typename\n    }\n    __typename\n  }\n  buildDestination {\n    destination\n    database\n    schema\n    __typename\n  }\n  modelStatistics {\n    enableModelStatistics\n    maxTablesPerCollection\n    recollectStatsInterval\n    __typename\n  }\n  experiments {\n    ...experiments\n    __typename\n  }\n  acceleration {\n    parentModelOid\n    __typename\n  }\n  __typename\n}\n\nfragment experiments on Experiments {\n  __typename\n}\n",
        # "operationName": "_"}

        if self.operating_system == 'Linux':
            payload = {
                "operationName": "elasticubesMetadata",
                "variables": {
                    "tenantFilter": None,
                    "isViewMode": False
                },
                "query": "query elasticubesMetadata($tenantFilter: String, $isViewMode: Boolean) {\n  elasticubesMetadata(tenantFilter: $tenantFilter, isViewMode: $isViewMode) {\n    ...ecMetaData\n    __typename\n  }\n}\n\nfragment ecMetaData on ElasticubeMetadata {\n  oid\n  title\n  server\n  tenant {\n    _id\n    name\n    systemManagement\n    __typename\n  }\n  fiscal\n  serverId\n  hasDatasets\n  type\n  relationType\n  provider\n  importTime\n  hasDatasetsWithoutConnectionParameters\n  datasets {\n    oid\n    database\n    schemaName\n    connection {\n      id\n      __typename\n    }\n    __typename\n  }\n  set {\n    title\n    __typename\n  }\n  status\n  lastBuildTime\n  lastBuildStatus\n  lastSuccessfulBuildStartTime\n  lastSuccessfulManualBuildStartTime\n  lastSuccessfulBuildTime\n  lastPublishTime\n  lastUpdated\n  hasPendingChanges\n  nextBuildTime\n  creator {\n    id\n    firstName\n    lastName\n    __typename\n  }\n  sizeInMb\n  shares {\n    partyId\n    type\n    permission\n    ... on ShareUserInfo {\n      firstName\n      lastName\n      email\n      __typename\n    }\n    ... on ShareGroupInfo {\n      name\n      ad\n      objectSid\n      everyone\n      tenantEveryone\n      admins\n      __typename\n    }\n    __typename\n  }\n  buildDestination {\n    destination\n    database\n    schema\n    resultLimit\n    queryTimeout\n    __typename\n  }\n  modelStatistics {\n    enableModelStatistics\n    allTablesPerCollection\n    maxTablesPerCollection\n    __typename\n  }\n  experiments {\n    ...experiments\n    __typename\n  }\n  acceleration {\n    parentModelOid\n    __typename\n  }\n  analyticalEngine {\n    queryAeMode\n    customTranslationMode\n    generateDependenciesMode\n    __typename\n  }\n  __typename\n}\n\nfragment experiments on Experiments {\n  __typename\n}\n"
            }

        else:
            payload = {
                "query": "query _ {\n  elasticubesMetadata {\n    oid\n    title\n    server\n    serverId\n    type\n    relationType\n    provider\n    datasets {\n      connection {\n        id\n        autoRefresh\n        refreshRate\n        timeout\n        resultLimit\n        __typename\n      }\n      __typename\n    }\n    set {\n      title\n      __typename\n    }\n    status\n    lastBuildTime\n    lastBuildStatus\n    lastSuccessfulBuildStartTime\n    lastSuccessfulBuildTime\n    lastPublishTime\n    lastUpdated\n    nextBuildTime\n    creator {\n      id\n      __typename\n    }\n    shares {\n      partyId\n      permission\n type\n      ... on ShareUserInfo {\n        firstName\n        lastName\n        email\n        __typename\n      }\n      ... on ShareGroupInfo {\n        name\n        ad\n        objectSid\n        everyone\n        admins\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n",
                "operationName": "_"}

        return self.__request_post(endpoint, json.dumps(payload))
    def get_elasticubes(self):
        endpoint = '/v1/elasticubes/getElasticubes'

        return self.__request_get(endpoint)



    def export_datamodel_by_id(self, datamodel_oid):

        if self.operating_system == 'Linux':
            endpoint = f'/v2/datamodel-exports/schema'
            querystring = {"datamodelId": datamodel_oid, "type": "schema-latest"}
        else:
            endpoint = f'/v1/elasticubes/{datamodel_oid}/datamodel-exports/stream/schema'
            querystring = {}

        return self.__request_get(endpoint, querystring)

    def load_datamodel(self, title, server="LocalHost"):
        endpoint = '/v2/ecm'
        payload = (
                    "{\"query\":\"query loadElasticube($title: String!, $server: String) {\\n  elasticubeByTitle(title: $title, server: $server) {\\n    oid\\n    __typename\\n  }\\n}\\n\","
                    "\"variables\":{\"title\":\"" + title + "\",\"server\":\"" + server + "\"}}")
        return self.__request_post(endpoint, payload)

    def import_datamodel(self, smodel):

        endpoint = '/v2/datamodel-imports/schema'

        return self.__request_post(endpoint, smodel)

    def delete_datamodel(self, dm_title, db_server):

        endpoint = '/v2/ecm'

        payload = {
            "query": "mutation removeElasticube($title: String!, $server: String!, $replacementDataSource: DataSourceInput) {\n  removeElasticube(title: $title, server: $server, replacementDataSource: $replacementDataSource)\n}\n",
            "variables": {
                "title": dm_title,
                "server": db_server
            },
            "operationName": "removeElasticube"
        }

        return self.__request_post(endpoint, json.dumps(payload))

    def move_dashboard_to_folder(self, dash_id, folder_id):

        endpoint = f"/dashboards/{dash_id}"

        payload = {"parentFolder": folder_id}

        return self.__request_put(endpoint, json.dumps(payload))

    def rename_dashboard(self, dash_id, new_title):
        endpoint = f"/dashboards/{dash_id}"
        payload = {"title": new_title}
        return self.__request_put(endpoint, json.dumps(payload))

    def get_dashboard_shares(self, dash_id):

        endpoint = f'/shares/dashboard/{dash_id}'
        querystring = {"adminAccess": "true"}
        return self.__request_get(endpoint, querystring)

    def get_dashboard_shares_v1(self, dash_id):
        endpoint = f'/v1/dashboards/{dash_id}/shares'
        querystring = {"adminAccess": "true"}
        return self.__request_get(endpoint, querystring)

    def share_dashboard(self, dash_id, payload):

        endpoint = f'/shares/dashboard/{dash_id}'

        # adminAccess is required for admins to update shares on dashboards they do not own
        # (previously only appended on Windows, which broke Linux/macOS migrations with 403).
        return self.__request_post(endpoint, payload, {"adminAccess": "true"})

    def publish_dashboard(self, dash_id):

        endpoint = f'/v1/dashboards/{dash_id}/publish?force=false&adminAccess=true'
        payload = {}
        return self.__request_post(endpoint, payload)

    def search_get_all_dashboards(self, limit = 100000, skip = 0):

        endpoint = "/v1/dashboards/searches"

        payload = {
            "queryParams": {
                "ownershipType": "allRoot",
                "search": "",
                "ownerInfo": True,
                "asObject": True
            },
            "queryOptions": {
                "sort": {"title": 1},
                "limit": limit,
                "skip": skip
            }
        }

        return self.__request_post(endpoint, json.dumps(payload))

    def search_get_dashboards_by_title(self, title):
        endpoint = "/v1/dashboards/searches"

        payload = {
            "queryParams": {
                "ownershipType": "allRoot",
                "search": title,
                "ownerInfo": True,
                "asObject": True
            },
            "queryOptions": {
                "sort": {"title": 1},
                "limit": 100000,
                "skip": 0
            }
        }

        return self.__request_post(endpoint, json.dumps(payload))

    def add_folder(self, payload):

        endpoint = "/v1/folders"

        return self.__request_post(endpoint, payload)

    def update_folder(self, folder_id, payload):
        endpoint = f"/v1/folders/{folder_id}"
        return self.__request_patch(endpoint, payload)

    def add_groups_bulk(self, payload):

        endpoint = '/v1/groups/bulk'
        return self.__request_post(endpoint, payload)

    def add_users_bulk(self, payload):

        endpoint = '/v1/users/bulk'
        return self.__request_post(endpoint, payload)

    def get_datasources(self):

        endpoint = "/datasources"
        querystring = {}

        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }

        url = self.server_base_url + endpoint

        response = None
        response = requests.request("GET", url, headers=headers, params=querystring, verify=self.session.verify)

        if response.reason == 'OK':
            return json.loads(response.text)
        else:
            raise SisenseRestAPIError(response)

    def get_datasource_measures(self, datasource_title, datasource_fullname):
        endpoint = f"/metadata/measures?datasource={datasource_title}&dsFullName={datasource_fullname}"
        return self.__request_get(endpoint)

    def get_datasource_dimensions(self, datasource_title, ds_full_name, server="LocalHost"):
        """
        Gets dimensions for a given datasource.

        Args:
            datasource_title (str): The title of the datasource (e.g., 'ECUBE_RPR').
            server (str): The server where the datasource resides (e.g., 'LocalHost').
            ds_full_name (str): The full name of the datasource (e.g., 'LocalHost/ECUBE_RPR').

        Returns:
            dict: JSON response with dimension data.
        """
        endpoint = "/metadata/dimensions"
        querystring = {
            "datasource": datasource_title,
            "server": server,
            "dsFullName": ds_full_name
        }
        return self.__request_get(endpoint, querystring)


    def add_datasource_measure(self, payload):

        endpoint = "/metadata/"

        return self.__request_post(endpoint, payload)

    def get_widgets(self, dashboard_id, fields=[]):

        endpoint = "/v1/dashboards/{0}/widgets".format(dashboard_id)

        querystring = {}

        if fields:
            delim = ","
            querystring["fields"] = delim.join(fields)

        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }

        url = self.server_base_url + endpoint

        response = None
        # Use (connect, read) so connection phase cannot hang indefinitely
        _timeout = (min(15, self.timeout), self.timeout) if isinstance(self.timeout, (int, float)) else self.timeout
        response = requests.request("GET", url, headers=headers, params=querystring, verify=self.session.verify, timeout=_timeout)

        if response.reason == 'OK':

            return json.loads(response.text)

        else:
            raise SisenseRestAPIError(response)

    def get_connections(self):
        endpoint = "/v2/connections"
        querystring = {}

        return self.__request_get(endpoint, querystring)

    def update_connection(self, connection_id, payload):
        endpoint = f"/v2/connections/{connection_id}"

        return self.__request_patch(endpoint, payload)

    def post_metadata_query(self, payload):
        """
        Posts a query to the /metadata endpoint.
        This is often used for complex queries like retrieving filter members.

        Args:
            payload (dict): The JSON payload for the metadata query.

        Returns:
            dict: JSON response from the server.
        """
        endpoint = "/metadata"
        return self.__request_post(endpoint, payload)


    def get_widget_by_id(self, dashboard_id, widget_id):

        endpoint = "/v1/dashboards/{0}/widgets/{1}".format(dashboard_id, widget_id)

        querystring = {}

        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }

        url = self.server_base_url + endpoint

        response = None
        response = requests.request("GET", url, headers=headers, params=querystring, verify=self.session.verify)

        if response.reason == 'OK':

            return json.loads(response.text)

        else:
            raise SisenseRestAPIError(response)

    def get_dashboard_by_id(self, dashboard_id, fields=[]):

        endpoint = "/v1/dashboards/{0}".format(dashboard_id)

        querystring = {}

        if fields:
            delim = ","
            querystring["fields"] = delim.join(fields)

        headers = {
            'accept': "application/json",
            'authorization': "Bearer " + self.__get_token(),
            'Accept-Encoding': "gzip, deflate"
        }

        url = self.server_base_url + endpoint

        response = None
        response = self.session.get(url, headers=headers, params=querystring)

        if response.reason == 'OK':
            return json.loads(response.text)
        else:
            raise SisenseRestAPIError(response)

    # For Linux env
    def elasticubes_run_jaql_csv(self, elasticube_title, metadata, error_reporting=False):
        endpoint = "/datasources/{0}/jaql/csv".format(elasticube_title, error_reporting)

        payload = "data={}".format(urllib.parse.quote(json.dumps(metadata)))
        headers = {
            'Accept': '*/*',
            # 'Content-Type': 'application/json;charset=UTF-8',
            'content-type': 'application/x-www-form-urlencoded',
            'Accept-Encoding': 'gzip, deflate',
            'authorization': "Bearer " + self.__get_token(),

        }

        url = self.server_base_url + endpoint
        response = requests.request("POST", url, headers=headers, data=payload, verify=self.session.verify)
        if response.reason == 'OK':
            print("****************>> " + response.headers._store['x-request-id'][1])
            return response.content.decode('ascii')

        else:
            raise SisenseRestAPIError(response)

    def get_blox_actions(self):

        if self.operating_system == 'Linux':
            endpoint = '/v1/blox/getCustomActions'
        else:
            # Windows endpoint
            endpoint = '/v1/getCustomActions/actions'
        
        return self.__request_get(endpoint)

    def import_blox_action(self, action):
        endpoint = '/v1/blox/saveCustomAction'
        return self.__request_post(endpoint, json.dumps(action))

    def start_backup(self, payload):
        endpoint = '/v2/backups'
        return self.__request_post(endpoint, payload)

    def get_backups(self, status=None):
        endpoint = f'/v2/backups{"?status="+status if status else ""}'
        return self.__request_get(endpoint)


class Error(Exception):
    # """Base class for exceptions in this module."""
    pass


class InputError(Error):

    def __init__(self, message):
        self.message = message


class SisenseRestAPIError(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, result):
        # self.endpoint = endpoint
        self.result = result
        # self.message = message
