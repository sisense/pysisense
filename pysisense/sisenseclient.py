import requests
import yaml
import urllib3
import logging
import re
import os
from .utils import convert_to_dataframe, export_to_csv as export_csv_util


class SisenseClient:

    def __init__(self, config_file="config.yaml", debug=False):
        """
        Initializes the SisenseClient with configuration, logging, and
        authorization headers.

        Parameters:
            config_file (str): Path to the YAML configuration file.
            debug (bool): Flag to enable debug-level logging.
        """
        # Load configuration from the YAML file
        self.config = self._load_config(config_file)

        # Get the domain or IP address from the configuration
        raw_domain = self.config['domain']
        # Strip protocol, port, and trailing slash
        cleaned = re.sub(r'^https?://', '', raw_domain).rstrip('/')
        self.domain = cleaned.split(':')[0]  # Remove port if present

        # Determine if SSL is enabled based on the configuration,
        # default is True (HTTPS)
        self.is_ssl = self.config.get('is_ssl', True)

        # Dynamically construct the base URL based on whether SSL is enabled
        if self.is_ssl:
            # Use default HTTPS (port 443)
            self.base_url = f"https://{self.domain}"
        else:
            self.base_url = (
                f"http://{self.domain}:30845"
            )  # Use non-SSL and specify port 30845

        # Extract the API token for authorization
        self.token = self.config['token']

        # Set up HTTP headers, including the Authorization Bearer token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }

        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set log level to DEBUG if debug is True, otherwise INFO
        log_level = logging.DEBUG if debug else logging.INFO
        log_file_path = os.path.join(log_dir, "pysisense.log")

        # Initialize the logger
        self.logger = self._get_logger(
            "SisenseClient", log_file_path, log_level
        )

        # Always disable SSL certificate verification
        self.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.logger.warning(
            "SSL verification is disabled. "
            "Avoid using this in production."
        )

    def _load_config(self, config_file):
        """
        Loads the configuration file in YAML format.

        Parameters:
            config_file (str): Path to the YAML configuration file.

        Returns:
            dict: Parsed YAML configuration as a dictionary.
        """
        # Open and parse the YAML file
        with open(config_file, 'r') as stream:
            return yaml.load(stream, Loader=yaml.FullLoader)

    def _get_logger(self, name, log_filename, log_level):
        """
        Sets up and configures a logger for the SisenseClient.

        Parameters:
            name (str): Name of the logger.
            log_filename (str): File path where logs will be saved.
            log_level (int): Logging level (DEBUG, INFO, etc.)

        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger(name)

        # Check if the logger already has handlers to avoid duplicates
        if not logger.handlers:
            # Create a file handler for the logger
            handler = logging.FileHandler(log_filename, mode='a')

            # Define the format for log messages
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)

            # Add the file handler to the logger
            logger.addHandler(handler)

        # Set the log level (DEBUG, INFO, etc.)
        logger.setLevel(log_level)

        return logger

    def get(self, endpoint, params=None):
        """
        Performs a GET request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            params (dict): Optional query parameters.

        Returns:
            requests.Response or None: The HTTP response object, or None if the
            request fails.

        """
        return self._make_request('GET', endpoint, params=params)

    def post(self, endpoint, data=None):
        """
        Performs a POST request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the POST request.

        Returns:
            requests.Response or None: The HTTP response object, or None if the
            request fails.

        """
        return self._make_request('POST', endpoint, data=data)

    def put(self, endpoint, data=None):
        """
        Performs a PUT request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PUT request.

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.

        """
        return self._make_request('PUT', endpoint, data=data)

    def patch(self, endpoint, data=None):
        """
        Performs a PATCH request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PATCH request.

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.

        """
        return self._make_request('PATCH', endpoint, data=data)

    def delete(self, endpoint):
        """
        Performs a DELETE request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.

        """
        return self._make_request('DELETE', endpoint)

    def _make_request(self, method, endpoint, params=None, data=None):
        """
        Makes an HTTP request to the API based on the specified method.

        Parameters:
            method (str): The HTTP method ('GET', 'POST', 'PUT',
                'PATCH', 'DELETE').
            endpoint (str): The API endpoint (relative to the base URL).
            params (dict): Optional query parameters (for GET requests).
            data (dict): Optional JSON data payload (for POST, PUT, PATCH requests).

        Returns:
            requests.Response or None: The full response object if the request succeeds, otherwise None if it fails.
        """
        # Construct the full URL for the API request
        url = f"{self.base_url}{endpoint}"

        # Log the request details (method, URL, params, and data)
        self.logger.debug(f"Making {method} request to {url} with data: {data} and params: {params}")

        try:
            # Perform the appropriate HTTP request based on the method
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params, verify=self.verify)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data, verify=self.verify)
            elif method == 'PUT':
                response = requests.put(url, headers=self.headers, json=data, verify=self.verify)
            elif method == 'PATCH':
                response = requests.patch(url, headers=self.headers, json=data, verify=self.verify)
            elif method == 'DELETE':
                response = requests.delete(url, headers=self.headers, verify=self.verify)
            else:
                # Raise an error for unsupported HTTP methods
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Handle known response codes
            if response.status_code in [200, 201, 204]:
                self.logger.debug(f"{method} request to {url} succeeded with status code {response.status_code}")
            elif response.status_code in [400, 404, 500]:
                # Log the error response text if available
                try:
                    error_message = response.json()
                except ValueError:
                    error_message = response.text  # If the response is not JSON, use raw text
                self.logger.error(
                    f"{method} request to {url} failed with status code "
                    f"{response.status_code}: {error_message}"
                )
            else:
                self.logger.warning(f"{method} request to {url} returned unexpected status code {response.status_code}")

            # Always return the full response object
            return response

        except requests.exceptions.RequestException as e:
            # Log and print the error for end-users
            error_message = f"{method} request to {url} failed: {e}"
            self.logger.error(error_message)
            return None

    def to_dataframe(self, data):
        """
        Converts a list of dictionaries, a single dictionary, or a simple list to a pandas DataFrame.
        Automatically handles flat and nested data.

        Parameters:
            data: dict, list of dicts, or a simple list

        Returns:
            DataFrame: A pandas DataFrame with the data flattened as much as possible.
        """
        return convert_to_dataframe(data, logger=self.logger)

    def export_to_csv(self, data, file_name="export.csv"):
        """
        Converts data to a DataFrame (handling nested structures) and exports it to a CSV file.

        Parameters:
            data: dict, list of dicts, or a simple list
            file_name: str, name of the file to export the CSV to
        """
        export_csv_util(data, file_name=file_name, logger=self.logger)
