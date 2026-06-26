import logging
import os
import re

import requests
import urllib3
import yaml

from .utils import convert_to_dataframe
from .utils import export_to_csv as export_csv_util

DEFAULT_NON_SSL_PORT = 30845
DEFAULT_NON_SSL_PORT_WINDOWS = 8081
VALID_OPERATING_SYSTEMS = frozenset({"linux", "windows"})
# Values from a YAML config or kwarg that are treated as "not set" → default to linux
_OS_ABSENT_VALUES = frozenset({"", "none", "na", "n/a", "null", "undefined"})


class SisenseClient:
    def __init__(
        self,
        config_file: str | None = "config.yaml",
        debug: bool = False,
        *,
        domain: str | None = None,
        token: str | None = None,
        is_ssl: bool | None = None,
        port: int | None = None,
        operating_system: str = "linux",
    ):
        """
        Initializes the SisenseClient with configuration, logging, and
        authorization headers.

        Two supported patterns:

        1) Legacy YAML-based usage (backward compatible):
            client = SisenseClient(config_file="config.yaml", debug=False)

           The YAML must contain:
             - domain: "https://your-domain.sisense.com"
             - token: "YOUR_API_TOKEN"
             - is_ssl: true  # optional, defaults to True
             - port: 30845  # optional; HTTP port when is_ssl is false (default 30845)

        2) Direct connection (no YAML, ideal for inline/runtime usage):
            client = SisenseClient(
                domain="https://your-domain.sisense.com",
                token="YOUR_API_TOKEN",
                is_ssl=True,          # optional, defaults to True
                debug=True,
            )

        Rules:
        - If domain and token are provided, they take precedence and config_file is
          ignored (connection is built from args).
        - Otherwise, config_file is required and used as before.

        Parameters:
            config_file (str): Path to the YAML configuration file. Ignored if
                both domain and token are provided.
            debug (bool): Flag to enable debug-level logging.
            domain (str | None): Sisense base URL or hostname. If provided together
                with token, inline config is used instead of YAML.
            token (str | None): API token for Sisense. If provided together with
                domain, inline config is used instead of YAML.
            is_ssl (bool | None): Whether to use HTTPS (True) or HTTP (False).
                If None and inline config is used, defaults to True.
            port (int | None): HTTP port for non-SSL connections. Ignored when
                ``is_ssl`` is True. Defaults to 30845 when omitted.
            operating_system (str): Target Sisense server OS. Accepted values are
                ``"linux"`` and ``"windows"``. Defaults to ``"linux"``. Some API
                endpoints and payload shapes differ between deployments; this flag
                controls which variant is used. Can also be set via the
                ``operating_system`` key in the YAML config file (the YAML value
                takes precedence over this argument when both are present).
        """
        # Decide how to build the base config
        if domain is not None or token is not None or is_ssl is not None or port is not None:
            # Direct connection mode – require domain + token
            if not domain or not token:
                raise ValueError("When using direct connection, both 'domain' and 'token' must be provided.")

            self.config = {
                "domain": domain,
                "token": token,
                # if is_ssl is None, default to True
                "is_ssl": True if is_ssl is None else bool(is_ssl),
            }
            if port is not None:
                self.config["port"] = port
        else:
            # Legacy YAML mode
            if not config_file:
                raise ValueError("config_file must be provided when 'domain' and 'token' are not supplied.")
            self.config = self._load_config(config_file)

        # Resolve operating_system: YAML config takes precedence over the kwarg.
        # Blank, null, "none", "NA", and similar absent-looking values all fall
        # back to "linux" so that an unmodified config.yaml continues to work.
        raw_os = self.config.get("operating_system", operating_system)
        normalized_os = str(raw_os).lower().strip() if raw_os is not None else ""
        if normalized_os in _OS_ABSENT_VALUES:
            normalized_os = "linux"
        if normalized_os not in VALID_OPERATING_SYSTEMS:
            raise ValueError(f"Invalid operating_system '{raw_os}'. Must be one of: {sorted(VALID_OPERATING_SYSTEMS)}")
        self.operating_system: str = normalized_os

        # Get the domain or IP address from the configuration
        raw_domain = self.config["domain"]
        # Strip protocol, port, and trailing slash
        cleaned = re.sub(r"^https?://", "", raw_domain).rstrip("/")
        self.domain = cleaned.split(":")[0]  # Remove port if present

        # Determine if SSL is enabled based on the configuration,
        # default is True (HTTPS)
        self.is_ssl = bool(self.config.get("is_ssl", True))

        # Dynamically construct the base URL based on whether SSL is enabled
        if self.is_ssl:
            # Use default HTTPS (port 443)
            self.base_url = f"https://{self.domain}"
        else:
            http_port = self._non_ssl_port()
            self.base_url = f"http://{self.domain}:{http_port}"

        # Extract the API token for authorization
        self.token = self.config["token"]

        # Set up HTTP headers, including the Authorization Bearer token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        # Logging setup
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Set log level to DEBUG if debug is True, otherwise INFO
        log_level = logging.DEBUG if debug else logging.INFO
        log_file_path = os.path.join(log_dir, "pysisense.log")

        # Initialize the logger
        self.logger = self._get_logger("SisenseClient", log_file_path, log_level)

        # Always disable SSL certificate verification (current behavior)
        self.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.logger.warning("SSL verification is disabled. Avoid using this in production.")

    @classmethod
    def from_connection(
        cls,
        domain: str,
        token: str,
        is_ssl: bool = True,
        port: int | None = None,
        debug: bool = False,
        operating_system: str = "linux",
    ) -> "SisenseClient":
        """
        Convenience alternative constructor for direct connection usage.

        Example:
            client = SisenseClient.from_connection(
                domain="https://your-domain.sisense.com",
                token="YOUR_API_TOKEN",
                is_ssl=True,
                debug=True,
                operating_system="linux",
            )
        """
        return cls(
            config_file=None,
            debug=debug,
            domain=domain,
            token=token,
            is_ssl=is_ssl,
            port=port,
            operating_system=operating_system,
        )

    def _non_ssl_port(self) -> int:
        """Return the HTTP port for non-SSL connections.

        Uses the ``port`` key from config when present. Otherwise defaults to
        ``8081`` for Windows deployments and ``30845`` for Linux.
        """
        raw = self.config.get("port")
        if raw is None:
            return DEFAULT_NON_SSL_PORT_WINDOWS if self.operating_system == "windows" else DEFAULT_NON_SSL_PORT
        return int(raw)

    def _load_config(self, config_file):
        """
        Loads the configuration file in YAML format.

        Parameters:
            config_file (str): Path to the YAML configuration file.

        Returns:
            dict: Parsed YAML configuration as a dictionary.
        """
        # Open and parse the YAML file
        with open(config_file) as stream:
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
            handler = logging.FileHandler(log_filename, mode="a")

            # Define the format for log messages
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)

            # Add the file handler to the logger
            logger.addHandler(handler)

        # Set the log level (DEBUG, INFO, etc.)
        logger.setLevel(log_level)

        return logger

    def get(self, endpoint, params=None, extra_headers=None):
        """
        Performs a GET request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            params (dict): Optional query parameters.
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The HTTP response object, or None if the
            request fails.
        """
        return self._make_request("GET", endpoint, params=params, extra_headers=extra_headers)

    def post(self, endpoint, data=None, extra_headers=None):
        """
        Performs a POST request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the POST request.
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The HTTP response object, or None if the
            request fails.
        """
        return self._make_request("POST", endpoint, data=data, extra_headers=extra_headers)

    def put(self, endpoint, data=None, extra_headers=None):
        """
        Performs a PUT request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PUT request.
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.
        """
        return self._make_request("PUT", endpoint, data=data, extra_headers=extra_headers)

    def patch(self, endpoint, data=None, extra_headers=None):
        """
        Performs a PATCH request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PATCH request.
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.
        """
        return self._make_request("PATCH", endpoint, data=data, extra_headers=extra_headers)

    def delete(self, endpoint, extra_headers=None):
        """
        Performs a DELETE request to the specified API endpoint.

        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The HTTP response object, or None if the request fails.
        """
        return self._make_request("DELETE", endpoint, extra_headers=extra_headers)

    def _make_request(self, method, endpoint, params=None, data=None, extra_headers=None):
        """
        Makes an HTTP request to the API based on the specified method.

        Parameters:
            method (str): The HTTP method ('GET', 'POST', 'PUT',
                'PATCH', 'DELETE').
            endpoint (str): The API endpoint (relative to the base URL).
            params (dict): Optional query parameters (for GET requests).
            data (dict): Optional JSON data payload (for POST, PUT, PATCH requests).
            extra_headers (dict): Optional headers merged into the default request headers.

        Returns:
            requests.Response or None: The full response object if the request succeeds,
            otherwise None if it fails.
        """
        # Construct the full URL for the API request
        url = f"{self.base_url}{endpoint}"
        headers = dict(self.headers)
        if extra_headers:
            headers.update(extra_headers)

        # Log the request details (method, URL, params, and data)
        self.logger.debug(f"Making {method} request to {url} with data: {data} and params: {params}")

        try:
            # Perform the appropriate HTTP request based on the method
            if method == "GET":
                response = requests.get(url, headers=headers, params=params, verify=self.verify)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=data, verify=self.verify)
            elif method == "PUT":
                response = requests.put(url, headers=headers, json=data, verify=self.verify)
            elif method == "PATCH":
                response = requests.patch(url, headers=headers, json=data, verify=self.verify)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, verify=self.verify)
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
                    # If the response is not JSON, use raw text
                    error_message = response.text
                self.logger.error(f"{method} request to {url} failed with status code {response.status_code}: {error_message}")
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
