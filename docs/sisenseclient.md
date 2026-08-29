# SisenseClient Module Documentation

This module defines the `SisenseClient` class, which handles low-level interactions with Sisense APIs.  
It supports HTTP methods, YAML-based configuration, logging, and helper functions for data export and transformation.

---

## Class: `SisenseClient`

### `__init__(self, config_file="config.yaml", debug=False, *, domain=None, token=None, is_ssl=None, port=None, operating_system="linux", verify_ssl=None, ssl_path=None, retries=None, timeout=None, connect_timeout=None)`

Initializes the Sisense client, sets up logging, and prepares headers. Supports YAML-based config or direct inline connection.

**Parameters:**

- `config_file` (str): Path to the YAML config file. Ignored when `domain` and `token` are provided directly.
- `debug` (bool): If True, enables debug logging.
- `domain` (str, optional): Sisense hostname or IP. When provided together with `token`, YAML config is bypassed.
- `token` (str, optional): Sisense admin API token for direct connection mode.
- `is_ssl` (bool, optional): `True` for HTTPS, `False` for HTTP. Defaults to `True` in direct mode.
- `port` (int, optional): HTTP port for non-SSL connections. Defaults to `30845` (Linux) or `8081` (Windows) when omitted.
- `operating_system` (str): Target Sisense server OS. `"linux"` (default) or `"windows"`. Controls OS-specific API endpoint routing and default non-SSL port. Can also be set via `operating_system:` in the YAML config file — the YAML value takes precedence. Blank, `null`, `none`, or `NA` values all fall back to `"linux"`.
- `verify_ssl` (bool, optional): Whether to verify the server's TLS certificate. Defaults to `True`. Can also be set via `verify_ssl:` in the YAML config file. Disabling it logs a warning and raises a `UserWarning`, only do so for trusted internal networks with self-signed certificates.
- `ssl_path` (str, optional): Path to a CA bundle file or directory used to verify the server's TLS certificate (e.g. a self-signed or internal CA's `.pem` file). Can also be set via `ssl_path:` in the YAML config file. Takes precedence over `verify_ssl` when both are set, unless `verify_ssl` is explicitly `False`.
- `retries` (bool, optional): Whether to automatically retry requests that fail with a transient server error (HTTP 429, 500, 502, 503, or 504), using exponential backoff. Defaults to `True`. Can also be set via `retries:` in the YAML config file; this argument overrides the config value whenever it is explicitly passed. Only idempotent methods (GET, PUT, DELETE) are retried, POST and PATCH are never retried automatically. Connection and read timeouts are never retried.
- `timeout` (float, optional): Client-side **read timeout** in seconds for every request. Defaults to `30`. Can also be set via `timeout:` in the YAML config file. Without it, a slow server holds each request until its own gateway timeout (observed at 300s per attempt on live instances). Lower it for read/export-heavy workloads that must fail fast; keep it generous for long-running operations (builds, bulk imports, large exports) and for **writes** — a client-side timeout on a POST/PATCH leaves the server outcome ambiguous (the change may still have been applied).
- `connect_timeout` (float, optional): Client-side **TCP connect timeout** in seconds. Defaults to `5`. Can also be set via `connect_timeout:` in the YAML config file.

**Note:** `from_connection(domain, token, ...)` is a classmethod alternative constructor for direct connection mode.

---

### `_load_config(self, config_file)`

Loads configuration from a YAML file.

**Parameters:**

- `config_file` (str): Path to the YAML config file.

**Returns:**

- `dict`: Parsed configuration dictionary.

---

### `_get_logger(self, name, log_filename, log_level)`

Sets up a file-based logger.

**Parameters:**

- `name` (str): Logger name.  
- `log_filename` (str): Path to log file.  
- `log_level` (int): Logging level.

**Returns:**

- `Logger`: A configured logger instance.

---

### `get(self, endpoint, params=None)`

Makes a GET request to a given API endpoint.

**Parameters:**

- `endpoint` (str): Relative endpoint path.  
- `params` (dict): Optional query parameters.

**Returns:**

- `Response`: HTTP response object.

---

### `post(self, endpoint, data=None)`

Makes a POST request to the API.

**Parameters:**

- `endpoint` (str): Relative endpoint path.  
- `data` (dict): Optional JSON payload.

**Returns:**

- `Response`: HTTP response object.

---

### `put(self, endpoint, data=None)`

Makes a PUT request to the API.

**Parameters:**

- `endpoint` (str): Relative endpoint path.  
- `data` (dict): Optional JSON payload.

**Returns:**

- `Response`: HTTP response object.

---

### `patch(self, endpoint, data=None)`

Makes a PATCH request to the API.

**Parameters:**

- `endpoint` (str): Relative endpoint path.  
- `data` (dict): Optional JSON payload.

**Returns:**

- `Response`: HTTP response object.

---

### `delete(self, endpoint)`

Makes a DELETE request to the API.

**Parameters:**

- `endpoint` (str): Relative endpoint path.

**Returns:**

- `Response`: HTTP response object.

---

### `_make_request(self, method, endpoint, params=None, data=None)`

General-purpose internal request method.

**Parameters:**

- `method` (str): One of 'GET', 'POST', 'PUT', 'PATCH', 'DELETE'.  
- `endpoint` (str): API path.  
- `params` (dict): Optional query params.  
- `data` (dict): Optional payload.

**Returns:**

- `Response`: Full HTTP response object or None on failure.

---

### `to_dataframe(self, data)`

Converts raw API data into a flattened pandas DataFrame.

**Parameters:**

- `data`: List, dict, or simple list structure.

**Returns:**

- `DataFrame`: Flattened DataFrame.

---

### `export_to_csv(self, data, file_name="export.csv")`

Exports structured data to CSV using the internal utility function.

**Parameters:**

- `data`: dict, list of dicts, or simple list  
- `file_name` (str): CSV filename

**Notes:**

- Internally uses `utils.export_to_csv()` for flattening and writing.  
- Automatically applies class-level logging.

---

### `decode_bearer_token(self)`

Decodes the JWT bearer token stored on this client. Extracts the payload segment, base64url-decodes it, and returns all claims as a plain dictionary. No network request is made; decoding is performed locally.

This is an inspection utility: the signature is not verified, and the claim names are internal details of the Sisense token format. To resolve the API token user's ID for other SDK calls, prefer `AccessManagement.get_my_user()`, which asks the server directly.

**Returns:**

- `dict`: All JWT payload claims. Common keys:
  - `"user"` (str): Sisense user ID of the token owner.
  - `"exp"` (int): Token expiry as a Unix timestamp.
  - `"iat"` (int): Token issued-at as a Unix timestamp.

  Returns `{"error": "..."}` when the token is missing, malformed, or cannot be decoded.
