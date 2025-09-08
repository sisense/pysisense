# SisenseClient Module Documentation

This module defines the `SisenseClient` class, which handles low-level interactions with Sisense APIs.  
It supports HTTP methods, YAML-based configuration, logging, and helper functions for data export and transformation.

---

## Class: `SisenseClient`

### `__init__(self, config_file="config.yaml", debug=False)`

Initializes the Sisense client, sets up logging, and prepares headers using a YAML config file.

**Parameters:**

- `config_file` (str): Path to the YAML config file.  
- `debug` (bool): If True, enables debug logging.

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
