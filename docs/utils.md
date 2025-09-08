Utils Module Documentation
==========================

This module provides reusable data utilities to support the SDK, including conversion to DataFrames, CSV export, and timestamp localization.

* * * * *

Function: `convert_to_dataframe(data, logger=None)`
---------------------------------------------------

Converts various data structures to a flattened pandas DataFrame.

**Parameters:**

-   `data`: Accepts a list of dicts, single dict, or simple list.

-   `logger` (Logger, optional): Optional logger instance for error/debug output.

**Returns:**

-   `DataFrame`: A structured pandas DataFrame or `None` if conversion fails.

* * * * *

Function: `export_to_csv(data, file_name="export.csv", logger=None)`
--------------------------------------------------------------------

Converts the data into a DataFrame and writes it to a CSV file.

**Parameters:**

-   `data`: Compatible data structure (list of dicts, dict, or list).

-   `file_name` (str): Target file path (default: `export.csv`).

-   `logger` (Logger, optional): Logger for debug or error messages.

**Returns:**

-   None

**Used by:** `SisenseClient.export_to_csv()`  

* * * * *

Function: `convert_utc_to_local(utc_str)`
-----------------------------------------

Converts a UTC timestamp string (ISO 8601 with 'Z') to the local time zone.

**Parameters:**

-   `utc_str` (str): Example input --- `'2025-05-14T16:24:33.537Z'`

**Returns:**

-   `str`: Local time formatted as `'YYYY-MM-DD HH:MM:SS TZ'`, or error message on failure.