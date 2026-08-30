from datetime import datetime

import pandas as pd
from pandas import json_normalize

# Key names (case-insensitive) whose values are replaced by redact_secrets().
_SENSITIVE_KEYS = {
    "password",
    "token",
    "secret",
    "value",
    "apisecret",
    "accesstoken",
    "refreshtoken",
    "clientsecret",
    "privatekey",
    "authorization",
}


def redact_secrets(data):
    """
    Recursively replaces values for credential-shaped keys with a placeholder so a dict/list is safe to write
    to a debug log. Returns a new structure; the input is left untouched.

    Parameters:
        data: dict, list, or any other value

    Returns:
        The same structure with sensitive values replaced by "***REDACTED***".
    """
    if isinstance(data, dict):
        return {key: ("***REDACTED***" if str(key).lower() in _SENSITIVE_KEYS else redact_secrets(value)) for key, value in data.items()}
    if isinstance(data, list):
        return [redact_secrets(item) for item in data]
    return data


# Maximum characters of a raw error body carried into a failure message.
_MAX_ERROR_REASON_CHARS = 300


def _extract_error_message(response, context, api_client=None):
    """
    Builds a standardized failure dict from a Sisense API response.

    Distinguishes three failure kinds:
    - HTTP error with a body: relays the Sisense reason and the HTTP status.
      The reason is taken best-effort from the JSON body ("detail", then
      "message", then "title", then "error"), falling back to the raw body
      text truncated to a safe length.
    - HTTP error with an empty body: says so, with the HTTP status.
    - No response at all (connection failure): names the target domain and
      carries no invented status code.

    The body is passed through redact_secrets() before use so credential-shaped
    values never reach the returned message.

    Parameters:
        response: requests.Response or None (as returned by SisenseClient requests)
        context (str): What the caller was doing, e.g. "Failed to retrieve dashboards"
        api_client: optional SisenseClient, used to name the target domain when
            there is no response

    Returns:
        dict: {"ok": False, "error": str} always; plus {"status_code": int}
        when an HTTP status is available. The explicit "ok": False marker is
        the forward-compatible failure signal — consumers detect failures via
        payload.get("ok") is False (or the presence of "error"), never by
        matching an exact key set, since additive keys may arrive in any
        release.
    """
    if response is None:
        domain = getattr(api_client, "domain", None) or "the Sisense server"
        return {"ok": False, "error": f"{context}: no response from {domain} — connection failed"}

    status = response.status_code
    reason = None
    try:
        body = redact_secrets(response.json())
    except ValueError:
        raw_text = (response.text or "").strip()
        if raw_text:
            reason = raw_text
    else:
        if isinstance(body, dict):
            for key in ("detail", "message", "title", "error"):
                value = body.get(key)
                if isinstance(value, str) and value.strip():
                    reason = value.strip()
                    break
            if reason is None and body:
                reason = str(body)
        elif body not in (None, "", [], {}):
            reason = str(body)

    if reason is None:
        reason = "the server returned an empty error body"
    elif len(reason) > _MAX_ERROR_REASON_CHARS:
        reason = reason[:_MAX_ERROR_REASON_CHARS] + "…"

    return {"ok": False, "error": f"{context}: {reason} (HTTP {status})", "status_code": status}


def convert_to_dataframe(data, logger=None):
    """
    Converts a list of dictionaries, a single dictionary, or a simple list to a pandas DataFrame.
    Automatically handles flat and nested structures.

    Parameters:
        data: dict, list of dicts, or a simple list
        logger: logging.Logger, optional logger for capturing debug/error output

    Returns:
        DataFrame: A pandas DataFrame with the data flattened as much as possible,
                   or None if conversion fails.
    """
    try:
        if isinstance(data, dict):
            df = json_normalize(data)
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                df = json_normalize(data) if any(any(isinstance(value, dict) for value in item.values()) for item in data) else pd.DataFrame(data)
            elif all(not isinstance(item, dict) for item in data):
                df = pd.DataFrame(data, columns=["Column_A"])
            else:
                raise ValueError("Data contains mixed types. Expected either a list of dictionaries or a simple list.")
        else:
            raise ValueError("Data must be a dictionary, list of dictionaries, or a plain list.")

        return df

    except ValueError as e:
        message = f"Data conversion failed: {e}"
        if logger:
            logger.error(message)
        return None


def export_to_csv(data, file_name="export.csv", logger=None):
    """
    Converts data to a DataFrame and exports it to a CSV file.

    Parameters:
        data: dict, list of dicts, or a simple list
        file_name (str): Name of the CSV file to export
        logger: logging.Logger, optional logger for capturing debug/error output
    """
    try:
        df = convert_to_dataframe(data, logger=logger)

        if df is not None:
            df.to_csv(file_name, index=False)
            message = f"Data successfully exported to {file_name}"
            if logger:
                logger.info(message)
        else:
            message = "Failed to export data due to invalid input format."
            if logger:
                logger.warning(message)

    except ValueError as e:
        message = f"Data export to CSV failed: {e}"
        if logger:
            logger.error(message)


def convert_utc_to_local(utc_str):
    """
    Converts a UTC timestamp string to the system's local timezone.
    Assumes the input is in ISO 8601 format with 'Z' suffix.

    Parameters:
        utc_str (str): A UTC timestamp string, e.g., '2025-05-14T16:24:33.537Z'

    Returns:
        str: Formatted local timestamp, e.g., '2025-05-14 12:24:33 EDT',
             or None if input is invalid.
    """
    if not utc_str:
        return None
    try:
        utc_time = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        local_time = utc_time.astimezone()
        return local_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception as e:
        return f"Invalid timestamp: {utc_str} - {str(e)}"
