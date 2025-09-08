import pandas as pd
from pandas import json_normalize
from datetime import datetime


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
                if any(any(isinstance(value, dict) for value in item.values()) for item in data):
                    df = json_normalize(data)
                else:
                    df = pd.DataFrame(data)
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
        print(message)
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
            print(message)
            if logger:
                logger.info(message)
        else:
            print("Failed to export data due to invalid input format.")

    except ValueError as e:
        message = f"Data export to CSV failed: {e}"
        if logger:
            logger.error(message)
        print(message)


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
