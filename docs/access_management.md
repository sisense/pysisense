AccessManagement Module Documentation
=====================================

This module provides programmatic access to manage Sisense users, groups, dashboards, permissions, and folder ownership through the `AccessManagement` class.

Class: `AccessManagement`
-------------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the AccessManagement class.

**Parameters:**

-   `api_client` (APIClient, optional): An existing APIClient instance. If None, a new APIClient is created.

-   `debug` (bool, optional): Enables debug logging if True. Default is False.

* * * * *

### `get_user(self, user_name)`

Retrieves user details by their email (username) and expands the response to include group and role information.

**Parameters:**

-   `user_name` (str): The email or username of the user to be retrieved.

**Returns:**

-   `dict`: User details on success, or `{'error': 'message'}` on failure or if not found.

* * * * *

### `get_users_all(self)`

Fetches all users along with tenant, group, and role information.

**Returns:**

-   `list`: List of user dictionaries or list containing one dict with an 'error' key.

* * * * *

### `get_group(self, name)`

Retrieves group details by name.

**Parameters:**

-   `name` (str): Group name.

**Returns:**

-   `dict`: Group details or error message.

* * * * *

### `create_user(self, user_data)`

Creates a new user by converting group and role names into IDs.

**Parameters:**

-   `user_data` (dict): Includes email, name, role name, groups, preferences.

**Returns:**

-   `dict`: API response or error message.

* * * * *

### `update_user(self, user_name, user_data)`

Updates a user's attributes by username.

**Parameters:**

-   `user_name` (str): Username or email.

-   `user_data` (dict): Fields to update (e.g., groups, role).

**Returns:**

-   `dict`: API response or error message.

* * * * *

### `delete_user(self, user_name)`

Deletes a user by their email or username.

**Parameters:**

-   `user_name` (str): User identifier.

**Returns:**

-   `dict`: Success or error message.

* * * * *

### `users_per_group(self, group_name)`

Returns all usernames for a given group.

**Parameters:**

-   `group_name` (str): Group name.

**Returns:**

-   `dict`: Group name and associated usernames or error.

* * * * *

### `users_per_group_all(self)`

Maps all groups to users, excluding system groups and reassigning admin roles to "Admins".

**Returns:**

-   `list`: Each entry is a dict with a group name and associated usernames.

* * * * *

### `change_folder_and_dashboard_ownership(self, executing_user, folder_name, new_owner_name, original_owner_rule='edit', change_dashboard_ownership=True)`

Changes ownership of folders and optionally dashboards.

**Parameters:**

-   `executing_user` (str): Admin user executing the change.

-   `folder_name` (str): Folder to transfer ownership.

-   `new_owner_name` (str): Recipient of the ownership.

-   `original_owner_rule` (str): Permission to assign to original owner ('edit' or 'view').

-   `change_dashboard_ownership` (bool): Whether to include dashboards.

**Returns:**

-   None (logs and updates executed internally).

* * * * *

### `get_datamodel_columns(self, datamodel_name)`

Extracts all columns from the datasets and tables of a specified DataModel.

**Parameters:**

-   `datamodel_name` (str): Name of the DataModel.

**Returns:**

-   `list`: List of dictionaries with model ID, name, table, and column.

* * * * *

### `get_unused_columns(self, datamodel_name)`

Identifies unused columns in a DataModel by comparing against dashboard usage.

**Parameters:**

-   `datamodel_name` (str): Name of the DataModel.

**Returns:**

-   `list`: Each entry includes table, column, and a 'used' flag.

**Limitations:**

-   Assumes API user has full dashboard access.

* * * * *

### `get_all_dashboard_shares(self)`

Retrieves all dashboard share settings, including user and group shares.

**Returns:**

-   `list`: Dashboard title, share type, and share name.

* * * * *

### `create_schedule_build(self, datamodel_name, build_type="ACCUMULATE", *, days=None, hour=None, minute=None, interval_days=None, interval_hours=None, interval_minutes=None)`

Schedules a build for a DataModel. Supports both:
- **Cron-based schedules** (e.g., specific days and time in UTC)
- **Interval-based schedules** (e.g., every N days/hours/minutes)

**Parameters:**

- `datamodel_name` (str): Name of the DataModel to schedule a build for.
- `build_type` (str): Type of build (`"ACCUMULATE"`, `"FULL"`, `"SCHEMA_CHANGES"`). Default is `"ACCUMULATE"`.
- `days` (list, optional): Days of the week to schedule the build (e.g., `["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]` or `["*"]` for all days).
- `hour` (int, optional): Hour of the day in UTC (0–23) for cron-based schedule.
- `minute` (int, optional): Minute of the hour in UTC (0–59) for cron-based schedule.
- `interval_days` (int, optional): Interval in days for interval-based schedule.
- `interval_hours` (int, optional): Interval in hours for interval-based schedule.
- `interval_minutes` (int, optional): Interval in minutes for interval-based schedule.

**Returns:**

- `dict`: API response confirming schedule creation or error details.
