AccessManagement Module Documentation
=====================================

This module provides programmatic access to manage Sisense users, groups, dashboards, permissions, and folder ownership through the `AccessManagement` class.

Every failure return in this module is a dict of the form `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}`. The shorthand `{"ok": False, "error": "..."}` below always refers to this failure dict.

> **Coming from 1.x?** The canonical user row is **additive**: `ROLE_NAME` and `GROUPS` keep their 1.x names and meanings, joined by the new `ROLE_DISPLAY_NAME` (same value as `ROLE_NAME`, unambiguously named), `ROLE_RAW_NAME` (the raw Sisense role value) and `GROUP_IDS`. The one changed value is that `GROUPS` now includes `Everyone`, which `get_users_all()` used to strip. See the [upgrade guide](upgrading.md).

Class: `AccessManagement`
-------------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the AccessManagement class.

**Parameters:**

-   `api_client` (APIClient, optional): An existing APIClient instance. If None, a new APIClient is created.

-   `debug` (bool, optional): Enables debug logging if True. Default is False.

* * * * *

### `get_user(self, user_email)`

Retrieves user details by email address and returns the canonical user row, carrying both role vocabularies and both group IDs and names.

**Parameters:**

-   `user_email` (str, **required**): Email address of the user to be retrieved.

**Returns:**

-   `dict`: The canonical user row:
    - `USER_ID`
    - `USER_NAME`
    - `EMAIL`
    - `FIRST_NAME`
    - `LAST_NAME`
    - `IS_ACTIVE`
    - `ROLE_ID`
    - `ROLE_NAME` — the name the Sisense UI shows (`viewer`, `sysAdmin`, `dashboardDesigner`); unchanged from 1.x
    - `ROLE_DISPLAY_NAME` — the same value, under a name that states which vocabulary it is
    - `ROLE_RAW_NAME` — the **raw** Sisense role value (`consumer`, `super`, `contributor`)
    - `GROUP_IDS` (list of group IDs)
    - `GROUPS` (list of group names)

    `GROUP_IDS`/`GROUPS` are unfiltered — the `Everyone` group **is** included (the SDK reports what Sisense says; consumers decide what to hide).

    Group membership is read from the **group** side (`GET /api/v1/groups?expand=users`), the same source `users_per_group` and the Sisense UI use, and is always the **complete** list — including `Everyone` and `All users in system`. Only `users_per_group()`'s all-groups view omits those two, for readability, so use `get_user` (not that view) to answer "which groups is this person in". Sisense resolves its auto-generated groups (`Admins`, `All users in system`) there and never writes them into a user's own record, so reading the user record alone would under-report membership and disagree with `users_per_group` about the same person. If the group fetch fails, the fields fall back to the user record rather than coming back empty.

    On failure or if the user is not found, returns `{"ok": False, "error": "..."}`.

* * * * *

### `get_my_user()`

Retrieves the currently logged-in user for the API token (``GET /api/users/loggedin``). Use for migration user identity resolution.

**Returns:**

-   `dict`: Logged-in user object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_roles()`

Retrieves all Sisense roles (``GET /api/roles``). Use to build role name-to-ID maps for multi-tenant migration.

**Returns:**

-   `list`: Role objects on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `change_user_password(user_id, password)`

Changes a user's password via ``PATCH /api/users/{user_id}``. Only the ``password`` field is sent in the request body.

**Parameters:**

-   `user_id` (str): Internal user ID (``_id``).
-   `password` (str): New password (must not be empty).

**Returns:**

-   `dict`: Updated user object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_user_with_role_and_group_names(self, user_name)`

> **Deprecated** — use [`get_user`](#get_userself-user_email) instead. This alias is
> marked deprecated (PEP 702) and its behavior is frozen until removal.
> `get_user` returns the canonical user row, which already carries both role
> and group **IDs and names**.

**Parameters:**

-   `user_name` (str): The email or username of the user to be retrieved.

**Returns:**

-   `dict`: User details including `USER_ID`, `USER_NAME`, `FIRST_NAME`,
    `LAST_NAME`, `EMAIL`, `IS_ACTIVE`, `ROLE_ID`, `ROLE_NAME`, `GROUP_IDS`,
    and `GROUP_NAMES`, or `{"ok": False, "error": "..."}` if the user is not
    found or the API call fails.

* * * * *

### `get_users_all(self)`

Fetches all users and returns one canonical user row per user.

**Returns:**

-   `list`: One canonical row per user, each with `USER_ID`, `USER_NAME`,
    `EMAIL`, `FIRST_NAME`, `LAST_NAME`, `IS_ACTIVE`, `ROLE_ID`,
    `ROLE_NAME` and `ROLE_DISPLAY_NAME` (UI name, e.g. `viewer`),
    `ROLE_RAW_NAME` (raw value, e.g. `consumer`), `GROUP_IDS`, and `GROUPS`
    (unfiltered — the `Everyone` group **is** included).

    An empty list means the instance genuinely has zero users. On failure,
    returns a plain `{"ok": False, "error": "..."}` dict (no longer a
    list-wrapped `[{"error": ...}]`).

* * * * *

### `get_users_with_role_names_and_group_names(self)`

> **Deprecated** — use [`get_users_all`](#get_users_allself) instead. This alias is
> marked deprecated (PEP 702) and its behavior is frozen until removal.
> `get_users_all` returns canonical rows that carry both the UI role name
> (`ROLE_NAME`/`ROLE_DISPLAY_NAME`) and the raw role value (`ROLE_RAW_NAME`),
> plus group IDs and names.

**Returns:**

-   `list`: Each entry is a dictionary containing `USER_ID`, `USER_NAME`,
    `FIRST_NAME`, `LAST_NAME`, `EMAIL`, `IS_ACTIVE`, `ROLE_ID`,
    `ROLE_NAME` (the **raw** Sisense role name, e.g. `"consumer"`),
    `GROUP_IDS`, and `GROUP_NAMES`, or a single-item list with an error dict
    if an API call fails (frozen deprecated-alias behavior).

### `get_users_expanded(self)`

> **Deprecated** — use [`get_users_all`](#get_users_allself) instead. This alias is
> marked deprecated (PEP 702) and its behavior is frozen until removal.
> `get_users_all` canonical rows already expose the raw role value in
> `ROLE_RAW_NAME` alongside the UI name in `ROLE_DISPLAY_NAME`.

Retrieves all users with raw, unmodified role and group objects (``GET /api/v1/users`` with ``groups`` and ``role`` expanded).

**Returns:**

-   `list` | `dict`: The raw list of user objects, or an error dict.

* * * * *

### `create_users_bulk(self, users)`

Creates multiple users in a single bulk request. Each entry must already carry a resolved `roleId` and `groups` (list of group IDs) — no name-to-ID resolution is performed.

**Parameters:**

-   `users` (list): User definitions to create. Each dictionary should use canonical Sisense user fields, at minimum `email`, `firstName`, and `roleId`.

**Returns:**

-   `list` | `dict`: The list of created user objects on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_group(self, name)`

> **Deprecated** — use [`get_groups`](#get_groupsself-namenone) with the `name=`
> filter instead. This alias is marked deprecated (PEP 702) and its behavior
> is frozen until removal.

**Parameters:**

-   `name` (str): Group name.

**Returns:**

-   `dict`: A dictionary with `GROUP_ID`, `GROUP_NAME`, and `defaultRole`, or an error dict.

* * * * *

### `get_groups(self, name=None)`

Retrieves groups — one named group, or all of them. With `name` the API filters server-side (`?name=`) to that group; without it, every group is returned.

**Parameters:**

-   `name` (str, optional): Group name to filter by. Omit for all groups.

**Returns:**

-   `list` | `dict`: A list of raw group objects in both modes (each with `_id`, `name`, `defaultRole`, and related fields). Without `name`, an empty list means the server genuinely has no groups. The `name` filter is an exact-match lookup — an unknown name returns `{"ok": False, "error": "..."}` naming it (same honesty rule as `get_user`), never an empty list. Returns the standard error dict on failure.

* * * * *

### `create_groups_bulk(self, groups)`

Creates multiple groups in a single bulk request.

**Parameters:**

-   `groups` (list): Group definitions to create. Each dictionary should use canonical Sisense group fields, at minimum `name`.

**Returns:**

-   `list` | `dict`: The list of created group objects on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `delete_group(self, group_id)`

Deletes a group by ID.

**Parameters:**

-   `group_id` (str): The ID of the group to delete.

**Returns:**

-   `dict`: A success message dict, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `create_user(self, user_data)`

Creates a new user by converting group and role names into IDs. Required fields are validated up front — a missing `email` or `role` is rejected with a clear error before any API call.

**Parameters:**

-   `user_data` (`CreateUserPayload`): User fields. `email` and `role` are **required**; `userName`, `firstName`, `lastName`, `groups` (names, resolved to IDs), and `preferences` are optional.

**Returns:**

-   `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `update_user(self, user_email, user_data)`

Updates a user’s attributes by email address (email-based lookup via get_user). All update fields must be provided inside user_data.

**Parameters:**

-   `user_name` (str): Email address of the user to update (used to find the user).

-   `user_data` (`UpdateUserPayload`): Fields to update as a dictionary — all optional, only include fields you want to change (for example: firstName, lastName, email, userName, role, groups).

**Returns:**

-   `dict`: API response on success, or `{"ok": False, "error": "..."}` if the operation fails.

* * * * *

### `delete_user(self, user_name)`

Deletes a user by their email or username.

**Parameters:**

-   `user_name` (str): User identifier.

**Returns:**

-   `dict`: A success message dict, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `users_per_group(self, group_name=None)`

Retrieves group memberships as **flat rows** — one row per (group, user) membership. With `group_name` the rows are that group's members; without it, **every membership** on the instance is returned. `Everyone` memberships are reported like any other — the SDK reports what Sisense says; consumers decide what to hide. There is no synthetic "Admins" bucket.

**Parameters:**

-   `group_name` (str, optional): The name of the group whose members to list. Omit for all memberships. A name that matches no group (e.g. a typo) returns `{"ok": False, "error": "..."}` naming the reference — never a silent empty list.

**Returns:**

-   `list` | `dict`: One row per (group, user) membership, each with `GROUP_ID`, `GROUP_NAME`, `USER_ID`, `USER_NAME`, `EMAIL`, `FIRST_NAME`, `LAST_NAME`, `IS_ACTIVE`, `ROLE_ID`, `ROLE_NAME` and `ROLE_DISPLAY_NAME` (the name the Sisense UI shows), and `ROLE_RAW_NAME` (the raw Sisense value). Membership is read from the group side (`GET /api/v1/groups?expand=users`), the same source the Sisense UI shows, so the auto-generated `Admins` and `All users in system` groups report their real members. `Everyone` and `All users in system` are omitted from the all-groups view (Sisense puts every user in both, so they restate `get_users_all` rather than describing group structure) — naming one directly still returns its members. A group with no members contributes no rows, so the row count always equals the real membership count. Returns `{"ok": False, "error": "..."}` on failure or unknown `group_name`.

* * * * *

### `users_per_group_all(self)`

> **Deprecated** — use [`users_per_group`](#users_per_groupself-group_namenone) with no
> argument instead. This alias is marked deprecated (PEP 702) and its behavior is frozen
> until removal.
>
> It derives its `"Admins"` entry from users' **roles** (`sysAdmin`/`dataAdmin`/`admin`)
> rather than from group membership, and excludes `Everyone` and `All users in system`.
> `users_per_group()` reads real group-side membership for every group, including the
> auto-generated ones, so it matches the counts shown in the Sisense UI.

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

-   `dict`: `{"total_folders_changed": int, "total_dashboards_changed": int}`
    on success, `{"ok": False, "error": "..."}` if the executing user or new owner
    cannot be resolved, or `None` when there are no folders or dashboards
    to change.

* * * * *

### `get_datamodel_columns(self, datamodel_name)`

Extracts all columns from the datasets and tables of a specified DataModel.

**Parameters:**

-   `datamodel_name` (str): Name of the DataModel.

**Returns:**

-   `list`: List of dictionaries with model ID, name, table, and column.

* * * * *

### `get_unused_columns(self, datamodel_name)`

> **Deprecated** — use [`get_unused_columns_bulk`](#get_unused_columns_bulkdatamodels)
> instead. This alias is marked deprecated (PEP 702) and its behavior is frozen
> until removal.

Identifies unused columns in a DataModel by comparing against dashboard usage.

**Parameters:**

-   `datamodel_name` (str): Name of the DataModel.

**Returns:**

-   `list`: Each entry includes table, column, and a 'used' flag.

**Limitations:**

-   Assumes API user has full dashboard access.

* * * * *

### `get_unused_columns_bulk(datamodels)`

Runs unused-column analysis for one or more data models and returns a combined per-model outcome.

A column counts as used when any dashboard on the model references it — in a dashboard filter (plain or dependent levels), a widget panel item, or a formula's `context`. Both `[Table.Column]` and `[Table].[Column]` references are understood, and names may contain any character; when a name itself contains dots, the model's own columns decide where the table name ends.

It accepts data model references (IDs or titles), resolves each one via
`Datamodel.resolve_datamodel_reference`, runs the unused-column analysis for
every successfully resolved model, and returns all rows plus per-reference
failures in a single dict.

**Parameters:**

- `datamodels` (str or list of str, **required**):  
  One or more data model references to analyze. Each reference can be:
  - A data model ID, or  
  - A data model title (name).  

  At runtime this parameter is tolerant of a single string and will normalize
  it to a one-element list.

**Returns:**

- `dict`:  
  Always a dict with `"results"` and `"errors"`:
  - `"results"`: a flat list of column rows across all processed data models.
    A model that resolves and genuinely has no unused columns contributes no rows.
  - `"errors"`: a list of `{"ref": ..., "error": ...}` entries, one per
    reference that could not be resolved or processed — empty when every
    reference succeeded. Partial success puts the good rows in `"results"`
    and the per-reference failures in `"errors"`.

  When **none** of the given references can be processed (or the input is
  invalid), the dict additionally carries `"ok": False` and a top-level
  `"error"` summary naming each reference and why it failed — never a silent
  empty result. (The old `failed_references` key is gone.)

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

- `dict`: API response confirming schedule creation, or `{"ok": False, "error": "..."}` on failure.


* * * * *

### `get_my_user()`

Retrieves the user profile for the currently authenticated API token. Sends `GET /api/users/loggedin`. Useful for resolving migration user identity (email, `_id`) without a separate lookup.

**Returns:**

-   `dict`: The logged-in user object from the API (includes `_id`, `email`, `userName`, `role`, and related fields), or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `get_roles()`

Lists all Sisense roles available on the instance. Sends `GET /api/roles`. Returns the raw role list used to build role name-to-ID maps.

**Returns:**

-   `list[dict]`: List of role objects (each includes at minimum `_id` and `name`), or `{"ok": False, "error": "..."}` on failure.

**Note:** Internal role names (`consumer`, `contributor`, `super`) map to user-facing names (`viewer`, `dashboardDesigner`, `sysAdmin`) per the role name mapping convention.

`create_user` and `update_user` accept **either** vocabulary for `role`, matched ignoring case, spaces and punctuation — `"super"`, `"sysAdmin"`, `"sys admin"` and `"System Administrator"` all resolve to the same role, so a value read from `ROLE_DISPLAY_NAME` can be written straight back. Roles the instance defines itself (`dataDesigner`, `dataAdmin`, `admin`, `tenantAdmin`, `custom_*`) are matched by their own name and always win over an alias: `"admin"` resolves to a real `admin` role rather than to `super`, and `"data designer"` resolves to `dataDesigner`, never to `contributor`. An unmatched name returns a failure dict listing the roles the instance actually has. (In 1.x, `sysAdmin` and `dashboardDesigner` were rejected.)

* * * * *

Tenant Management
------------------

### `get_tenants(self)`

Retrieves the full list of tenants. Only meaningful on multi-tenant deployments.

**Returns:**

-   `list` | `dict`: A list of raw tenant objects, or `{"ok": False, "error": "..."}` on failure (for example, on a single-tenant deployment where the tenants endpoint is unavailable).
