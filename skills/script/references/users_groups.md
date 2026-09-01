# User & group management reference

`from pysisense import AccessManagement`, `access_mgmt = AccessManagement(api_client=api_client)`

**Failure contract:** every method below returns the standard `{"ok": False, "error": "...", "status_code": <int, when available>}` dict on failure. An empty list from a read method means a genuinely empty result, not a failure.

## Canonical user row shape

`get_user` and `get_users_all` both return the same canonical row: `USER_ID`, `USER_NAME`, `EMAIL`, `FIRST_NAME`, `LAST_NAME`, `IS_ACTIVE`, `ROLE_ID`, `ROLE_NAME` (raw Sisense value, e.g. `"consumer"`), `ROLE_DISPLAY_NAME` (the name the Sisense UI shows, e.g. `"viewer"`), `GROUP_IDS`, and `GROUPS` (unfiltered, includes `Everyone`).

`ROLE_NAME` deliberately keeps its 1.x meaning (the raw value) so role comparisons written against 1.x keep working; prefer `ROLE_DISPLAY_NAME` in new code since it says explicitly which vocabulary it holds.

| `ROLE_NAME` (raw Sisense) | `ROLE_DISPLAY_NAME` (UI) |
|---|---|
| `consumer` | `viewer` |
| `super` | `sysAdmin` |
| `contributor` | `dashboardDesigner` |

## Users: read

| Method | Returns |
|---|---|
| `get_user(email)` | Single user, canonical row, keyed by email. `USER_ID` is for use in ownership/share calls. |
| `get_my_user()` | The API token's own identity. |
| `get_users_all()` | Every user, canonical rows. |
| `get_roles()` | All roles, build a `{name: _id}` map for resolving role assignments manually. |

`get_user_with_role_and_group_names(email)` and `get_users_with_role_names_and_group_names()` are deprecated aliases for `get_user`/`get_users_all` (same canonical row now covers both use cases); `get_users_expanded()` is a deprecated alias for `get_users_all()` too. Prefer the canonical names in new code, the old ones still work but will be removed in a future major version.

## Users: write

`create_user(user_data)` resolves `role` (name, case-insensitive; `"viewer"`→`consumer`, `"designer"`/`"dashboardDesigner"`→`contributor`) and `groups` (names) to IDs before POSTing.

```python
user_data = {
    "email": "john.doe@example.com",  # required
    "firstName": "John",
    "lastName": "Doe",
    "role": "dashboardDesigner",  # omit entirely for default "viewer"; never pass ""
    "groups": ["mig_test"],  # omit or [] if no groups
    "password": "...",  # omit to send a set-password email instead
    "preferences": {"language": "en-US"},
}
access_mgmt.create_user(user_data)
```

`update_user(user_email, user_data)`: partial update (PATCH), located by email. **Only include fields you intend to change**, `role`/`groups` names are resolved to IDs the same way as `create_user`. Never pass `"groups": []` unless the intent is genuinely to clear all group membership.

`delete_user(user_name)`: by email/username.

`change_user_password(user_id, password)`: takes the internal `_id` (not email), PATCHes only `password`.

`create_users_bulk(users)`: **no name resolution**, every entry must already carry a resolved `roleId` and `groups` (list of group IDs). Look up IDs with `get_roles()` / `get_groups(name=...)` first if building from names.

```python
users_to_create = [{"email": "alice@example.com", "firstName": "Alice", "roleId": role_id, "groups": [group_id]}]
access_mgmt.create_users_bulk(users_to_create)
```

## Groups

| Method | Notes |
|---|---|
| `get_groups(name=None)` | With `name`, an exact-match lookup, still returns a **list** (index `[0]` for the common single-group case); an unknown name returns the standard error dict, not `[]`. Without `name`, every group, and an empty list there means the server genuinely has none. |
| `create_groups_bulk(groups)` | `[{"name": "Sales Team"}, ...]`. |
| `delete_group(group_id)` | By ID, fetch via `get_groups(name=...)` first. |
| `users_per_group(group_name=None)` | With `group_name`, that group's members; without it, every membership on the instance. `Everyone` and `All users in system` are omitted from the all-groups view (Sisense fills both with every user, so they'd just restate `get_users_all`), but naming either one directly still returns its members. |

`get_group(name)` and `users_per_group_all()` are deprecated aliases for `get_groups(name=...)`/`users_per_group()`. Prefer the new names.

```python
groups = access_mgmt.get_groups(name="Sales Team")
if isinstance(groups, list):
    access_mgmt.delete_group(groups[0]["GROUP_ID"])
```

**Provisioning order**: create groups before users that reference them by name, see SKILL.md Worked Example 3.

## Folder + dashboard ownership transfer

`change_folder_and_dashboard_ownership(executing_user, folder_name, new_owner_name, original_owner_rule="edit", change_dashboard_ownership=True)`: reassigns an **entire folder tree** (subfolders, sibling/parent folders in that structure) and optionally every dashboard inside it. This is a different, broader operation than `Dashboard.change_dashboard_owner` (which moves one dashboard). Use this one when the ask is "move everything in folder X to Y," and the narrower dashboard method when the ask is "move these specific dashboards."

```python
response = access_mgmt.change_folder_and_dashboard_ownership(
    executing_user="admin@sisense.com",
    folder_name="Sales Reports",
    new_owner_name="bob@example.com",
    original_owner_rule="edit",
    change_dashboard_ownership=True,
)
if response and response.get("ok") is not False:
    print(f"Folders changed: {response.get('total_folders_changed', 0)}, dashboards changed: {response.get('total_dashboards_changed', 0)}")
```

`executing_user` must be an admin, this call needs elevated API access.

## Column-level security (governance)

```python
access_mgmt.get_datamodel_columns(datamodel_name)
result = access_mgmt.get_unused_columns_bulk(datamodels=["Sample ECommerce", "id_or_name_2"])
# {"results": [...], "errors": [...]}
```

`get_unused_columns_bulk` always returns `{"results": [...], "errors": [...]}`. `results` is a flat list of column rows across every model that resolved; `errors` lists `{"ref", "error"}` entries for references that couldn't be processed, empty when everything succeeded. Only when *none* of the given references can be processed does the dict additionally carry a top-level `"ok": False` and `"error"`, so standard failure detection (`payload.get("ok") is False`) still fires for that all-fail case.

`get_unused_columns(datamodel_name=...)` is a deprecated alias for `get_unused_columns_bulk(datamodels=...)`; prefer the new name.

## Dashboard shares (admin-wide report)

`access_mgmt.get_all_dashboard_shares()`: sharing info for every dashboard on the instance, for governance/audit scripts (as opposed to `Dashboard.get_dashboard_share`, which is per-dashboard).

## Scheduled builds

```python
access_mgmt.create_schedule_build(days=["MON", "TUE"], hour=21, minute=0, datamodel_name="my_ec")
access_mgmt.create_schedule_build(datamodel_name="my_ec", interval_hours=1)
```

## Tenants (multi-tenant only)

`access_mgmt.get_tenants()`: meaningless on single-tenant deployments, returns the tenant list otherwise.
