# User & group management reference

`from pysisense import AccessManagement` — `access_mgmt = AccessManagement(api_client=api_client)`

## Users — read

| Method | Returns |
|---|---|
| `get_user(email)` | Single user, expanded groups/role, keyed by email. Has `USER_ID` for use in ownership/share calls. |
| `get_my_user()` | The API token's own identity. |
| `get_users_all()` | All users, display-name aliases for role/group. |
| `get_users_expanded()` | All users, raw role/group objects (not display aliases) — use when you need the underlying role/group names verbatim. |
| `get_user_with_role_and_group_names(email)` | Single user with both IDs and names for role/groups — good for audit exports. |
| `get_users_with_role_names_and_group_names()` | Same, for all users. |
| `get_roles()` | All roles — build a `{name: _id}` map for resolving role assignments manually. |

## Users — write

`create_user(user_data)` — resolves `role` (name, case-insensitive; `"viewer"`→`consumer`, `"designer"`/`"dashboardDesigner"`→`contributor`) and `groups` (names) to IDs before POSTing.

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

`update_user(user_email, user_data)` — partial update (PATCH), located by email. **Only include fields you intend to change** — `role`/`groups` names are resolved to IDs the same way as `create_user`. Never pass `"groups": []` unless the intent is genuinely to clear all group membership.

`delete_user(user_name)` — by email/username.

`change_user_password(user_id, password)` — takes the internal `_id` (not email), PATCHes only `password`.

`create_users_bulk(users)` — **no name resolution**: every entry must already carry a resolved `roleId` and `groups` (list of group IDs). Look up IDs with `get_roles()` / `get_group()` first if building from names.

```python
users_to_create = [{"email": "alice@example.com", "firstName": "Alice", "roleId": role_id, "groups": [group_id]}]
access_mgmt.create_users_bulk(users_to_create)
```

## Groups

| Method | Notes |
|---|---|
| `get_group(name)` | Single group by name — has `GROUP_ID`. |
| `get_groups()` | All groups. |
| `create_groups_bulk(groups)` | `[{"name": "Sales Team"}, ...]`. |
| `delete_group(group_id)` | By ID — fetch via `get_group(name)` first. |
| `users_per_group(group_name)` | Members of one group. |
| `users_per_group_all()` | Members of every group (excludes `Everyone` / `All users in system`). |

```python
group = access_mgmt.get_group("Sales Team")
if "error" not in group:
    access_mgmt.delete_group(group["GROUP_ID"])
```

**Provisioning order**: create groups before users that reference them by name — see SKILL.md Worked Example 3.

## Folder + dashboard ownership transfer

`change_folder_and_dashboard_ownership(executing_user, folder_name, new_owner_name, original_owner_rule="edit", change_dashboard_ownership=True)` — reassigns an **entire folder tree** (subfolders, sibling/parent folders in that structure) and optionally every dashboard inside it. This is a different, broader operation than `Dashboard.change_dashboard_owner` (which moves one dashboard). Use this one when the ask is "move everything in folder X to Y," and the narrower dashboard method when the ask is "move these specific dashboards."

```python
response = access_mgmt.change_folder_and_dashboard_ownership(
    executing_user="admin@sisense.com",
    folder_name="Sales Reports",
    new_owner_name="bob@example.com",
    original_owner_rule="edit",
    change_dashboard_ownership=True,
)
if response and "error" not in response:
    print(f"Folders changed: {response.get('total_folders_changed', 0)}, dashboards changed: {response.get('total_dashboards_changed', 0)}")
```

`executing_user` must be an admin — this call needs elevated API access.

## Column-level security (governance)

```python
access_mgmt.get_datamodel_columns(datamodel_name)
access_mgmt.get_unused_columns(datamodel_name="Sample ECommerce")
access_mgmt.get_unused_columns_bulk(datamodels=["id_or_name_1", "id_or_name_2"])
```

## Dashboard shares (admin-wide report)

`access_mgmt.get_all_dashboard_shares()` — sharing info for every dashboard on the instance, for governance/audit scripts (as opposed to `Dashboard.get_dashboard_share`, which is per-dashboard).

## Scheduled builds

```python
access_mgmt.create_schedule_build(days=["MON", "TUE"], hour=21, minute=0, datamodel_name="my_ec")
access_mgmt.create_schedule_build(datamodel_name="my_ec", interval_hours=1)
```

## Tenants (multi-tenant only)

`access_mgmt.get_tenants()` — meaningless on single-tenant deployments, returns the tenant list otherwise.
