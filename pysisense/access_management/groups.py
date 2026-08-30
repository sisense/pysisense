from __future__ import annotations

from typing import Any

from typing_extensions import deprecated

from ..utils import _extract_error_message


class GroupsMixin:
    @deprecated("use get_groups")
    def get_group(self, name: str) -> dict[str, Any]:
        """Retrieve group details by name.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`get_groups` with its optional ``name`` filter, which
        returns the raw group objects.

        Looks up a group by its name and returns its ID, name, and default
        role.

        Parameters
        ----------
        name : str
            The name of the group to be retrieved.

        Returns
        -------
        dict[str, Any]
            A dictionary with ``GROUP_ID``, ``GROUP_NAME``, and ``defaultRole``,
            or ``{"error": "..."}`` if retrieval fails or the group is not found.
        """
        self.logger.debug(f"Starting 'get_group' method for group name: {name}")

        # Make the API call to fetch groups by name
        response = self.api_client.get(f"/api/v1/groups?name={name}")

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to retrieve groups for name '{name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse group response JSON.")
            return {"ok": False, "error": f"Failed to parse group response JSON: {str(e)}"}

        if not response_data:
            self.logger.warning(f"No group found with name '{name}'")
            return {"ok": False, "error": f"No group found with name '{name}'"}

        group = response_data[0]
        group_id = group.get("_id")
        group_name = group.get("name")

        if not group_id or not group_name:
            self.logger.error(f"Incomplete group data for name '{name}'")
            return {"ok": False, "error": f"Group '{name}' found but missing expected fields"}

        self.logger.debug(f"Group '{name}' found. ID: {group_id}")
        return {"GROUP_ID": group_id, "GROUP_NAME": group_name, "defaultRole": group.get("defaultRole", "")}

    def get_groups(self, name: str | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve groups — one named group, or all of them.

        Fetches the groups defined on the Sisense server. With ``name`` the
        API filters server-side to that group; without it, every group is
        returned. One row per group.

        Parameters
        ----------
        name : str or None, optional
            Group name to filter by. Omit for all groups.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of raw group objects as returned by the API (each with
            ``_id``, ``name``, ``defaultRole``, and related fields) — empty
            when a ``name`` filter matches nothing. Returns ``{"error": "..."}``
            on failure.
        """
        self.logger.debug(f"Starting 'get_groups' method (name={name!r}).")

        endpoint = f"/api/v1/groups?name={name}" if name is not None else "/api/v1/groups"
        response = self.api_client.get(endpoint)

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve groups", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            groups = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse groups response JSON.")
            return {"ok": False, "error": f"Failed to parse groups response JSON: {str(e)}"}

        self.logger.debug(f"Retrieved {len(groups or [])} group(s).")
        return groups or []

    def create_groups_bulk(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]] | dict[str, Any]:
        """Create multiple groups in a single bulk request.

        Sends the provided group definitions to the bulk group creation
        endpoint.

        Parameters
        ----------
        groups : list[dict[str, Any]]
            Group definitions to create. Each dictionary should use
            canonical Sisense group fields, at minimum ``name``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The list of created group objects on success, or
            ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Starting 'create_groups_bulk' method for {len(groups)} group(s).")

        response = self.api_client.post("/api/v1/groups/bulk", data=groups)

        if response is None:
            self.logger.error("No response received while creating groups in bulk.")
            return {"ok": False, "error": "No response received while creating groups in bulk."}

        if response.status_code != 201:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "Unknown error"
            self.logger.error(f"Failed to create groups in bulk. Error: {error_message}")
            return {"ok": False, "error": f"Failed to create groups in bulk. {error_message}"}

        try:
            created_groups = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse bulk group creation response JSON.")
            return {"ok": False, "error": f"Failed to parse bulk group creation response JSON: {str(e)}"}

        self.logger.info(f"Successfully created {len(created_groups or [])} group(s).")
        return created_groups or []

    def delete_group(self, group_id: str) -> dict[str, Any]:
        """Delete a group by ID.

        Sends a DELETE request to remove the group from the Sisense server.

        Parameters
        ----------
        group_id : str
            The ID of the group to delete.

        Returns
        -------
        dict[str, Any]
            A success message dict if successful, or ``{"error": "..."}``
            on failure.
        """
        self.logger.debug(f"Starting 'delete_group' method for group ID: {group_id}")

        response = self.api_client.delete(f"/api/v1/groups/{group_id}")

        if response and response.status_code == 204:
            self.logger.info(f"Group (ID: {group_id}) deleted. No content returned.")
            return {"message": "Group deleted successfully."}

        if response and response.ok:
            try:
                response_data = response.json()
            except Exception:
                response_data = {"message": "Group deleted, but no JSON body returned."}
            self.logger.info(f"Group (ID: {group_id}) deleted.")
            return response_data

        failure = _extract_error_message(response, f"Failed to delete group (ID: {group_id})", self.api_client)
        self.logger.error(failure["error"])
        return failure

    def users_per_group(self, group_name: str | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve group memberships — one group's members, or every membership.

        Returns one flat row per (group, user) membership. With ``group_name``
        the rows are that group's members; without it, every membership on the
        instance is returned (ask ``get_groups`` for the per-group view).
        ``Everyone`` memberships are reported like any other — the SDK reports
        what Sisense says; consumers decide what to hide.

        Parameters
        ----------
        group_name : str or None, optional
            The name of the group whose members to list. Omit for all
            memberships. A name that matches no group returns
            ``{"error": "..."}`` naming it — never a silent empty list.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            One row per (group, user) membership, each with ``GROUP_ID``,
            ``GROUP_NAME``, ``USER_ID``, ``USER_NAME``, ``EMAIL``,
            ``FIRST_NAME``, ``LAST_NAME``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME`` (raw Sisense value), and ``ROLE_DISPLAY_NAME`` (the
            name the Sisense UI shows). A group with no members contributes no
            rows, so the row count always equals the real membership count.
            Returns ``{"error": "..."}`` on failure or unknown ``group_name``.
        """
        self.logger.debug(f"Starting 'users_per_group' method (group_name={group_name!r}).")

        # A filtered request for a group that doesn't exist must fail loudly —
        # an empty list would read as "the group has no members".
        if group_name is not None:
            groups = self.get_groups(name=group_name)
            if isinstance(groups, dict):
                return groups
            if not groups:
                error_msg = f"Group '{group_name}' not found."
                self.logger.error(error_msg)
                return {"ok": False, "error": error_msg}

        users = self._get_users_raw()
        if isinstance(users, dict):
            return users

        memberships: list[dict[str, Any]] = []
        for user in users:
            row = self._user_row(user)
            for gid, gname in zip(row["GROUP_IDS"], row["GROUP_NAMES"], strict=False):
                if group_name is not None and gname != group_name:
                    continue
                memberships.append(
                    {
                        "GROUP_ID": gid,
                        "GROUP_NAME": gname,
                        "USER_ID": row["USER_ID"],
                        "USER_NAME": row["USER_NAME"],
                        "EMAIL": row["EMAIL"],
                        "FIRST_NAME": row["FIRST_NAME"],
                        "LAST_NAME": row["LAST_NAME"],
                        "IS_ACTIVE": row["IS_ACTIVE"],
                        "ROLE_ID": row["ROLE_ID"],
                        "ROLE_NAME": row["ROLE_NAME"],
                        "ROLE_DISPLAY_NAME": row["ROLE_DISPLAY_NAME"],
                    }
                )

        self.logger.info(f"Resolved {len(memberships)} membership row(s) (group_name={group_name!r}).")
        return memberships

    @deprecated("use users_per_group")
    def users_per_group_all(self) -> list[dict[str, Any]]:
        """Retrieve all groups mapped to the users belonging to them.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`users_per_group` with no argument, which returns flat
        one-row-per-membership results without the exclusions and synthetic
        groups below.

        Groups like ``Everyone`` and ``All users in system`` are excluded.
        Users with roles like ``admin``, ``dataAdmin``, and ``sysAdmin`` are
        mapped to the existing ``Admins`` group. Groups with no users are also
        included in the final result.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries, where each dictionary contains a group name
            and the list of usernames in that group. An empty list is returned
            on failure.
        """
        EXCLUDED_GROUPS = {"Everyone", "All users in system"}

        self.logger.debug("Starting to retrieve all groups and their users.")

        # Step 1: Fetch all groups
        group_data = self.get_groups()
        if isinstance(group_data, dict) and "error" in group_data:
            self.logger.error("Failed to retrieve groups from API.")
            return []

        self.logger.debug(f"Retrieved {len(group_data)} groups.")

        # Step 2: Fetch all users (canonical rows)
        all_users = self.get_users_all()
        if isinstance(all_users, dict) or not all_users:
            self.logger.error("No users returned from 'get_users_all' method.")
            return []

        self.logger.debug(f"Retrieved {len(all_users)} users.")

        # Step 3: Build the initial group dictionary
        groups_dict = {group["name"]: [] for group in group_data if group["name"] not in EXCLUDED_GROUPS}
        if "Admins" not in groups_dict:
            groups_dict["Admins"] = []  # Ensure 'Admins' group exists

        self.logger.debug(f"Initialized groups dictionary with {len(groups_dict)} entries (excluding excluded groups).")

        # Step 4: Populate group membership from users (frozen output: the
        # canonical rows now include Everyone, so the old exclusion filter is
        # applied here to keep this deprecated alias's behavior unchanged).
        for user in all_users:
            for group in user.get("GROUP_NAMES", []):
                if group in EXCLUDED_GROUPS:
                    continue
                if group not in groups_dict:
                    self.logger.debug(f"Skipping user '{user.get('USER_NAME')}' group '{group}' not in current group list")
                    continue
                groups_dict[group].append(user["USER_NAME"])
                self.logger.debug(f"Added user '{user['USER_NAME']}' to group '{group}'")

        # Step 5: Add users with admin-like roles to 'Admins' (display names —
        # the vocabulary this alias always matched against).
        for user in all_users:
            if user.get("ROLE_DISPLAY_NAME") in ["sysAdmin", "dataAdmin", "admin"]:
                groups_dict["Admins"].append(user["USER_NAME"])
                self.logger.debug(f"Added user '{user['USER_NAME']}' to Admins group based on role.")

        # Step 6: Prepare final result
        result = [{"group": group_name, "username": usernames} for group_name, usernames in groups_dict.items()]

        if result:
            self.logger.info(f"Resolved {len(result)} group entries.")
        else:
            self.logger.error("No groups or users found.")

        return result
