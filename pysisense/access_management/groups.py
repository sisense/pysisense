from __future__ import annotations

from typing import Any


class GroupsMixin:
    def get_group(self, name: str) -> dict[str, Any]:
        """Retrieve group details by name.

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

        if not response or not response.ok:
            status_code = response.status_code if response else "No response"
            self.logger.error(f"Failed to retrieve groups for name '{name}'. Status Code: {status_code}")
            return {"error": f"Failed to retrieve groups for name '{name}'"}

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse group response JSON.")
            return {"error": f"Failed to parse group response JSON: {str(e)}"}

        if not response_data:
            self.logger.warning(f"No group found with name '{name}'")
            return {"error": f"No group found with name '{name}'"}

        group = response_data[0]
        group_id = group.get("_id")
        group_name = group.get("name")

        if not group_id or not group_name:
            self.logger.error(f"Incomplete group data for name '{name}'")
            return {"error": f"Group '{name}' found but missing expected fields"}

        self.logger.debug(f"Group '{name}' found. ID: {group_id}")
        return {"GROUP_ID": group_id, "GROUP_NAME": group_name, "defaultRole": group.get("defaultRole", "")}

    def users_per_group(self, group_name: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all users within a specific group by name.

        Resolves the group by name and then fetches the user objects that
        belong to it.

        Parameters
        ----------
        group_name : str
            The name of the group whose members to list.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of user objects in the group, or ``{"error": "..."}`` if
            the operation fails.
        """
        self.logger.debug(f"Starting 'users_per_group' method for group: {group_name}")

        # Step 1: Fetch group details
        group = self.get_group(group_name)
        if not group or "error" in group:
            error_msg = f"Group '{group_name}' not found. Cannot proceed."
            self.logger.error(error_msg)
            return {"error": error_msg}

        group_id = group.get("GROUP_ID")
        self.logger.debug(f"Group '{group_name}' found with ID: {group_id}. Proceeding to fetch users.")

        # Step 2: Fetch users for the group
        url = f"/api/v1/users?groupId={group_id}"
        response = self.api_client.get(url)

        if not response or not response.ok:
            status = response.status_code if response else "No response"
            error_msg = f"Failed to retrieve users for group '{group_name}'. Status Code: {status}"
            self.logger.error(error_msg)
            return {"error": error_msg}

        try:
            users = response.json()
            self.logger.debug(f"Found {len(users)} users in group '{group_name}'")
            return users
        except Exception as e:
            error_msg = f"Failed to parse user list for group '{group_name}': {e}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def users_per_group_all(self) -> list[dict[str, Any]]:
        """Retrieve all groups mapped to the users belonging to them.

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
        group_response = self.api_client.get("/api/v1/groups")
        if not group_response or not group_response.ok:
            self.logger.error("Failed to retrieve groups from API.")
            return []

        group_data = group_response.json()
        self.logger.debug(f"Retrieved {len(group_data)} groups.")

        # Step 2: Fetch all users
        all_users = self.get_users_all()
        if not all_users:
            self.logger.error("No users returned from 'get_users_all' method.")
            return []

        self.logger.debug(f"Retrieved {len(all_users)} users.")

        # Step 3: Build the initial group dictionary
        groups_dict = {group["name"]: [] for group in group_data if group["name"] not in EXCLUDED_GROUPS}
        if "Admins" not in groups_dict:
            groups_dict["Admins"] = []  # Ensure 'Admins' group exists

        self.logger.debug(f"Initialized groups dictionary with {len(groups_dict)} entries (excluding excluded groups).")

        # Step 4: Populate group membership from users
        for user in all_users:
            for group in user.get("GROUPS", []):
                if group not in EXCLUDED_GROUPS:
                    groups_dict[group].append(user["USER_NAME"])
                    self.logger.debug(f"Added user '{user['USER_NAME']}' to group '{group}'")

        # Step 5: Add users with admin-like roles to 'Admins'
        for user in all_users:
            if user.get("ROLE_NAME") in ["sysAdmin", "dataAdmin", "admin"]:
                groups_dict["Admins"].append(user["USER_NAME"])
                self.logger.debug(f"Added user '{user['USER_NAME']}' to Admins group based on role.")

        # Step 6: Prepare final result
        result = [{"group": group_name, "username": usernames} for group_name, usernames in groups_dict.items()]

        if result:
            self.logger.info(f"Resolved {len(result)} group entries.")
        else:
            self.logger.error("No groups or users found.")

        return result
