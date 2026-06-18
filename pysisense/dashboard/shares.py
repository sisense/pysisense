from __future__ import annotations

from typing import Any


class SharesMixin:
    def add_dashboard_shares(self, dashboard_id, shares):
        """
        Adds or updates shares for a dashboard, specifying users and groups along with their access rules.

        Parameters:
            dashboard_id (str): The ID of the dashboard to which the shares will be added.
            shares (list of dicts): A list of dictionaries, each containing:
                - "name" (str): The username or group name.
                - "type" (str): Either "user" or "group" to indicate the share type.
                - "rule" (str): The access level (e.g., "view", "edit").

        Returns:
            str: Success message or error details.
        """

        endpoint = f"/api/shares/dashboard/{dashboard_id}?adminAccess=true"

        self.logger.info(f"Starting to add/update shares for dashboard {dashboard_id}.")
        self.logger.debug(f"Received shares payload: {shares}")

        # Get users and groups from shares
        users = [share for share in shares if share["type"] == "user"]
        groups = [share for share in shares if share["type"] == "group"]

        # Resolve user IDs
        for user in users:
            user_info = self.access_mgmt.get_user(user["name"])
            if user_info is None:
                self.logger.error(f"User '{user['name']}' not found. Skipping.")
                continue  # Skip this user
            user["shareId"] = user_info["USER_ID"]

        # Resolve group IDs
        for group in groups:
            group_info = self.access_mgmt.get_group(group["name"])
            if group_info is None:
                self.logger.error(f"Group '{group['name']}' not found. Skipping.")
                continue  # Skip this group
            group["shareId"] = group_info["GROUP_ID"]

        # Remove 'name' key after resolving IDs
        for user in users:
            user.pop("name", None)
        for group in groups:
            group.pop("name", None)

        # Fetch existing shares
        shares_response = self.api_client.get(endpoint)
        if shares_response is None or shares_response.status_code != 200:
            self.logger.warning(f"Failed to retrieve existing shares for dashboard {dashboard_id} with admin access. Trying without admin access.")
            # Try without admin access
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}")
            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(f"Failed to retrieve existing shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to retrieve existing shares for dashboard {dashboard_id}."

        existing_shares = shares_response.json().get("sharesTo", [])
        # Ignore shares without a "rule" key to prevent KeyError since the dashboard owner does not have a rule
        existing_share_map = {share["shareId"]: share["rule"] for share in existing_shares if "rule" in share}

        self.logger.info(f"Existing shares for dashboard {dashboard_id}: {len(existing_shares)} found.")
        self.logger.debug(f"Existing shares details: {existing_shares}")

        # Determine new shares & updates
        new_users = []
        new_groups = []
        updated_users = []
        updated_groups = []

        for user in users:
            if user["shareId"] in existing_share_map:
                if user["rule"] != existing_share_map[user["shareId"]]:  # Rule change detected
                    self.logger.info(f"Updating rule for existing user {user['shareId']} from '{existing_share_map[user['shareId']]}' to '{user['rule']}'.")
                    updated_users.append(user)
            else:
                new_users.append(user)

        for group in groups:
            if group["shareId"] in existing_share_map:
                if group["rule"] != existing_share_map[group["shareId"]]:  # Rule change detected
                    self.logger.info(f"Updating rule for existing group {group['shareId']} from '{existing_share_map[group['shareId']]}' to '{group['rule']}'.")
                    updated_groups.append(group)
            else:
                new_groups.append(group)

        if not new_users and not new_groups and not updated_users and not updated_groups:
            reason = "All provided users/groups already have access with the same rule."
            self.logger.info(f"No new or updated shares for dashboard {dashboard_id}. Reason: {reason}")
            return f"No new or updated shares added. Reason: {reason}"

        # Remove updated users/groups from existing_shares to prevent duplication
        existing_shares = [share for share in existing_shares if share["shareId"] not in {user["shareId"] for user in updated_users}]
        existing_shares = [share for share in existing_shares if share["shareId"] not in {group["shareId"] for group in updated_groups}]
        # Prepare final payload (keeping existing shares + new shares + updated shares)
        payload = {"sharesTo": existing_shares + new_users + new_groups + updated_users + updated_groups}
        self.logger.debug(f"Final payload for adding/updating shares: {payload}")

        # Make the POST request to update shares
        try:
            response = self.api_client.post(endpoint, data=payload)

            # If response is None or failed, try fallback endpoint
            if response is None or response.status_code != 200:
                self.logger.warning(f"POST to '{endpoint}' failed for dashboard '{dashboard_id}'. Trying fallback without admin access.")
                fallback_endpoint = f"/api/shares/dashboard/{dashboard_id}"
                response = self.api_client.post(fallback_endpoint, data=payload)

                # If fallback also fails, return error
                if response is None or response.status_code != 200:
                    error_message = response.json() if response and response.content else "No response received."
                    self.logger.error(f"Failed to add/update shares for dashboard '{dashboard_id}' via fallback. Error: {error_message}")
                    return f"Error: Failed to add/update shares for dashboard '{dashboard_id}'."

            if response.status_code == 200:
                success_message = (
                    f"Shares successfully added/updated for dashboard {dashboard_id}. "
                    f"New users: {[user['shareId'] for user in new_users]}, "
                    f"New groups: {[group['shareId'] for group in new_groups]}, "
                    f"Updated users: {[user['shareId'] for user in updated_users]}, "
                    f"Updated groups: {[group['shareId'] for group in updated_groups]}"
                )
                self.logger.info(success_message)
                return success_message
            else:
                error_message = response.json() if response else response.text
                self.logger.error(f"Failed to add/update shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: {error_message}"

        except Exception as e:
            self.logger.exception(f"Exception while adding/updating shares for dashboard {dashboard_id}: {e}")
            return f"Exception: {str(e)}"

    def get_dashboard_share(self, dashboard_name):
        """
        Retrieves share details (users and groups) for a specific dashboard by title.

        Parameters:
            dashboard_name (str): The title of the dashboard to retrieve share information for.

        Returns:
            list: A list of dictionaries containing share type (user or group), and share name (email or group name),
                or an empty list if the dashboard is not found or has no shares.
        """
        self.logger.info(f"Fetching share details for dashboard: '{dashboard_name}'")

        # Step 1: Retrieve dashboard(s) by name
        dashboards = self.get_dashboard_by_name(dashboard_name)

        # Handle case where response is a list
        dashboard = next((d for d in dashboards if d.get("title", "").lower() == dashboard_name.lower()), None) if isinstance(dashboards, list) else dashboards

        if not dashboard:
            self.logger.warning(f"Dashboard '{dashboard_name}' not found.")
            return []

        shares = dashboard.get("shares", [])
        if not shares:
            self.logger.info(f"Dashboard '{dashboard_name}' has no shares.")
            return []

        # Step 2: Fetch all users
        users_response = self.api_client.get("/api/v1/users")
        if not users_response or users_response.status_code != 200:
            self.logger.error("Failed to fetch users.")
            return []

        users_data = users_response.json()
        users_detail = {user["_id"]: user.get("email", "Unknown Email") for user in users_data}

        # Step 3: Fetch all groups
        groups_response = self.api_client.get("/api/v1/groups")
        if not groups_response or groups_response.status_code != 200:
            self.logger.error("Failed to fetch groups.")
            return []

        groups_data = groups_response.json()
        groups_detail = {group["_id"]: group.get("name", "Unknown Group") for group in groups_data}

        # Step 4: Resolve shares
        shared_list = []
        for share in shares:
            share_type = share.get("type")
            share_id = share.get("shareId")

            if share_type == "user" and share_id in users_detail:
                shared_list.append({"type": "user", "name": users_detail[share_id]})
            elif share_type == "group" and share_id in groups_detail:
                shared_list.append({"type": "group", "name": groups_detail[share_id]})

        self.logger.info(f"Found {len(shared_list)} shares for dashboard '{dashboard_name}'.")
        return shared_list

    def get_dashboard_shares_v1(
        self,
        dashboard_id: str,
        *,
        admin_access: bool = True,
    ) -> dict[str, Any]:
        """Retrieve share details for a dashboard using the v1 shares endpoint.

        Sends ``GET /api/v1/dashboards/{dashboard_id}/shares``. This returns the
        raw Sisense shares payload (for example ``sharesTo`` and ``owner``),
        unlike ``get_dashboard_share`` which resolves names from the admin list.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid``.
        admin_access : bool, optional
            When ``True`` (default), request with ``adminAccess=true``.

        Returns
        -------
        dict[str, Any]
            The shares response from the API, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/dashboards/{dashboard_id}/shares"
        if admin_access:
            endpoint += "?adminAccess=true"

        self.logger.debug(f"Fetching v1 shares for dashboard {dashboard_id}")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request to retrieve v1 shares for dashboard {dashboard_id} failed: No response received.")
            return {"error": f"No response received while retrieving shares for dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve v1 shares for dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed to retrieve shares for dashboard '{dashboard_id}'. {error_message}"}

        shares_data = response.json()
        self.logger.info(f"Successfully retrieved v1 shares for dashboard {dashboard_id}.")
        return shares_data
