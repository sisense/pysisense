from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class SharesMixin:
    def change_dashboard_owner(
        self,
        dashboard_id: str,
        new_owner_id: str,
        *,
        admin_access: bool = True,
        original_owner_rule: str = "edit",
    ) -> dict[str, Any]:
        """Transfer ownership of a dashboard to a different user.

        Sends ``POST /api/v1/dashboards/{dashboard_id}/change_owner``.
        The previous owner is demoted to a share entry with the rule
        specified by ``original_owner_rule``.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard whose owner will be changed.
        new_owner_id : str
            The Sisense user ID (``_id``) of the user who will become the new owner.
        admin_access : bool, optional
            When ``True`` (default), appends ``?adminAccess=true`` to the request.
            Required when the API token user is not the current dashboard owner.
            Pass ``False`` when restoring ownership back to the original owner
            (the caller is already the temporary owner at that point).
        original_owner_rule : str, optional
            The share rule assigned to the outgoing owner after the transfer.
            Defaults to ``"edit"``.

        Returns
        -------
        dict[str, Any]
            The API response body on success, or ``{"success": True}`` when the
            API responds 200 with an empty body. ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/dashboards/{dashboard_id}/change_owner"
        if admin_access:
            endpoint += "?adminAccess=true"

        payload = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
        self.logger.debug(f"Changing owner of dashboard {dashboard_id} to {new_owner_id} (admin_access={admin_access})")

        response = self.api_client.post(endpoint, data=payload)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to change owner of dashboard '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Dashboard {dashboard_id} owner changed to {new_owner_id}.")
        return response.json() if response.content else {"success": True}

    def add_dashboard_shares(self, dashboard_id: str, shares: list[dict[str, Any]]) -> str:
        """Add or update shares for a dashboard for the given users and groups.

        Resolves each share's ``name`` to its user or group ``shareId``, compares
        against the dashboard's existing shares, and posts only new shares and
        shares whose ``rule`` changed. Existing shares that are unaffected are
        preserved in the payload.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard to which the shares will be applied.
        shares : list[dict[str, Any]]
            A list of share entries. Each entry must contain:

            - ``name`` (str): The username (email) or group name.
            - ``type`` (str): Either ``"user"`` or ``"group"``.
            - ``rule`` (str): The access level (for example ``"view"`` or
              ``"edit"``).

            The ``name`` is resolved to ``shareId`` internally; entries that
            cannot be resolved are skipped.

        Returns
        -------
        str
            A success message summarizing the new and updated shares, a message
            indicating no changes were needed, or an error description on
            failure.
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
                failure = _extract_error_message(response, f"Failed to add/update shares for dashboard {dashboard_id}", self.api_client)
                self.logger.error(failure["error"])
                return f"Error: {failure['error']}"

        except Exception as e:
            self.logger.exception(f"Exception while adding/updating shares for dashboard {dashboard_id}: {e}")
            return f"Exception: {str(e)}"

    def get_dashboard_share(self, dashboard_name: str) -> list[dict[str, Any]]:
        """Retrieve share details (users and groups) for a dashboard by title.

        Resolves the dashboard by title, then maps each share's ``shareId`` to a
        readable name using the users and groups lists: user shares resolve to
        the user's ``email`` and group shares to the group ``name``.

        Parameters
        ----------
        dashboard_name : str
            Title of the dashboard to retrieve share information for.

        Returns
        -------
        list[dict[str, Any]]
            A list of share entries, each containing ``type`` (``"user"`` or
            ``"group"``) and ``name`` (email or group name). Returns an empty
            list when the dashboard is not found, has no shares, or the users or
            groups lookup fails.
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

        # Step 2: Fetch user/group ID-to-name lookup maps
        maps = self.access_mgmt.get_user_email_and_group_name_maps()
        if "error" in maps:
            self.logger.error(f"Failed to fetch users or groups: {maps['error']}")
            return []

        users_detail = maps["users_by_id"]
        groups_detail = maps["groups_by_id"]

        # Step 3: Resolve shares
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
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Retrieve share details for a dashboard using the v1 shares endpoint.

        Sends ``GET /api/v1/dashboards/{dashboard_id}/shares``. This returns the
        raw Sisense shares payload (for example ``sharesTo`` and ``owner``),
        unlike ``get_dashboard_share`` which resolves names from the admin list.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid``.
        admin_access : bool, optional
            When ``True`` (default), request with ``adminAccess=true``. Some
            Sisense versions reject the ``adminAccess`` query parameter with
            HTTP 422 (strict query-schema validation); the request is then
            retried automatically without it.

        Returns
        -------
        dict[str, Any] | list[dict[str, Any]]
            The shares response from the API, or ``{"error": "..."}`` on
            failure. The payload shape varies by Sisense version (a dict with
            ``sharesTo``/``owner``, or a list of share entries).
        """
        base_endpoint = f"/api/v1/dashboards/{dashboard_id}/shares"
        endpoint = f"{base_endpoint}?adminAccess=true" if admin_access else base_endpoint

        self.logger.debug(f"Fetching v1 shares for dashboard {dashboard_id}")
        response = self.api_client.get(endpoint)

        # Some Sisense versions validate the query schema strictly and reject
        # adminAccess as an unknown property (422) — retry without it.
        if admin_access and response is not None and response.status_code == 422:
            self.logger.debug(f"adminAccess rejected by this Sisense version (HTTP 422) for dashboard {dashboard_id}; retrying without it.")
            response = self.api_client.get(base_endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to retrieve shares for dashboard '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        shares_data = response.json()
        self.logger.info(f"Successfully retrieved v1 shares for dashboard {dashboard_id}.")
        return shares_data
