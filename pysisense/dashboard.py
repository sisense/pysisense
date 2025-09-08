from .sisenseclient import SisenseClient
from .access_management import AccessManagement
import json


class Dashboard:

    def __init__(self, api_client=None, debug=False):
        """
        Initializes the Dashboard class, managing API interactions for dashboards.

        If no Sisense client is provided, a new SisenseClient is created.

        Parameters:
            api_client (SisenseClient, optional): An existing SisenseClient instance.
                If None, a new SisenseClient is created.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Use provided Sisense client or create a new one
        self.api_client = api_client if api_client else SisenseClient(debug=debug)

        # Initialize AccessManagement for user and group management
        self.access_mgmt = AccessManagement(self.api_client, debug=debug)

        # Use the logger from the SisenseClient instance
        self.logger = self.api_client.logger
        self.logger.debug("Dashboard class initialized.")

    def get_all_dashboards(self):
        """
        Retrieves all dashboards from the Sisense server.

        Returns:
            list or dict: A list of dashboards if successful,
                        or a dict containing an error message.
        """
        endpoint = "/api/v1/dashboards/admin?dashboardType=owner"
        self.logger.debug(f"Fetching all dashboards from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve dashboards failed: No response received.")
            return {"error": "No response received from the server."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboards. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboards. {error_message}"}

        dashboards = response.json()
        self.logger.info(f"Successfully retrieved {len(dashboards)} dashboards.")
        return dashboards

    def get_dashboard_by_id(self, dashboard_id):
        """
        Retrieves a specific dashboard by its ID.

        Parameters:
            dashboard_id (str): The ID of the dashboard to retrieve.

        Returns:
            dict: A dictionary containing dashboard details if found,
                or a dict with an error message if the request fails.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}"
        self.logger.debug(f"Fetching dashboard with ID {dashboard_id} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request to retrieve dashboard {dashboard_id} failed: No response received.")
            return {"error": f"No response received while retrieving dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboard '{dashboard_id}'. {error_message}"}

        dashboard_data = response.json()
        if not dashboard_data:
            self.logger.warning(f"No dashboard found with ID {dashboard_id}.")
            return {"error": f"No dashboard found with ID '{dashboard_id}'"}

        self.logger.info(f"Successfully retrieved dashboard with ID {dashboard_id}.")
        return dashboard_data

    def get_dashboard_by_name(self, dashboard_name):
        """
        Retrieves a specific dashboard by its name.

        Parameters:
            dashboard_name (str): The name of the dashboard to retrieve.

        Returns:
            dict or list: A dictionary containing dashboard details if found,
                        or {'error': 'message'} if not found or failed.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&name={dashboard_name}"
        self.logger.debug(f"Fetching dashboard with name {dashboard_name} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            error_msg = f"GET request to retrieve dashboard {dashboard_name} failed: No response received."
            self.logger.error(error_msg)
            return {"error": error_msg}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboard {dashboard_name}. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboard '{dashboard_name}'. {error_message}"}

        dashboard_data = response.json()
        if not dashboard_data:
            warning_msg = f"No dashboard found with name {dashboard_name}."
            self.logger.warning(warning_msg)
            return {"error": warning_msg}

        self.logger.info(f"Successfully retrieved dashboard with name {dashboard_name}.")
        return dashboard_data

    def add_dashboard_script(self, dashboard_id, script, executing_user=None):
        """
        Adds or overwrites a script to a dashboard, temporarily changing ownership if required.

        Parameters:
            dashboard_id (str): The ID of the dashboard where the script will be added.
            script (str): The JavaScript script as either:
                        - A properly formatted JSON string.
                        - A raw Python docstring (multi-line string).
            executing_user (str, optional): The username of the API user. This is used to temporarily change
                                        the owner of the dashboard, as only the owner can add scripts.
                                        If not provided, assumes the dashboard owner is the same as the API user.

        Returns:
            str: Success message or error details.
        """

        add_dashboard_script_endpoint = f"/api/dashboards/{dashboard_id}"

        # If executing_user is provided, temporarily change dashboard ownership
        if executing_user:
            self.logger.debug(
                f"API username '{executing_user}' provided. "
                f"Fetching original owner of dashboard {dashboard_id}."
            )

            dashboard_response = self.api_client.get(
                f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}"
                f"&asObject=false"
            )
            if dashboard_response is None or dashboard_response.status_code != 200:
                self.logger.error(f"Dashboard with ID '{dashboard_id}' not found or failed to retrieve.")
                return f"Error: Dashboard '{dashboard_id}' not found."

            dashboard_data = dashboard_response.json()
            original_owner_id = dashboard_data[0].get("owner")

            # Fetch existing dashboard shares before changing ownership
            self.logger.debug(f"Retrieving existing shares of dashboard {dashboard_id} to restore later.")
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")

            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(f"Failed to retrieve shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to retrieve shares for dashboard {dashboard_id}."

            shares = shares_response.json().get("sharesTo", [])

            # Change ownership to executing_user
            self.logger.info(f"Changing ownership of dashboard {dashboard_id} to '{executing_user}'.")
            api_user = self.access_mgmt.get_user(executing_user)
            api_user_id = api_user.get("USER_ID")

            if not api_user_id:
                self.logger.error(f"User '{executing_user}' not found.")
                return f"Error: User '{executing_user}' not found."

            ownership_response = self.api_client.post(
                f"/api/v1/dashboards/{dashboard_id}/change_owner?adminAccess=true",
                data={"ownerId": api_user_id, "originalOwnerRule": "edit"}
            )

            if ownership_response is None or ownership_response.status_code != 200:
                error_message = ownership_response.json() if ownership_response else "No response received."
                self.logger.error(f"Failed to change ownership of dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to change ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully changed to '{executing_user}'.")
        else:
            self.logger.debug("No API username provided. Assuming the dashboard owner is the same as the API user.")

        # Convert script to JSON format if needed
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Dashboard Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)

            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final dashboard script payload prepared: {script_dict}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for dashboard script.")
            return "Error: Dashboard Script must be a valid JSON string."

        # Add script to the dashboard
        # Add script to the dashboard
        script_response = self.api_client.put(add_dashboard_script_endpoint, data=script_dict)

        if script_response is None or script_response.status_code != 200:
            try:
                error_message = script_response.json()
            except Exception:
                error_message = getattr(script_response, 'text', 'No response text')

            self.logger.error(f"Failed to add script to dashboard {dashboard_id}. Error: {error_message}")

            if script_response.status_code == 404 and executing_user is None:
                return (
                    f"Error: Failed to add dashboard script to dashboard '{dashboard_id}'. "
                    f"This may be because the API token used does not belong to the dashboard owner, "
                    f"and no 'executing_user' was provided. Only the dashboard owner can modify scripts."
                    f" Please provide the 'executing_user' parameter to change ownership temporarily or "
                    f"set the API token user as dashboard owner."
                )

            return f"Error: Failed to add dashboard script to dashboard {dashboard_id}."

        self.logger.info(f"Dashboard Script successfully added to dashboard {dashboard_id}.")

        # Restore original ownership if changed
        if executing_user:
            self.logger.info(f"Restoring original ownership of dashboard {dashboard_id} to '{original_owner_id}'.")

            shares_payload = [
                {
                    "shareId": s["shareId"],
                    "type": s["type"],
                    "rule": s.get("rule", "edit"),
                    "subscribe": s.get("subscribe", False)
                }
                for s in shares
            ]

            restore_shares_response = self.api_client.post(
                f"/api/shares/dashboard/{dashboard_id}",
                data={"sharesTo": shares_payload}
            )

            if restore_shares_response is None or restore_shares_response.status_code != 200:
                error_message = restore_shares_response.json() if restore_shares_response else "No response received."
                self.logger.error(f"Failed to restore shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to restore shares for dashboard {dashboard_id}."

            ownership_restore_response = self.api_client.post(
                f"/api/v1/dashboards/{dashboard_id}/change_owner",
                data={"ownerId": original_owner_id, "originalOwnerRule": "edit"}
            )

            if ownership_restore_response is None or ownership_restore_response.status_code != 200:
                error_message = (
                    ownership_restore_response.json()
                    if ownership_restore_response else "No response received."
                )
                self.logger.error(
                    f"Failed to revert ownership of dashboard {dashboard_id} to original owner. "
                    f"Error: {error_message}"
                )
                return f"Error: Failed to revert ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully restored to original owner.")

        return "Dashboard Script added successfully."

    def add_widget_script(self, dashboard_id, widget_id, script, executing_user=None):
        """
        Adds or overwrites a script for a specific widget within a dashboard.

        If required, temporarily changes the dashboard ownership, as only the owner can modify widget scripts.

        Parameters:
            dashboard_id (str): The ID of the dashboard containing the widget.
            widget_id (str): The ID of the widget where the script will be added.
            script (str): The JavaScript script as either:
                        - A properly formatted JSON string.
                        - A raw Python docstring (multi-line string).
            executing_user (str, optional): The username of the API user. This is used to temporarily change
                                        the owner of the dashboard, as only the owner can add scripts.
                                        If not provided, assumes the dashboard owner is the same as the API user.

        Returns:
            str: Success message or error details.
        """

        add_widget_script_endpoint = f"/api/dashboards/{dashboard_id}/widgets/{widget_id}"

        # If executing_user is provided, temporarily change dashboard ownership
        if executing_user:
            self.logger.debug(
                f"API username '{executing_user}' provided. "
                f"Fetching original owner of dashboard {dashboard_id}."
            )

            dashboard_response = self.api_client.get(
                f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}&asObject=false"
            )
            if dashboard_response is None or dashboard_response.status_code != 200:
                self.logger.error(f"Dashboard with ID '{dashboard_id}' not found or failed to retrieve.")
                return f"Error: Dashboard '{dashboard_id}' not found."

            dashboard_data = dashboard_response.json()
            original_owner_id = dashboard_data[0].get("owner")

            # Fetch existing dashboard shares before changing ownership
            self.logger.debug(f"Retrieving existing shares of dashboard {dashboard_id} to restore later.")
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")

            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(f"Failed to retrieve shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to retrieve shares for dashboard {dashboard_id}."

            shares = shares_response.json().get("sharesTo", [])

            # Change ownership to executing_user
            self.logger.info(f"Changing ownership of dashboard {dashboard_id} to '{executing_user}'.")
            api_user = self.access_mgmt.get_user(executing_user)
            api_user_id = api_user.get("USER_ID")

            if not api_user_id:
                self.logger.error(f"User '{executing_user}' not found.")
                return f"Error: User '{executing_user}' not found."

            ownership_response = self.api_client.post(
                f"/api/v1/dashboards/{dashboard_id}/change_owner?adminAccess=true",
                data={"ownerId": api_user_id, "originalOwnerRule": "edit"}
            )

            if ownership_response is None or ownership_response.status_code != 200:
                error_message = ownership_response.json() if ownership_response else "No response received."
                self.logger.error(f"Failed to change ownership of dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to change ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully changed to '{executing_user}'.")
        else:
            self.logger.debug("No API username provided. Assuming the dashboard owner is the same as the API user.")

        # Convert script to JSON format if needed
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Widget Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)

            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final widget script payload prepared: {script_dict}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for widget script.")
            return "Error: Widget Script must be a valid JSON string."

        # Add script to the dashboard
        script_response = self.api_client.put(add_widget_script_endpoint, data=script_dict)

        if script_response is None or script_response.status_code != 200:
            try:
                error_message = script_response.json()
            except Exception:
                error_message = getattr(script_response, 'text', 'No response text')

            self.logger.error(
                f"Failed to add widget script to dashboard {dashboard_id} "
                f"widget {widget_id}. Error: {error_message}"
            )

            if script_response.status_code == 403 and executing_user is None:
                return (
                    f"Error: Failed to add widget script to dashboard '{dashboard_id}', widget '{widget_id}'. "
                    f"This may be because the API token used does not belong to the dashboard owner, "
                    f"and no 'executing_user' was provided. Only the dashboard owner can modify scripts."
                    f" Please provide the 'executing_user' parameter to change ownership temporarily or "
                    f"set the API token user as dashboard owner."
                )

            return f"Error: Failed to add widget script to dashboard {dashboard_id} widget {widget_id}."

        self.logger.info(f"Widget Script successfully added to dashboard {dashboard_id} widget {widget_id}.")

        # Republish the dashboard to apply changes
        self.logger.info(f"Republishing dashboard {dashboard_id} to apply changes.")
        republish_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/publish?force=true")
        if republish_response.status_code == 204:
            self.logger.info(f"Dashboard {dashboard_id} republished successfully.")
        else:
            error_message = republish_response.json() if republish_response else "No response received."
            self.logger.error(f"Failed to republish dashboard {dashboard_id}. Error: {error_message}")
            return f"Error: Failed to republish dashboard {dashboard_id}. Error: {error_message}"

        # Restore original ownership if changed
        if executing_user:
            self.logger.info(f"Restoring original ownership of dashboard {dashboard_id} to '{original_owner_id}'.")

            shares_payload = [
                {
                    "shareId": s["shareId"],
                    "type": s["type"],
                    "rule": s.get("rule", "edit"),
                    "subscribe": s.get("subscribe", False)
                }
                for s in shares
            ]

            restore_shares_response = self.api_client.post(
                f"/api/shares/dashboard/{dashboard_id}",
                data={"sharesTo": shares_payload}
            )

            if restore_shares_response is None or restore_shares_response.status_code != 200:
                error_message = restore_shares_response.json() if restore_shares_response else "No response received."
                self.logger.error(f"Failed to restore shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to restore shares for dashboard {dashboard_id}."

            ownership_restore_response = self.api_client.post(
                f"/api/v1/dashboards/{dashboard_id}/change_owner",
                data={"ownerId": original_owner_id, "originalOwnerRule": "edit"}
            )

            if ownership_restore_response is None or ownership_restore_response.status_code != 200:
                error_message = (
                    ownership_restore_response.json()
                    if ownership_restore_response else "No response received."
                )
                self.logger.error(
                    f"Failed to revert ownership of dashboard {dashboard_id} to original owner. "
                    f"Error: {error_message}"
                )
                return f"Error: Failed to revert ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully restored to original owner.")

        return "Widget Script added successfully."

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
            self.logger.warning(
                f"Failed to retrieve existing shares for dashboard {dashboard_id} with admin access. "
                f"Trying without admin access."
            )
            # Try without admin access
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}")
            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(
                    f"Failed to retrieve existing shares for dashboard {dashboard_id}. "
                    f"Error: {error_message}"
                )
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
                    self.logger.info(
                        f"Updating rule for existing user {user['shareId']} from "
                        f"'{existing_share_map[user['shareId']]}' to '{user['rule']}'."
                    )
                    updated_users.append(user)
            else:
                new_users.append(user)

        for group in groups:
            if group["shareId"] in existing_share_map:
                if group["rule"] != existing_share_map[group["shareId"]]:  # Rule change detected
                    self.logger.info(
                        f"Updating rule for existing group {group['shareId']} from "
                        f"'{existing_share_map[group['shareId']]}' to '{group['rule']}'."
                    )
                    updated_groups.append(group)
            else:
                new_groups.append(group)

        if not new_users and not new_groups and not updated_users and not updated_groups:
            reason = "All provided users/groups already have access with the same rule."
            self.logger.info(f"No new or updated shares for dashboard {dashboard_id}. Reason: {reason}")
            return f"No new or updated shares added. Reason: {reason}"

        # Remove updated users/groups from existing_shares to prevent duplication
        existing_shares = [
            share for share in existing_shares if share["shareId"] not in {user["shareId"] for user in updated_users}
        ]
        existing_shares = [
            share for share in existing_shares if share["shareId"] not in {group["shareId"] for group in updated_groups}
        ]
        # Prepare final payload (keeping existing shares + new shares + updated shares)
        payload = {"sharesTo": existing_shares + new_users + new_groups + updated_users + updated_groups}
        self.logger.debug(f"Final payload for adding/updating shares: {payload}")

        # Make the POST request to update shares
        try:
            response = self.api_client.post(endpoint, data=payload)

            # If response is None or failed, try fallback endpoint
            if response is None or response.status_code != 200:
                self.logger.warning(
                    f"POST to '{endpoint}' failed for dashboard '{dashboard_id}'. Trying fallback without admin access."
                )
                fallback_endpoint = f"/api/shares/dashboard/{dashboard_id}"
                response = self.api_client.post(fallback_endpoint, data=payload)

                # If fallback also fails, return error
                if response is None or response.status_code != 200:
                    error_message = (
                        response.json() if response and response.content else "No response received."
                    )
                    self.logger.error(
                        (
                            f"Failed to add/update shares for dashboard '{dashboard_id}' via fallback. "
                            f"Error: {error_message}"
                        )
                    )
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

    def get_dashboard_columns(self, dashboard_name):
        """
        Retrieves columns from a specific dashboard, including both widget and filter-level columns.

        This method:
        - Uses the `get_dashboard_by_name` method to fetch the dashboard.
        - Extracts columns from widgets and filters.
        - Deduplicates the final column list.

        Parameters:
            dashboard_name (str): The name of the dashboard to retrieve columns from.

        Returns:
            list: A list of dictionaries containing distinct table and column information from the dashboard.
        """
        self.logger.info(f"Starting column retrieval for dashboard: {dashboard_name}")

        dashboard_columns = []

        # Step 1: Get dashboard details using existing method
        dashboard = self.get_dashboard_by_name(dashboard_name)
        if not dashboard or 'error' in dashboard:
            error_msg = f"Dashboard '{dashboard_name}' not found."
            self.logger.error(error_msg)
            return []
        dashboard_id = dashboard[0].get("oid")
        self.logger.info(f"Dashboard '{dashboard_name}' found with ID: {dashboard_id}")

        # Step 2: Export full dashboard metadata
        dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
        dashboard_response = self.api_client.get(dashboard_url)

        if not dashboard_response or dashboard_response.status_code != 200:
            self.logger.error(f"Failed to export dashboard with ID '{dashboard_id}'")
            return []

        try:
            dashboard_data = dashboard_response.json()
        except Exception:
            self.logger.exception(f"Failed to parse dashboard export response for ID '{dashboard_id}'")
            return []

        if not dashboard_data or not isinstance(dashboard_data, list):
            self.logger.error(f"Unexpected dashboard data structure for ID '{dashboard_id}'")
            return []

        dashboard = dashboard_data[0]
        self.logger.debug(f"Analyzing dashboard '{dashboard['title']}' (ID: {dashboard_id})")

        # Step 3: Extract columns from filters
        filter_count = 0
        self.logger.debug(f"Extracting columns from filters for dashboard '{dashboard_name}'")

        if "filters" in dashboard:
            total_filters = len(dashboard["filters"])
            self.logger.debug(f"Total filters found: {total_filters}")

            for filter_index, filter in enumerate(dashboard["filters"], start=1):
                filter_count += 1
                self.logger.debug(f"Processing filter {filter_index}/{total_filters}")

                if "levels" in filter:
                    levels_count = len(filter["levels"])
                    self.logger.debug(f"Filter {filter_index}: Extracting {levels_count} levels")

                    for level in filter["levels"]:
                        dim_value = level.get("dim", "Unknown.Table")
                        table, column = (
                            dim_value.strip("[]").split(".", 1)
                            if "." in dim_value
                            else (dim_value.strip("[]"), "Unknown Column")
                        )

                        dashboard_columns.append({
                            "dashboard_name": dashboard_name,
                            "source": "filter",
                            "widget_id": "N/A",
                            "table": table,
                            "column": column
                        })

                        self.logger.debug(
                            f"Filter {filter_index}: Extracted from levels - Table: {table}, "
                            f"Column: {column}"
                        )

                elif "jaql" in filter:
                    dim_value = filter["jaql"].get("dim", "Unknown.Table")
                    table, column = (
                        dim_value.strip("[]").split(".", 1)
                        if "." in dim_value
                        else (dim_value.strip("[]"), "Unknown Column")
                    )

                    dashboard_columns.append({
                        "dashboard_name": dashboard_name,
                        "source": "filter",
                        "widget_id": "N/A",
                        "table": table,
                        "column": column
                    })

                    self.logger.debug(f"Filter {filter_index}: Extracted from JAQL - Table: {table}, Column: {column}")

        self.logger.info(f"Processed {filter_count} filters for dashboard '{dashboard_name}'")

        # Step 4: Extract columns from widgets
        total_widgets = len(dashboard.get("widgets", []))
        column_count = 0

        self.logger.debug(f"Extracting columns from {total_widgets} widgets in dashboard '{dashboard_name}'")

        for widget_index, widget in enumerate(dashboard.get("widgets", []), start=1):
            # Safely access widget ID and handle potential issues
            try:
                columns = dashboard.get("layout", {}).get("columns", [])
                if not columns:
                    self.logger.warning(f"No columns found in dashboard layout for widget index {widget_index}")
                    widget_id = "Unknown Widget ID"
                else:
                    cells = columns[0].get("cells", [])
                    if len(cells) < widget_index:
                        self.logger.warning(f"Insufficient cells in layout for widget index {widget_index}")
                        widget_id = "Unknown Widget ID"
                    else:
                        subcells = cells[widget_index - 1].get("subcells", [])
                        if not subcells or not subcells[0].get("elements"):
                            self.logger.warning(f"No elements found in subcell for widget index {widget_index}")
                            widget_id = "Unknown Widget ID"
                        else:
                            widget_id = subcells[0]["elements"][0].get("widgetid", "Unknown Widget ID")
            except Exception:
                self.logger.exception(f"Exception occurred while extracting widget ID for index {widget_index}")
                widget_id = "Unknown Widget ID"

            widget_title = widget.get("title", "Unnamed Widget")

            self.logger.debug(
                f"Processing widget {widget_index}/{total_widgets} - ID: {widget_id}, "
                f"Title: {widget_title}"
            )

            for panel in widget.get("metadata", {}).get("panels", []):
                for item in panel.get("items", []):
                    jaql = item.get("jaql", {})

                    # Case 1: Extract from 'context' (Formula-based columns)
                    if "context" in jaql and isinstance(jaql["context"], dict):
                        for context_key, value in jaql["context"].items():
                            dim_value = value.get("dim", "Unknown.Table")
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({
                                "dashboard_name": dashboard_name,
                                "source": "widget",
                                "widget_id": widget_id,
                                "table": table,
                                "column": column
                            })
                            column_count += 1

                            self.logger.debug(
                                f"Widget {widget_index}: Extracted from context (Formula) - "
                                f"Key: {context_key}, Table: {table}, Column: {column}"
                            )

                    # Case 2: Extract from 'dim' (Regular columns)
                    else:
                        dim_value = jaql.get("dim", "Unknown.Table")
                        if "." in dim_value:
                            table, column = dim_value.strip("[]").split(".", 1)
                        else:
                            table, column = dim_value.strip("[]"), "Unknown Column"

                        dashboard_columns.append({
                            "dashboard_name": dashboard_name,
                            "source": "widget",
                            "widget_id": widget_id,
                            "table": table,
                            "column": column
                        })
                        column_count += 1

                        self.logger.debug(
                            f"Widget {widget_index}: Extracted from regular source - Table: {table}, "
                            f"Column: {column}"
                        )

        self.logger.info(
            f"Processed {total_widgets} widgets and extracted {column_count} columns "
            f"for dashboard '{dashboard_name}'"
        )

        # Step 5: Deduplicate columns based on 'table' and 'column'
        distinct_columns_set = set()
        distinct_dashboard_columns = []

        for entry in dashboard_columns:
            table = entry["table"]
            column = entry["column"]

            # Remove (Calendar) from column names if present
            if column.endswith(" (Calendar)"):
                column = column.replace(" (Calendar)", "").strip()

            key = (table, column)
            if key not in distinct_columns_set:
                distinct_dashboard_columns.append(entry)
                distinct_columns_set.add(key)

        self.logger.info(
            f"Retrieved {len(distinct_dashboard_columns)} distinct columns from dashboard "
            f"'{dashboard_name}'"
        )

        return distinct_dashboard_columns

    def get_dashboard_share(self, dashboard_name):
        """
        Retrieves share details (users and groups) for a specific dashboard by title.

        Args:
            dashboard_name (str): The title of the dashboard to retrieve share information for.

        Returns:
            list: A list of dictionaries containing share type (user or group), and share name (email or group name),
                or an empty list if the dashboard is not found or has no shares.
        """
        self.logger.info(f"Fetching share details for dashboard: '{dashboard_name}'")

        # Step 1: Retrieve dashboard(s) by name
        dashboards = self.get_dashboard_by_name(dashboard_name)

        # Handle case where response is a list
        if isinstance(dashboards, list):
            dashboard = next((d for d in dashboards if d.get("title", "").lower() == dashboard_name.lower()), None)
        else:
            dashboard = dashboards

        if not dashboard:
            self.logger.warning(f"Dashboard '{dashboard_name}' not found.")
            return []

        shares = dashboard.get("shares", [])
        if not shares:
            self.logger.info(f"Dashboard '{dashboard_name}' has no shares.")
            return []

        # Step 2: Fetch all users
        users_response = self.api_client.get('/api/v1/users')
        if not users_response or users_response.status_code != 200:
            self.logger.error("Failed to fetch users.")
            return []

        users_data = users_response.json()
        users_detail = {user["_id"]: user.get("email", "Unknown Email") for user in users_data}

        # Step 3: Fetch all groups
        groups_response = self.api_client.get('/api/v1/groups')
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
                shared_list.append({
                    "type": "user",
                    "name": users_detail[share_id]
                })
            elif share_type == "group" and share_id in groups_detail:
                shared_list.append({
                    "type": "group",
                    "name": groups_detail[share_id]
                })

        self.logger.info(f"Found {len(shared_list)} shares for dashboard '{dashboard_name}'.")
        return shared_list
