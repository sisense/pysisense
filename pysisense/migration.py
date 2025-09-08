from .sisenseclient import SisenseClient
import time


class Migration:

    def __init__(self, source_yaml, target_yaml, debug=False):
        """
        Initializes the Migration class with API clients and Access Management for both source and target environments.

        Parameters:
            source_yaml (str): Path to the YAML file for source environment configuration.
            target_yaml (str): Path to the YAML file for target environment configuration.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Initialize API clients for both source and target environments
        self.source_client = SisenseClient(config_file=source_yaml, debug=debug)
        self.target_client = SisenseClient(config_file=target_yaml, debug=debug)

        # Use the logger from the source client for consistency
        self.logger = self.source_client.logger

    def migrate_groups(self, group_name_list):
        """
        Migrates specific groups from the source environment to the target environment using the bulk endpoint.

        Parameters:
            group_name_list (list): A list of group names to migrate.

        Returns:
            list: A list of group migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        self.logger.debug("Fetching groups from the source environment.")
        source_response = self.source_client.get("/api/v1/groups")
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the source environment.")
            return []
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_groups = source_response.json()
        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter the groups to migrate
        bulk_group_data = []
        for group in source_groups:
            if group["name"] in group_name_list:
                # Prepare group data excluding unnecessary fields
                group_data = {
                    key: value for key, value in group.items()
                    if key not in ["created", "lastUpdated", "tenantId", "_id"]
                }
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # If no groups match, log an info message and exit early
        if not bulk_group_data:
            self.logger.info("No matching groups found for migration. Ending process.")
            return [{
                "message": (
                    "No matching groups found for migration. Ending process. "
                    "Please verify the group names and try again."
                )
            }]

        # Step 3: Make the bulk POST request with the group data
        self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
        self.logger.debug(f"Payload for bulk migration: {bulk_group_data}")
        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        # Log the full response at debug level
        self.logger.debug(
            f"Target environment response status code: "
            f"{response.status_code if response else 'No response'}"
        )
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # If response is missing or empty
        if response is None:
            self.logger.error("No response received from the migration API.")
            return {
                "results": [{"name": group["name"], "status": "Failed"} for group in bulk_group_data],
                "raw_error": "No response received from the migration API."
            }
        elif not response.text.strip():
            self.logger.error(f"Empty response body received. Status code: {response.status_code}")
            return {
                "results": [{"name": group["name"], "status": "Failed"} for group in bulk_group_data],
                "raw_error": f"Empty response body. Status code: {response.status_code}"
            }

        # Step 4: Handle the response from the bulk API call
        migration_results = []
        raw_error = None

        if response and response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated groups)
                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    self.logger.info(f"Successfully migrated group: {group_name}")
                    migration_results.append({"name": group_name, "status": "Success"})
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                # Assume success if status code is correct but response is not JSON
                migration_results = [{"name": group["name"], "status": "Success"} for group in bulk_group_data]
        else:
            # Log and handle unsuccessful status codes
            try:
                raw_error = response.json()
            except Exception:
                raw_error = response.text or "Unknown error"

            self.logger.error(f"Bulk migration failed. Status code: {response.status_code}")
            self.logger.error(f"Raw error response: {raw_error}")
            migration_results = [{"name": group["name"], "status": "Failed"} for group in bulk_group_data]

        # Summary
        success_count = sum(1 for r in migration_results if r["status"] == "Success")
        self.logger.info(
            f"Finished migrating groups. Successfully migrated {success_count} "
            f"out of {len(bulk_group_data)} groups."
        )

        # Return results and raw error if any
        return {
            "results": migration_results,
            "total_count": len(bulk_group_data),
            "raw_error": raw_error
        }

    def migrate_all_groups(self):
        """
        Migrates all groups from the source environment to the target environment using the bulk endpoint.

        Returns:
            list: A list of group migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        self.logger.debug("Fetching groups from the source environment.")
        source_response = self.source_client.get("/api/v1/groups")
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the source environment.")
            return [
                {
                    "message": (
                        "Failed to retrieve groups from the source environment. "
                        "Please check the logs for more details."
                    )
                }
            ]

        # Log the full response at debug level
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_groups = source_response.json()
        if not source_groups:
            self.logger.info("No groups found in the source environment. Ending process.")
            return [{"message": "No groups found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter out specific groups
        bulk_group_data = []
        for group in source_groups:
            if group["name"] not in ["Admins", "All users in system", "Everyone"]:
                # Prepare group data excluding unnecessary fields
                group_data = {
                    key: value for key, value in group.items()
                    if key not in ["created", "lastUpdated", "tenantId", "_id"]
                }
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # If no groups to migrate, log and exit early
        if not bulk_group_data:
            self.logger.info("No eligible groups found for migration. Ending process.")
            return [{"message": "No eligible groups found for migration. Please verify the group list and try again."}]

        # Step 3: Make the bulk POST request with the group data
        self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
        self.logger.debug(f"Payload for bulk migration: {bulk_group_data}")
        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        # Log the full response at debug level
        status_code = response.status_code if response else 'No response'
        self.logger.debug(f"Target environment response status code: {status_code}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 4: Handle the response from the bulk API call
        migration_results = []
        raw_error = None

        if response and response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated groups)
                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    self.logger.info(f"Successfully migrated group: {group_name}")
                    migration_results.append({"name": group_name, "status": "Success"})
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                # Assume success if status code is correct but response is not JSON
                migration_results = [
                    {"name": group_data["name"], "status": "Success"}
                    for group_data in bulk_group_data
                ]
        else:
            try:
                raw_error = response.json()
            except Exception as e:
                self.logger.warning(f"Failed to parse error response as JSON. Falling back to raw text. Error: {e}")
                raw_error = response.text if response and response.text else "No response body"

            self.logger.error(
                f"Bulk migration failed. Status code: "
                f"{response.status_code if response else 'No response'}"
            )
            self.logger.error(f"Raw error response: {raw_error}")
            migration_results = [{"name": group_data["name"], "status": "Failed"} for group_data in bulk_group_data]

        # Summary log
        success_count = sum(1 for r in migration_results if r["status"] == "Success")
        self.logger.info(
            f"Finished migrating groups. Successfully migrated {success_count} "
            f"out of {len(source_groups)} groups."
        )

        # Return structured response along with raw error if applicable
        return {
            "results": migration_results,
            "total_count": len(bulk_group_data),
            "raw_error": raw_error
        }

    def migrate_users(self, user_name_list):
        """
        Migrates specific users from the source environment to the target environment.

        Parameters:
            user_name_list (list): A list of user names to migrate.

        Returns:
            list: A list of user migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting user migration from source to target.")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Step 1: Get all users from the source environment
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve users from the source environment.")
            return [{
                "message": (
                    "Failed to retrieve users from the source environment. "
                    "Please check the logs for more details."
                )
            }]
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_users = source_response.json()
        if not source_users:
            self.logger.info("No users found in the source environment. Ending process.")
            return [{"message": "No users found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        self.logger.debug("Fetching roles and groups from the target environment.")
        target_roles_response = self.target_client.get("/api/roles")
        target_groups_response = self.target_client.get("/api/v1/groups")

        if not target_roles_response or target_roles_response.status_code != 200:
            self.logger.error("Failed to retrieve roles from the target environment.")
            return [{
                "message": (
                    "Failed to retrieve roles from the target environment. "
                    "Please check the logs for details."
                )
            }]

        if not target_groups_response or target_groups_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the target environment.")
            return [{
                "message": (
                    "Failed to retrieve groups from the target environment. "
                    "Please check the logs for details."
                )
            }]

        target_roles = target_roles_response.json()
        target_groups = target_groups_response.json()
        self.logger.debug(f"Retrieved {len(target_roles)} roles from the target environment.")
        self.logger.debug(f"Retrieved {len(target_groups)} groups from the target environment.")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}

        # Step 3: Find and process the users based on the input list
        bulk_user_data = []  # List to hold data for all users to be migrated
        for user in source_users:
            if user["email"] in user_name_list:  # Match users by email
                # Construct the required payload for the user
                user_data = {
                    "email": user["email"],
                    "firstName": user["firstName"],
                    "lastName": user.get("lastName", ""),  # Optional field
                    "roleId": next(
                        (role["_id"] for role in target_roles if role["name"] == user["role"]["name"]),
                        None
                    ),
                    "groups": [
                        group["_id"] for group in target_groups
                        if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS
                    ],
                    "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language.
                }

                # Append user data to the bulk list
                bulk_user_data.append(user_data)
                self.logger.debug(f"Prepared data for user: {user['email']}")

        # If no matching users, log and exit
        if not bulk_user_data:
            self.logger.info("No matching users found for migration. Ending process.")
            return [{"message": "No matching users found for migration. Please verify the user list and try again."}]

        # Step 4: Make the POST request with the bulk user data
        self.logger.info(f"Sending bulk migration request for {len(bulk_user_data)} users")
        self.logger.debug(f"Payload for bulk user migration: {bulk_user_data}")
        response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)

        # Log the full response for debugging
        status_code = response.status_code if response else 'No response'
        self.logger.debug(f"Target environment response status code: {status_code}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 5: Early exit if response is missing or empty
        if response is None:
            self.logger.error("No response received from the migration API.")
            return {
                "results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data],
                "raw_error": "No response received from the migration API."
            }
        elif not response.text.strip():
            self.logger.error(f"Empty response body received. Status code: {response.status_code}")
            return {
                "results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data],
                "raw_error": f"Empty response body. Status code: {response.status_code}"
            }

        # Step 6: Handle the response
        migration_results = []
        raw_error = None

        if response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk user migration succeeded. Response: {response_data}")
                for user in response_data:
                    user_name = user.get("email", "Unknown User")
                    self.logger.info(f"Successfully migrated user: {user_name}")
                    migration_results.append({"name": user_name, "status": "Success"})
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                migration_results = [{"name": user["email"], "status": "Success"} for user in bulk_user_data]
        else:
            try:
                raw_error = response.json()
            except Exception:
                raw_error = response.text or "Unknown error"

            self.logger.error(f"Bulk user migration failed. Status code: {response.status_code}")
            self.logger.error(f"Raw error response: {raw_error}")
            migration_results = [{"name": user["email"], "status": "Failed"} for user in bulk_user_data]

        # Step 7: Final summary
        success_count = sum(1 for r in migration_results if r["status"] == "Success")
        self.logger.info(
            f"Finished migrating users. Successfully migrated {success_count} "
            f"out of {len(bulk_user_data)} users."
        )

        # Step 8: Return structured result
        return {
            "results": migration_results,
            "total_count": len(bulk_user_data),
            "raw_error": raw_error
        }

    def migrate_all_users(self):
        """
        Migrates all users from the source environment to the target environment using the bulk endpoint.

        Returns:
            list: A list of user migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting full user migration from source to target.")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Step 1: Get all users from the source environment
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve users from the source environment.")
            return [{
                "message": (
                    "Failed to retrieve users from the source environment. "
                    "Please check the logs for details."
                )
            }]

        # Log the full response at debug level
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_users = source_response.json()
        if not source_users:
            self.logger.info("No users found in the source environment. Ending process.")
            return [{"message": "No users found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        self.logger.debug("Fetching roles and groups from the target environment.")
        target_roles_response = self.target_client.get("/api/roles")
        target_groups_response = self.target_client.get("/api/v1/groups")

        if not target_roles_response or target_roles_response.status_code != 200:
            self.logger.error("Failed to retrieve roles from the target environment.")
            return [{
                "message": (
                    "Failed to retrieve roles from the target environment. "
                    "Please check the logs for details."
                )
            }]

        if not target_groups_response or target_groups_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the target environment.")
            return [{
                "message": (
                    "Failed to retrieve groups from the target environment. "
                    "Please check the logs for details."
                )
            }]

        target_roles = target_roles_response.json()
        target_groups = target_groups_response.json()
        self.logger.debug(f"Retrieved {len(target_roles)} roles from the target environment.")
        self.logger.debug(f"Retrieved {len(target_groups)} groups from the target environment.")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        bulk_user_data = []  # List to hold the user data for bulk upload

        # Step 3: Process each user and prepare the payload for the bulk endpoint
        for user in source_users:
            if user["role"]["name"] == 'super':
                continue  # Skip sysadmin users as they are not migrated
            user_data = {
                "email": user["email"],
                "firstName": user["firstName"],
                "lastName": user.get("lastName", ""),  # Optional field
                "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),
                "groups": [
                    group["_id"] for group in target_groups
                    if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS
                ],
                "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language preference
            }

            bulk_user_data.append(user_data)
            self.logger.debug(f"Prepared data for user: {user['email']}")

        # Step 4: Make the bulk POST request with the user data
        if not bulk_user_data:
            self.logger.info("No users to migrate. Ending process.")
            return {
                "results": [],
                "raw_error": "No users to migrate. Nothing to process."
            }

        self.logger.info(f"Sending bulk migration request for {len(bulk_user_data)} users")
        self.logger.debug(f"Payload for bulk user migration: {bulk_user_data}")
        response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)

        # Log the full response for debugging
        status_code = response.status_code if response else 'No response'
        self.logger.debug(f"Target environment response status code: {status_code}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 5: Handle missing or empty response
        if response is None:
            self.logger.error("No response received from the migration API.")
            return {
                "results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data],
                "raw_error": "No response received from the migration API."
            }
        elif not response.text.strip():
            self.logger.error(f"Empty response body received. Status code: {response.status_code}")
            return {
                "results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data],
                "raw_error": f"Empty response body. Status code: {response.status_code}"
            }

        # Step 6: Parse and process the response
        migration_results = []
        raw_error = None

        if response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                for user in response_data:
                    user_email = user.get("email", "Unknown User")
                    self.logger.info(f"Successfully migrated user: {user_email}")
                    migration_results.append({"name": user_email, "status": "Success"})
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                migration_results = [{"name": user["email"], "status": "Success"} for user in bulk_user_data]
        else:
            try:
                raw_error = response.json()
            except Exception:
                raw_error = response.text or "Unknown error"

            self.logger.error(f"Bulk user migration failed. Status code: {response.status_code}")
            self.logger.error(f"Raw error response: {raw_error}")
            migration_results = [{"name": user["email"], "status": "Failed"} for user in bulk_user_data]

        # Step 7: Summary
        success_count = sum(1 for r in migration_results if r["status"] == "Success")
        self.logger.info(
            f"Finished migrating users. Successfully migrated {success_count} "
            f"out of {len(bulk_user_data)} users."
        )

        # Step 8: Return structured result
        return {
            "results": migration_results,
            "total_count": len(bulk_user_data),
            "raw_error": raw_error
        }

    def migrate_dashboard_shares(self, source_dashboard_ids, target_dashboard_ids, change_ownership=False):
        """
        Migrates shares for specific dashboards from the source to the target environment.

        Parameters:
            source_dashboard_ids (list): A list of dashboard IDs from the source environment to fetch shares from.
            target_dashboard_ids (list): A list of dashboard IDs from the target environment to apply shares to.
            change_ownership (bool, optional): Whether to change ownership of the target dashboard. Defaults to False.

        Returns:
            dict: A summary of the share migration process with counts of succeeded and failed shares,
                and details of failed dashboards.

        Raises:
            ValueError: If `source_dashboard_ids` or `target_dashboard_ids` are not provided,
                        or if their lengths do not match.
        """
        dashboard_results = []

        if not source_dashboard_ids or not target_dashboard_ids:
            raise ValueError("Both 'source_dashboard_ids' and 'target_dashboard_ids' must be provided.")
        if len(source_dashboard_ids) != len(target_dashboard_ids):
            raise ValueError("The lengths of 'source_dashboard_ids' and 'target_dashboard_ids' must match.")

        self.logger.info("Starting share migration for specified dashboards.")
        self.logger.debug(f"Source Dashboard IDs: {source_dashboard_ids}")
        self.logger.debug(f"Target Dashboard IDs: {target_dashboard_ids}")

        share_migration_summary = {'new_share_success_count': 0, 'share_fail_count': 0, 'failed_dashboards': []}

        # Step 1: Fetch users and groups once
        self.logger.info("Fetching users and groups from source and target environments.")
        try:
            # Fetch source users and groups
            source_users = self.source_client.get("/api/v1/users").json()
            source_user_map = {user["_id"]: user["email"] for user in source_users}
            source_groups = self.source_client.get("/api/v1/groups").json()
            source_group_map = {group["_id"]: group["name"] for group in source_groups}

            # Fetch target users and groups
            target_users = self.target_client.get("/api/v1/users").json()
            target_user_map = {user["email"]: user["_id"] for user in target_users}
            target_groups = self.target_client.get("/api/v1/groups").json()
            target_group_map = {group["name"]: group["_id"] for group in target_groups}

            user_mapping = {source_id: target_user_map.get(email) for source_id, email in source_user_map.items()}
            group_mapping = {source_id: target_group_map.get(name) for source_id, name in source_group_map.items()}
            self.logger.info("User and group mapping created successfully.")
        except Exception as e:
            self.logger.error(f"Failed to fetch users or groups: {e}")
            return share_migration_summary

        # Step 2: Process each dashboard pair
        for source_id, target_id in zip(source_dashboard_ids, target_dashboard_ids):
            self.logger.info(f"Processing shares for dashboard: Source ID {source_id}, Target ID {target_id}")

            # Fetch shares from the source environment
            dashboard_shares_response = self.source_client.get(f"/api/shares/dashboard/{source_id}?adminAccess=true")
            response_text = dashboard_shares_response.text if dashboard_shares_response else 'No response'
            self.logger.debug(
                f"Response for shares of source dashboard ID {source_id}: {response_text}"
            )
            if not dashboard_shares_response or dashboard_shares_response.status_code != 200:
                self.logger.error(f"Failed to fetch shares for source dashboard ID: {source_id}.")
                share_migration_summary['failed_dashboards'].append(
                    {"source_id": source_id, "target_id": target_id}
                )
                continue

            response_json = dashboard_shares_response.json()
            dashboard_shares = response_json.get("sharesTo", [])
            if not dashboard_shares:
                self.logger.warning(f"No shares found for source dashboard ID: {source_id}.")
                continue

            # Identify the potential owner
            owner_field = response_json.get("owner", {})
            source_owner_id = owner_field.get("_id")
            owner_username = owner_field.get("userName", "Unknown User")
            potential_owner_id = user_mapping.get(source_owner_id)
            potential_owner_name = user_mapping.get(owner_username)

            if potential_owner_id:
                self.logger.info(f"Potential owner identified: {owner_username} (ID: {potential_owner_id})")
            else:
                self.logger.warning(f"Potential owner {owner_username} not found in the target environment.")

            # Prepare the shares for migration
            self.logger.info(f"Preparing shares for migration to target dashboard ID {target_id}.")
            new_shares = []
            for share in dashboard_shares:
                if share["type"] == "user":
                    new_share_user_id = user_mapping.get(share["shareId"])
                    user_email = source_user_map.get(share["shareId"], "Unknown User")
                    if new_share_user_id:
                        rule = share.get("rule", "edit")
                        new_shares.append({
                            "shareId": new_share_user_id,
                            "type": "user",
                            "rule": rule,
                            "subscribe": share.get("subscribe", False),
                            "userName": user_email  # Add email for later duplicate check
                        })
                        self.logger.debug(f"Prepared user share for migration: {user_email} (Rule: {rule})")
                elif share["type"] == "group":
                    new_share_group_id = group_mapping.get(share["shareId"])
                    group_name = source_group_map.get(share["shareId"], "Unknown Group")
                    if new_share_group_id:
                        new_shares.append({
                            "shareId": new_share_group_id,
                            "type": "group",
                            "rule": share.get("rule", "viewer"),
                            "subscribe": share.get("subscribe", False),
                            "name": group_name  # Add group name for later duplicate check
                        })
                        self.logger.debug(
                            f"Prepared group share for migration: {group_name} "
                            f"(Rule: {share.get('rule', 'viewer')})"
                        )

            # Combine new shares with existing ones
            self.logger.debug(f"Fetching shares for target dashboard ID {target_id} with adminAccess=true.")
            target_dashboard_shares_url = (
                f"/api/shares/dashboard/{target_id}?adminAccess=true"
            )
            target_dashboard_shares_response = self.target_client.get(target_dashboard_shares_url)

            if target_dashboard_shares_response is not None:
                if target_dashboard_shares_response.status_code == 403:
                    self.logger.warning(
                        f"Access denied for target dashboard ID {target_id} with adminAccess. "
                        f"Retrying without adminAccess."
                    )
                    target_dashboard_shares_response = self.target_client.get(f"/api/shares/dashboard/{target_id}")
                    if target_dashboard_shares_response and target_dashboard_shares_response.status_code == 200:
                        self.logger.debug(
                            f"Successfully fetched shares for target dashboard ID {target_id} "
                            "without adminAccess."
                        )
                    else:
                        self.logger.error(
                            f"Retry without adminAccess also failed for target dashboard ID {target_id}. "
                            "Ending processing for this dashboard."
                        )
                        share_migration_summary['failed_dashboards'].append(
                            {"source_id": source_id, "target_id": target_id}
                        )
                        share_migration_summary['share_fail_count'] += len(new_shares)
                        dashboard_results.append({
                            "source_id": source_id,
                            "target_id": target_id,
                            "shares_added": 0,
                            "status": "Skipped",
                            "reason": "Target dashboard not found or inaccessible"
                        })
                        continue
                elif target_dashboard_shares_response.status_code == 200:
                    self.logger.debug(f"Shares fetched with adminAccess for target dashboard ID {target_id}.")
                else:
                    self.logger.error(
                        f"Unexpected status code when accessing target dashboard ID {target_id}: "
                        f"{target_dashboard_shares_response.status_code}"
                    )
                    share_migration_summary['failed_dashboards'].append(
                        {"source_id": source_id, "target_id": target_id}
                    )
                    share_migration_summary['share_fail_count'] += len(new_shares)
                    continue
            else:
                self.logger.error(
                    f"Failed to fetch shares for target dashboard ID {target_id}. "
                    "Response is None. Ending processing for this dashboard."
                )
                share_migration_summary['failed_dashboards'].append({"source_id": source_id, "target_id": target_id})
                share_migration_summary['share_fail_count'] += len(new_shares)
                continue

            existing_shares = target_dashboard_shares_response.json().get("sharesTo", [])
            # Log simplified existing shares
            simplified_existing = []
            for share in existing_shares:
                if share.get("type") == "user":
                    simplified_existing.append({
                        "type": "user",
                        "userName": share.get("userName", "Unknown")
                    })
                elif share.get("type") == "group":
                    simplified_existing.append({
                        "type": "group",
                        "name": share.get("name", "Unknown Group")
                    })

            self.logger.debug(f"Existing shares for target dashboard ID {target_id}: {simplified_existing}")

            # Build a set of existing share identifiers
            existing_share_keys = set()
            for share in existing_shares:
                if share.get("type") == "user":
                    existing_share_keys.add(f"user:{share.get('userName')}")
                elif share.get("type") == "group":
                    existing_share_keys.add(f"group:{share.get('name')}")

            # Filter out duplicates from new_shares
            filtered_new_shares = []
            for share in new_shares:
                if share.get("type") == "user":
                    key = f"user:{share.get('userName')}"
                elif share.get("type") == "group":
                    key = f"group:{share.get('name')}"
                else:
                    continue
                if key not in existing_share_keys:
                    filtered_new_shares.append(share)

            # Final shares to post
            all_shares = existing_shares + filtered_new_shares

            # Log concise summary of filtered shares
            simplified_filtered = [
                {
                    "type": share.get("type"),
                    "shareId": share.get("shareId"),
                    "rule": share.get("rule"),
                    "subscribe": share.get("subscribe", False)
                }
                for share in filtered_new_shares
            ]
            self.logger.debug(f"Filtered new shares to be added: {simplified_filtered}")

            # Prepare filtered_new_shares for API by removing comparison-only keys
            final_new_shares = []
            for share in filtered_new_shares:
                final_new_shares.append({
                    "shareId": share["shareId"],
                    "type": share["type"],
                    "rule": share["rule"],
                    "subscribe": share.get("subscribe", False)
                })

            # Combine with existing shares
            all_shares = existing_shares + final_new_shares
            self.logger.debug(f"Total shares to be posted: {len(all_shares)}")
            self.logger.debug(f"Final shares payload: {all_shares}")

            if not all_shares:
                self.logger.warning(
                    f"No valid shares found for source dashboard ID {source_id}. "
                    "Ensure users and groups exist in the target environment."
                )
                continue

            # Post the shares to the target environment
            self.logger.info(f"Migrating shares to target dashboard ID {target_id}.")
            post_url = f"/api/shares/dashboard/{target_id}?adminAccess=true"
            self.logger.debug(f"Making POST request to {post_url}.")

            response = self.target_client.post(post_url, data={"sharesTo": all_shares})

            # Check if response is 403 and attempt retry without adminAccess
            if response is not None:
                if response.status_code == 403:
                    self.logger.warning(f"Access denied for POST request to {post_url}. Retrying without adminAccess.")
                    post_url_without_admin = f"/api/shares/dashboard/{target_id}"
                    self.logger.debug(f"Retrying POST request to {post_url_without_admin}.")
                    response = self.target_client.post(post_url_without_admin, data={"sharesTo": all_shares})
                    if response and response.status_code in [200, 201]:
                        self.logger.debug(f"POST request successful without adminAccess for dashboard ID {target_id}.")
                    else:
                        self.logger.error(
                            f"Retry without adminAccess also failed for POST request to dashboard ID {target_id}. "
                            f"Status Code: {response.status_code if response else 'No response'}"
                        )
                elif response.status_code not in [200, 201]:
                    self.logger.error(f"Unexpected status code for POST request to {post_url}: {response.status_code}.")
            else:
                self.logger.error(f"POST request to {post_url} failed. No response received.")

            # Handle the response or fallback logic
            if response and response.status_code in [200, 201]:
                self.logger.info(f"Shares migrated successfully to target dashboard ID {target_id}.")
                share_migration_summary['new_share_success_count'] += len(filtered_new_shares)
            else:
                self.logger.error(
                    f"Failed to migrate shares for target dashboard ID {target_id}. "
                    f"Status Code: {response.status_code if response else 'No response'}"
                )
                share_migration_summary['share_fail_count'] += len(filtered_new_shares)
                share_migration_summary['failed_dashboards'].append({"source_id": source_id, "target_id": target_id})
            dashboard_results.append({
                "source_id": source_id,
                "target_id": target_id,
                "shares_added": len(filtered_new_shares),
                "status": "Success" if response and response.status_code in [200, 201] else "Failed"
            })

            # Step 3: Handle ownership change if requested
            self.logger.debug('Starting ownership change process.')

            # Handle ownership change if required
            if change_ownership and potential_owner_id:
                # Get the existing owner ID from the target dashboard shares response
                target_owner_field = {}
                try:
                    if target_dashboard_shares_response and target_dashboard_shares_response.status_code == 200:
                        target_owner_field = target_dashboard_shares_response.json().get("owner", {})
                except Exception as e:
                    self.logger.warning(f"Failed to extract owner from target dashboard ID {target_id}: {e}")

                current_target_owner_id = target_owner_field.get("_id")

                # Proceed only if the owner is different
                if current_target_owner_id and current_target_owner_id == potential_owner_id:
                    self.logger.info(
                        f"Target dashboard ID {target_id} already owned by user ID {potential_owner_id}. "
                        "Skipping ownership change."
                    )
                else:
                    self.logger.info(
                        f"Changing ownership of target dashboard ID {target_id} to user: "
                        f"{potential_owner_name} (ID: {potential_owner_id})."
                    )

                    ownership_url = f"/api/v1/dashboards/{target_id}/change_owner?adminAccess=true"
                    self.logger.debug(f"Making POST request to {ownership_url} for ownership change.")

                    owner_change_response = self.target_client.post(
                        ownership_url,
                        data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"}
                    )

                    # Check for 403 and retry without adminAccess
                    if owner_change_response is None or owner_change_response.status_code == 403:
                        self.logger.warning(
                            f"Access denied for ownership change at {ownership_url}. "
                            "Retrying without adminAccess."
                        )
                        ownership_url_without_admin = f"/api/v1/dashboards/{target_id}/change_owner"
                        self.logger.debug(f"Retrying ownership change POST request to {ownership_url_without_admin}.")
                        owner_change_response = self.target_client.post(
                            ownership_url_without_admin,
                            data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"}
                        )

                    # Handle the response after retry logic
                    if owner_change_response and owner_change_response.status_code in [200, 201]:
                        self.logger.info(f"Ownership changed successfully for dashboard ID {target_id}.")
                    else:
                        self.logger.error(
                            f"Failed to change ownership for dashboard ID {target_id}. "
                            f"Status Code: "
                            f"{owner_change_response.status_code if owner_change_response else 'No response'}."
                        )

        self.logger.info("Finished share migration.")
        self.logger.info(share_migration_summary)
        return {
            "summary": {
                "total_dashboard_count": len(source_dashboard_ids),
                "total_share_success_count": share_migration_summary['new_share_success_count'],
                "total_share_fail_count": share_migration_summary['share_fail_count']
            },
            "dashboard_results": dashboard_results
        }

    def migrate_dashboards(self, dashboard_ids=None, dashboard_names=None, action=None, republish=False,
                           migrate_share=False, change_ownership=False):
        """
        Migrates specific dashboards from the source to the target environment using the bulk endpoint.

        Parameters:
            dashboard_ids (list, optional): A list of dashboard IDs to migrate.
                Either `dashboard_ids` or `dashboard_names` must be provided.
            dashboard_names (list, optional): A list of dashboard names to migrate.
                Either `dashboard_ids` or `dashboard_names` must be provided.
            action (str, optional): Determines how to handle existing dashboards in the target environment.
                                    Options:
                                    - 'skip': Skip existing dashboards in the target; new dashboards are processed
                                      normally, including shares and ownership.
                                    - 'overwrite': Overwrite existing dashboards; shares and ownership will not be
                                      migrated. If the dashboard already exists, shares will be retained, but the API
                                      user will be set as the new owner.
                                    - 'duplicate': Create a duplicate of existing dashboards without migrating shares or
                                      ownership.
                                    Default: None. Existing dashboards are skipped, and only new ones are migrated.
                                    **Note:** If an existing dashboard in the target environment has a different owner
                                    than the user's token running the SDK, the dashboard will be migrated with a new ID,
                                    and its shares and ownership will be migrated from the original source dashboard.
            republish (bool, optional): Whether to republish dashboards after migration. Default: False.
            migrate_share (bool, optional): Whether to migrate shares for the dashboards. If `True`, shares will be
                migrated, and ownership migration will be controlled by the `change_ownership` parameter.
                If `False`, both shares and ownership migration will be skipped. Default: False.
            change_ownership (bool, optional): Whether to change ownership of the target dashboards.
                Effective only if `migrate_share` is True. Default: False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed dashboards.

        Notes:
            - **When `action` is not provided, existing dashboards in the target environment are skipped,
              and only new dashboards are added.
            - **Best Use Case**: Suitable when migrating dashboards for the first time to a target environment.
            - **Overwrite Action:** When using `overwrite`, shares and ownership will not be migrated.
              If a dashboard already exists, the target dashboard will be overwritten,
              retaining its existing shares but setting the API user as the new owner.
              Subsequent adjustments to shares and ownership will not be supported in this mode.
            - **Duplicate Action**: Creates duplicate dashboards without shares and ownership migration.
            - **Skip Action**: Skips migration for existing dashboards, but new ones are processed normally.
        """

        if dashboard_ids and dashboard_names:
            raise ValueError("Please provide either 'dashboard_ids' or 'dashboard_names', not both.")

        if not migrate_share and change_ownership:
            raise ValueError("The `change_ownership` parameter requires `migrate_share=True`.")

        self.logger.info("Starting dashboard migration from source to target.")

        # Step 1: Fetch dashboards based on provided IDs or names
        migration_summary = {
            "succeeded": [],
            "skipped": [],
            "failed": []
        }
        bulk_dashboard_data = []
        if dashboard_ids:
            self.logger.info(f"Processing dashboard migration by IDs: {dashboard_ids}")
            for dashboard_id in dashboard_ids:
                source_dashboard_response = self.source_client.get(
                    f"/api/dashboards/{dashboard_id}/export?adminAccess=true"
                )
                self.logger.debug(
                    f"Response for source dashboard ID {dashboard_id}: "
                    f"{source_dashboard_response.text if source_dashboard_response else 'No response'}"
                )
                if source_dashboard_response and source_dashboard_response.status_code == 200:
                    self.logger.debug(f"Dashboard with ID: {dashboard_id} retrieved successfully.")
                    bulk_dashboard_data.append(source_dashboard_response.json())
                else:
                    self.logger.error(
                        f"Failed to export dashboard with ID: {dashboard_id}. Status Code: "
                        f"{source_dashboard_response.status_code if source_dashboard_response else 'No response'}"
                    )
                    migration_summary["failed"].append({
                        "id": dashboard_id,
                        "reason": (
                            f"Export failed with status code {source_dashboard_response.status_code}"
                            if source_dashboard_response else "No response from server"
                        )
                    })
        elif dashboard_names:
            self.logger.info(f"Processing dashboard migration by names: {dashboard_names}")
            limit = 50
            skip = 0
            dashboards = []
            # Fetch dashboards from the source environment
            while True:
                self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
                dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                    "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                    "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
                })

                if not dashboard_response or dashboard_response.status_code != 200:
                    self.logger.debug("No more dashboards found or failed to retrieve.")
                    break

                items = dashboard_response.json().get("items", [])
                if not items:
                    self.logger.debug("No more items in response; breaking pagination loop.")
                    break

                self.logger.debug(f"Fetched {len(items)} dashboards in this batch.")
                dashboards.extend(items)
                skip += limit

            # Filter dashboards by name and avoid duplicates
            unique_dashboards = {dash["oid"]: dash for dash in dashboards}
            dashboards = list(unique_dashboards.values())
            self.logger.info(f"Total unique dashboards retrieved: {len(dashboards)}.")
            bulk_dashboard_data = []
            for dashboard in dashboards:
                if dashboard["title"] in dashboard_names:
                    self.logger.debug(f"Matching dashboard: {dashboard['title']}")
                    source_dashboard_response = self.source_client.get(
                        f"/api/dashboards/{dashboard['oid']}/export?adminAccess=true"
                    )
                    if source_dashboard_response and source_dashboard_response.status_code == 200:
                        bulk_dashboard_data.append(source_dashboard_response.json())
                        self.logger.debug(f"Dashboard {dashboard['title']} added to migration list.")
                    else:
                        self.logger.error(f"Failed to export dashboard: {dashboard['title']} (ID: {dashboard['oid']}).")
                        migration_summary["failed"].append({
                            "id": dashboard["oid"],
                            "title": dashboard["title"],
                            "reason": (
                                f"Export failed with status code {source_dashboard_response.status_code}"
                                if source_dashboard_response else "No response from server"
                            )
                        })
                else:
                    self.logger.debug(f"Dashboard {dashboard['title']} not in the provided names; skipping.")

        # Step 2: Perform bulk migration
        source_dash_dict = {
            dash['oid']: dash['title']
            for dash in bulk_dashboard_data
        }  # Create a map of source OIDs to titles
        migrated_target_dash_dict = {}  # Placeholder for target OIDs and titles after migration
        if bulk_dashboard_data:
            url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
            if action:
                url += f"&action={action}"

            self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
            response = self.target_client.post(url, data=bulk_dashboard_data)
            self.logger.debug(f"Response for bulk migration: {response.text if response else 'No response'}")

            # Handle the migration results
            if response and response.status_code == 201:
                response_data = response.json()

                # Process succeeded dashboards
                if "succeded" in response_data:
                    for response_dash in response_data['succeded']:
                        target_oid = response_dash['oid']
                        title = response_dash['title']

                        # Populate the target map dictionary
                        migrated_target_dash_dict[target_oid] = title
                        migration_summary['succeeded'].append(title)

                        self.logger.debug(
                            f"Captured Target OID '{target_oid}' with title '{title}' "
                            "in migrated_target_map_dict."
                        )

                # Process skipped dashboards
                if "skipped" in response_data:
                    migration_summary['skipped'] = [dash['title'] for dash in response_data['skipped']]
                    for dash_title in migration_summary['skipped']:
                        self.logger.info(f"Skipped dashboard: {dash_title}")

                # Process failed dashboards
                if "failed" in response_data:
                    failed_items = response_data['failed']
                    for category, errors in failed_items.items():
                        for error in errors:
                            migration_summary['failed'].append(error['title'])
                            self.logger.warning(
                                f"Failed to migrate dashboard: {error['title']} - "
                                f"{error['error']['message']}"
                            )
            else:
                self.logger.error(
                    f"Bulk migration failed. Status Code: "
                    f"{response.status_code if response else 'No response'}"
                )
                migration_summary['failed'].extend([dash['title'] for dash in bulk_dashboard_data])

        self.logger.info("Dashboard migration completed.")
        self.logger.debug(f"Source Map Dictionary: {source_dash_dict}")
        self.logger.debug(f"Migrated Target Map Dictionary: {migrated_target_dash_dict}")

        # Step 3: Handle shares and ownership migration
        if not migrate_share:
            self.logger.info("Migrate Share is set to False. Skipping shares and ownership migration.")
        elif action in ["duplicate", "overwrite"]:
            self.logger.info(f"Action '{action}' selected. Skipping shares and ownership migration.")
        else:
            self.logger.info("Starting share and ownership migration.")

            # Compare source and target OIDs to identify dashboards to process
            dash_to_process = {}
            problem_dash = []

            for source_oid, source_title in source_dash_dict.items():
                # Match title in the migrated target dictionary
                matching_target = next(
                    (
                        target_oid
                        for target_oid, target_title in migrated_target_dash_dict.items()
                        if target_title == source_title
                    ),
                    None
                )
                if matching_target:
                    if source_oid != matching_target:
                        # Log mismatched OIDs with matching titles
                        problem_dash.append({
                            "title": source_title,
                            "source_id": source_oid,
                            "target_id": matching_target
                        })
                        self.logger.warning(
                            f"Title '{source_title}' has mismatched OIDs: "
                            f"Source ID '{source_oid}' and Target ID '{matching_target}'."
                        )
                    # Add to dashboards to process
                    dash_to_process[source_oid] = matching_target
                else:
                    # Log missing target for a source dashboard
                    self.logger.warning(
                        (
                            f"Source dashboard '{source_title}' with ID '{source_oid}' "
                            "was not found in the target environment."
                        )
                    )

            self.logger.info(f"Dashboards to process: {dash_to_process}")
            self.logger.info(f"Problematic dashboards: {problem_dash}")

            if not dash_to_process:
                self.logger.info(
                    "No successfully migrated dashboards were captured. Skipping shares and ownership migration. "
                    (
                        "This may be due to errors during migration or because the dashboards already existed "
                        "and were skipped. Review the logs for more details."
                    )
                )
            else:
                self.logger.info(f"Processing shares and ownership for dashboards: {dash_to_process}")
                self.migrate_dashboard_shares(
                    source_dashboard_ids=list(dash_to_process.keys()),      # Original source OIDs
                    target_dashboard_ids=list(dash_to_process.values()),    # Corresponding target OIDs
                    change_ownership=change_ownership
                )
                self.logger.info("Share and ownership migration completed.")

        self.logger.info("Finished dashboard migration.")
        self.logger.info(f"Total Dashboards Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Dashboards Skipped: {len(migration_summary['skipped'])}")
        self.logger.info(f"Total Dashboards Failed: {len(migration_summary['failed'])}")
        self.logger.info(migration_summary)

        return migration_summary

    def migrate_all_dashboards(self, action=None, republish=False, migrate_share=False, change_ownership=False,
                               batch_size=10, sleep_time=10):
        """
        Migrates all dashboards from the source to the target environment in batches.

        Parameters:
            action (str, optional): Determines how to handle existing dashboards in the target environment.
                                    Options:
                                    - 'skip': Skip existing dashboards in the target; new dashboards are processed
                                      normally, including shares and ownership.
                                    - 'overwrite': Overwrite existing dashboards; shares and ownership will not be
                                      migrated. If the dashboard already exists, shares will be retained, but the API
                                      user will be set as the new owner.
                                    - 'duplicate': Create a duplicate of existing dashboards without migrating shares
                                      or ownership.
                                    Default: None. Existing dashboards are skipped, and only new ones are migrated.
                                    Unless existing dashboards are different owners, shares will be migrated.
                                    **Note:** If an existing dashboard in the target environment has a different owner
                                     than the user's token running the SDK, the dashboard will be migrated with a new
                                     ID, and its shares and ownership will be migrated from the original source
                                     dashboard.
            republish (bool, optional): Whether to republish dashboards after migration. Default: False.
            migrate_share (bool, optional): Whether to migrate shares for the dashboards. If `True`, shares will be
                migrated, and ownership migration will be controlled by the `change_ownership` parameter.
                If `False`, both shares and ownership migration will be skipped. Default: False.
            change_ownership (bool, optional): Whether to change ownership of the target dashboards.
                Effective only if `migrate_share` is True. Default: False.
            batch_size (int, optional): Number of dashboards to process in each batch. Default: 10.
            sleep_time (int, optional): Time (in seconds) to sleep between batches. Default: 10 seconds.

        Returns:
            dict: A summary of the migration results for all batches, containing lists of succeeded, skipped,
                and failed dashboards.

        Notes:
            - **Batch Processing**: Dashboards are processed in batches to avoid overloading the system.
            - **Best Use Case**: This method is suitable when migrating all dashboards from a source to a
              target environment.
            - **Overwrite Action**: When using `overwrite`, shares and ownership will not be migrated.
              If a dashboard already exists, the target dashboard will be overwritten, retaining its existing shares
              but setting the API user as the new owner. Subsequent adjustments to shares and ownership will not be
              supported in this mode.
            - **Duplicate Action**: Creates duplicate dashboards without shares and ownership migration.
            - **Skip Action**: Skips migration for existing dashboards, but new ones are processed normally.
        """

        self.logger.info("Fetching all dashboards from the source environment.")
        all_dashboard_ids = set()

        # Step 1: Fetch all dashboards
        limit = 50
        skip = 0
        while True:
            dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
            })

            if not dashboard_response or dashboard_response.status_code != 200:
                self.logger.debug("No more dashboards found or failed to retrieve.")
                break

            items = dashboard_response.json().get("items", [])
            if not items:
                self.logger.debug("No more items in response; breaking pagination loop.")
                break

            self.logger.debug(f"Fetched {len(items)} dashboards in this batch.")
            all_dashboard_ids.update([dash["oid"] for dash in items])
            skip += limit

        self.logger.info(f"Total unique dashboards retrieved: {len(all_dashboard_ids)}.")

        # Step 2: Migrate dashboards in batches
        all_dashboard_ids = list(all_dashboard_ids)
        migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}

        for i in range(0, len(all_dashboard_ids), batch_size):
            batch_ids = all_dashboard_ids[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            self.logger.info(f"Processing batch {batch_number} with {len(batch_ids)} dashboards: {batch_ids}")

            try:
                batch_summary = self.migrate_dashboards(
                    dashboard_ids=batch_ids,
                    action=action,
                    republish=republish,
                    migrate_share=migrate_share,
                    change_ownership=change_ownership
                )
                self.logger.info(f"Batch {batch_number} migration summary: {batch_summary}")

                # Aggregate batch results into the overall summary
                migration_summary['succeeded'].extend(batch_summary['succeeded'])
                migration_summary['skipped'].extend(batch_summary['skipped'])
                migration_summary['failed'].extend(batch_summary['failed'])
            except Exception as e:
                self.logger.error(f"Error occurred in batch {batch_number}: {e}")
                continue  # Continue with the next batch even if an error occurs

            if i + batch_size < len(all_dashboard_ids):  # Avoid sleeping after the last batch
                self.logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch.")
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all dashboards.")
        self.logger.info(f"Total Dashboards Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Dashboards Skipped: {len(migration_summary['skipped'])}")
        self.logger.info(f"Total Dashboards Failed: {len(migration_summary['failed'])}")
        self.logger.info(migration_summary)
        return migration_summary

    def migrate_datamodels(self, datamodel_ids=None, datamodel_names=None, provider_connection_map=None,
                           dependencies=None, shares=False, action=None, new_title=None):
        """
        Migrates specific data models from the source environment to the target environment.

        Parameters:
            datamodel_ids (list, optional): A list of data model IDs to migrate.
                Either `datamodel_ids` or `datamodel_names` must be provided.
            datamodel_names (list, optional): A list of data model names to migrate.
                Either `datamodel_ids` or `datamodel_names` must be provided.
            provider_connection_map (dict, optional): A dictionary mapping provider names to connection IDs.
                This allows specifying different connections per provider.
                For example:
                {
                    "Databricks": "Connection ID",
                    "GoogleBigQuery": "Connection ID"
                }
            dependencies (list, optional): A list of dependencies to include in the migration.
                If not provided or if 'all' is passed, all dependencies are selected by default.
                                        Possible values for `dependencies` are:
                                        - "dataSecurity" (includes both Data Security and Scope Configuration)
                                        - "formulas" (for Formulas)
                                        - "hierarchies" (for Drill Hierarchies)
                                        - "perspectives" (for Perspectives)
                                        If left blank or set to "all", all dependencies are included by default.
            shares (bool, optional): Whether to also migrate the data model's shares. Default is False.
            action (str, optional): Strategy to handle existing data models in the target environment.
                - "overwrite": Attempts to overwrite existing model using its original ID via the datamodelId parameter.
                  If the model is not found in target environment, it will automatically fall back and create the model.
                - "duplicate": Creates a new model by passing a `new_title` to the `newTitle` parameter of the
                  import API endpoint. If `new_title` is not provided, the original title will be used with
                  " (Duplicate)" appended.
            new_title (str, optional): New name for the duplicated data model. Used only when `action='duplicate'`.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed data models.
        """
        # Mapping user-friendly terms to API parameters
        dependency_mapping = {
            "dataSecurity": ["dataContext", "scopeConfiguration"],
            "formulas": ["formulaManagement"],
            "hierarchies": ["drillHierarchies"],
            "perspectives": ["perspectives"]
        }

        # Set default dependencies if none are provided
        if dependencies is None or dependencies == "all":
            dependencies = list(dependency_mapping.keys())

        api_dependencies = list({dep for key in dependencies for dep in dependency_mapping.get(key, [])})

        # Validate input parameters
        if datamodel_ids and datamodel_names:
            raise ValueError("Please provide either 'datamodel_ids' or 'datamodel_names', not both.")
        if not datamodel_ids and not datamodel_names:
            raise ValueError("You must provide either 'datamodel_ids' or 'datamodel_names'.")

        self.logger.info("Starting data model migration from source to target.")
        self.logger.debug(
            f"Input Parameters: datamodel_ids={datamodel_ids}, "
            f"datamodel_names={datamodel_names}, "
            f"dependencies={dependencies}, shares={shares}"
        )

        # Initialize migration summary
        migration_summary = {
            'succeeded': [],
            'failed': [],
            'share_success_count': 0,
            'share_fail_count': 0,
            'share_details': {},
            'failure_reasons': {}
        }
        success_count = 0
        fail_count = 0

        # Fetch data models based on provided parameters (IDs or names)
        all_datamodel_data = []
        if datamodel_ids:
            self.logger.debug(f"Processing data model migration by IDs: {datamodel_ids}")
            for datamodel_id in datamodel_ids:
                response = self.source_client.get("/api/v2/datamodel-exports/schema", params={
                    "datamodelId": datamodel_id,
                    "type": "schema-latest",
                    "dependenciesIdsToInclude": ",".join(api_dependencies),
                })
                if response.status_code == 200:
                    data_model_json = response.json()
                    self.logger.info(
                        f"Successfully fetched data model name "
                        f"{data_model_json.get('title', 'Unknown Title')}."
                    )
                    self.logger.debug(f"Successfully fetched data model ID {datamodel_id}: {response.json()}")
                    all_datamodel_data.append(response.json())
                else:
                    self.logger.error(f"Failed to fetch data model ID {datamodel_id}. Response: {response.text}")

        elif datamodel_names:
            self.logger.debug("Fetching all data models to filter by names.")
            response = self.source_client.get("/api/v2/datamodels/schema", params={"fields": "oid,title"})
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch data models. Response: {response.text}")
                return migration_summary
            self.logger.info(f"Retrieved {len(response.json())} data models from the source environment.")

            source_datamodels = response.json()
            self.logger.debug(f"Source data models fetched: {source_datamodels}")

            # Filter the data models to migrate
            for datamodel in source_datamodels:
                if datamodel["title"] in datamodel_names:
                    response = self.source_client.get("/api/v2/datamodel-exports/schema", params={
                        "datamodelId": datamodel["oid"],
                        "type": "schema-latest",
                        "dependenciesIdsToInclude": ",".join(api_dependencies),
                    })
                    if response.status_code == 200:
                        self.logger.debug(
                            f"Successfully fetched data model '{datamodel['title']}' "
                            f"with ID {datamodel['oid']}."
                        )
                        all_datamodel_data.append(response.json())
                    else:
                        self.logger.error(
                            f"Failed to fetch data model '{datamodel['title']}' "
                            f"(ID: {datamodel['oid']}). Response: {response.text}"
                        )

        # Migrate each data model one by one
        if all_datamodel_data:
            self.logger.info(f"Migrating '{len(all_datamodel_data)}' datamodels one by one to the target environment.")
            successfully_migrated_datamodels = []
            migration_summary['failure_reasons'] = {}

            for data_model in all_datamodel_data:
                for dataset in data_model.get("datasets", []):
                    connection = dataset.get("connection")

                    if connection and isinstance(connection, dict):
                        provider = connection.get("provider")

                        if provider_connection_map and provider in provider_connection_map:
                            dataset["connection"] = {
                                "oid": provider_connection_map[provider],
                                "provider": provider
                            }
                        else:
                            # fallback to cleaning parameters if no override
                            if "parameters" in connection:
                                connection["parameters"] = ""

                self.logger.debug(f"Data model after processing connections: {data_model}")
                datasets_log = data_model.get("datasets", [])
                if datasets_log:
                    self.logger.debug(f"Connection object: {datasets_log[0].get('connection', {})}")
                else:
                    self.logger.warning(f"No datasets found in data model: {data_model.get('title', 'Unknown Title')}")

                # Prepare request URL based on action (overwrite or duplicate)
                import_url = "/api/v2/datamodel-imports/schema"
                query_string = ""
                if action == "overwrite":
                    query_string = f"?datamodelId={data_model.get('oid')}"
                elif action == "duplicate":
                    new_model_title = new_title or f"{data_model.get('title', 'Untitled')} (Duplicate)"
                    query_string = f"?newTitle={new_model_title}"

                try:
                    response = self.target_client.post(f"{import_url}{query_string}", data=data_model)
                    if response.status_code == 201:
                        self.logger.info(f"Successfully migrated data model: {data_model['title']}")
                        migration_summary['succeeded'].append(data_model['title'])
                        successfully_migrated_datamodels.append(data_model)
                        success_count += 1
                    elif response.status_code == 404 and action == "overwrite":
                        fallback_reason = (
                            f"Data model '{data_model['title']}' not found in target for overwrite. "
                            f"Retrying without overwrite option."
                        )
                        self.logger.warning(fallback_reason)

                        # Retry without query param
                        fallback_response = self.target_client.post(import_url, data=data_model)
                        if fallback_response.status_code == 201:
                            self.logger.info(
                                f"Successfully migrated datamodel without overwrite: {data_model['title']}"
                            )
                            migration_summary['succeeded'].append(data_model['title'])
                            successfully_migrated_datamodels.append(data_model)
                            success_count += 1
                        elif (
                            fallback_response.status_code == 400
                            and fallback_response.json().get("title") == "ElasticubeAlreadyExists"
                        ):
                            final_reason = (
                                f"Datamodel '{data_model['title']}' already exists on the target with a different ID. "
                                (
                                    "Consider using action='duplicate' with a new title, "
                                    "or delete the existing model manually."
                                )
                            )
                            self.logger.error(final_reason)
                            migration_summary['failed'].append(data_model['title'])
                            migration_summary['failure_reasons'][data_model['title']] = final_reason
                            fail_count += 1
                        else:
                            error_message = fallback_response.json().get("detail", "Unknown error")
                            self.logger.error(
                                f"Fallback failed to migrate data model: {data_model['title']}. "
                                f"Error: {error_message}"
                            )
                            migration_summary['failed'].append(data_model['title'])
                            migration_summary['failure_reasons'][data_model['title']] = error_message
                            fail_count += 1
                    else:
                        error_message = response.json().get("detail", "Unknown error")
                        self.logger.error(
                            f"Failed to migrate data model: {data_model['title']}. "
                            f"Error: {error_message}"
                        )
                        migration_summary['failed'].append(data_model['title'])
                        migration_summary['failure_reasons'][data_model['title']] = error_message
                        fail_count += 1
                except Exception as e:
                    reason = f"Exception occurred: {str(e)}"
                    self.logger.error(f"Exception while migrating data model '{data_model['title']}': {reason}")
                    migration_summary['failed'].append(data_model['title'])
                    migration_summary['failure_reasons'][data_model['title']] = reason
                    fail_count += 1
        else:
            self.logger.warning("No data models were successfully retrieved for migration.")
            return migration_summary

        # Final logging for data model migration success and failure counts
        self.logger.info(f"Data model migration completed. Success: {success_count}, Failed: {fail_count}")

        # Handle shares if the flag is set
        if shares:
            self.logger.info("Processing shares for the migrated datamodels.")

            # Fetch source and target users/groups
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids.status_code == 200:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}

            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids.status_code == 200:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}

            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids.status_code == 200:
                source_group_ids = {
                    group["name"]: group["_id"]
                    for group in source_group_ids.json()
                    if group["name"] not in ["Everyone", "All users in system"]
                }
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}

            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids.status_code == 200:
                target_group_ids = {
                    group["name"]: group["_id"]
                    for group in target_group_ids.json()
                    if group["name"] not in ["Everyone", "All users in system"]
                }
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}

            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}

            # Proceed with share logic for successfully migrated datamodels
            share_fail_count = 0
            if successfully_migrated_datamodels:
                for datamodel in successfully_migrated_datamodels:
                    datamodel_id = datamodel['oid']
                    if datamodel["type"] == "extract":
                        datamodel_shares_response = self.source_client.get(
                            f"/api/elasticubes/localhost/{datamodel['title']}/permissions"
                        )
                        datamodel_shares = (
                            datamodel_shares_response.json().get("shares", [])
                            if datamodel_shares_response.status_code == 200
                            else []
                        )
                    elif datamodel["type"] == "live":
                        datamodel_shares_response = self.source_client.get(
                            f"/api/v1/elasticubes/live/{datamodel_id}/permissions"
                        )
                        datamodel_shares = (
                            datamodel_shares_response.json()
                            if datamodel_shares_response.status_code == 200
                            else []
                        )
                    else:
                        self.logger.warning(f"Unknown datamodel type for: {datamodel['title']}")
                        continue
                    # Handle failed response
                    if datamodel_shares_response.status_code != 200:
                        self.logger.error(
                            f"Failed to fetch shares for datamodel: '{datamodel['title']}' "
                            f"(ID: {datamodel['oid']}). "
                            f"Error: {datamodel_shares_response.json()}"
                        )
                        share_fail_count += 1
                        migration_summary['failed'].append(datamodel['title'])
                        continue

                    # Process the shares if they exist
                    if datamodel_shares:
                        new_shares = []
                        for share in datamodel_shares:
                            if share["type"] == "user":
                                new_share_user_id = user_mapping.get(share["partyId"], None)
                                if new_share_user_id:
                                    new_shares.append({
                                        "partyId": new_share_user_id,
                                        "type": "user",
                                        "permission": share.get("permission", "a"),
                                    })
                            elif share["type"] == "group":
                                new_share_group_id = group_mapping.get(share["partyId"], None)
                                if new_share_group_id:
                                    new_shares.append({
                                        "partyId": new_share_group_id,
                                        "type": "group",
                                        "permission": share.get("permission", "a"),
                                    })
                        # Post the new shares to the target datamodel
                        share_count = len(new_shares)
                        if share_count > 0:
                            if datamodel["type"] == "extract":
                                response = self.target_client.put(
                                    f"/api/elasticubes/localhost/{datamodel['title']}/permissions",
                                    data=new_shares
                                )
                            elif datamodel["type"] == "live":
                                self.logger.info(f"Publishing datamodel '{datamodel['title']}' to update shares.")
                                publish_response = self.target_client.post(
                                    "/api/v2/builds",
                                    data={"datamodelId": datamodel_id, "buildType": "publish"}
                                )
                                if publish_response.status_code == 201:
                                    self.logger.info(
                                        f"Datamodel '{datamodel['title']}' published successfully. "
                                        "Now updating shares."
                                    )
                                    response = self.target_client.patch(
                                        f"/api/v1/elasticubes/live/{datamodel_id}/permissions",
                                        data=new_shares
                                    )
                                else:
                                    self.logger.error(
                                        f"Failed to publish datamodel '{datamodel['title']}'. "
                                        f"Error: {publish_response.json() if publish_response else 'No response'}"
                                    )
                                    response = None

                            if response and response.status_code in [200, 201]:
                                self.logger.info(f"Datamodel '{datamodel['title']}' shares migrated successfully.")
                                migration_summary['share_success_count'] += share_count
                                migration_summary['share_details'][datamodel['title']] = share_count
                            else:
                                self.logger.error(
                                    f"Failed to migrate shares for datamodel: {datamodel['title']}. "
                                    f"Error: {response.json() if response else 'No response received.'}"
                                )
                                migration_summary['share_fail_count'] += 1
                        else:
                            self.logger.warning(f"No valid shares found for datamodel: {datamodel['title']}.")

        # Final log for the entire migration process
        self.logger.info("Finished data model migration.")
        self.logger.info(migration_summary)

        return {
            "summary": {
                "total_requested": len(datamodel_ids or datamodel_names or []),
                "total_succeeded": len(migration_summary["succeeded"]),
                "total_failed": len(migration_summary["failed"]),
                "shares_migrated": migration_summary.get("share_success_count", 0),
                "shares_failed": migration_summary.get("share_fail_count", 0)
            },
            "details": migration_summary
        }

    def migrate_all_datamodels(self, dependencies=None, shares=False, batch_size=10, sleep_time=5, action=None):
        """
        Migrates all data models from the source environment to the target environment in batches.

        Parameters:
            dependencies (list, optional): A list of dependencies to include in the migration.
                If not provided or if 'all' is passed, all dependencies are selected by default.
                Possible values for `dependencies` are:
                - "dataSecurity" (includes both Data Security and Scope Configuration)
                - "formulas" (for Formulas)
                - "hierarchies" (for Drill Hierarchies)
                - "perspectives" (for Perspectives)
                If left blank or set to "all", all dependencies are included by default.
            shares (bool, optional): Whether to also migrate the data model's shares. Default is False.
            batch_size (int, optional): Number of data models to migrate in each batch. Default is 10.
            sleep_time (int, optional): Time in seconds to wait between processing batches. Default is 5 seconds.
            action (str, optional): Strategy to handle existing data models in the target environment.
                - "overwrite": Attempts to overwrite an existing model using its original ID via the
                  datamodelId parameter. If the model is not found in the target environment, it will
                  automatically fall back and create the model.
                - "duplicate": Creates a new model by appending " (Duplicate)" to the original name.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed data models.
        """
        self.logger.info("Starting migration of all data models from source to target.")
        self.logger.debug(
            f"Input Parameters: dependencies={dependencies}, shares={shares}, "
            f"batch_size={batch_size}, sleep_time={sleep_time}"
        )

        # Fetch all data models
        response = self.source_client.get("/api/v2/datamodels/schema", params={"fields": "oid,title"})
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch data models. Response: {response.text}")
            return {"succeeded": [], "skipped": [], "failed": []}

        source_datamodels = response.json()
        all_datamodel_ids = [datamodel["oid"] for datamodel in source_datamodels]
        self.logger.info(f"Retrieved {len(all_datamodel_ids)} data models from the source environment.")

        migration_summary = {"succeeded": [], "skipped": [], "failed": [], "failure_reasons": {}}

        for i in range(0, len(all_datamodel_ids), batch_size):
            batch_ids = all_datamodel_ids[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            self.logger.info(f"Processing batch {batch_number} with {len(batch_ids)} data models: {batch_ids}")

            try:
                batch_result = self.migrate_datamodels(
                    datamodel_ids=batch_ids,
                    dependencies=dependencies,
                    shares=shares,
                    action=action
                )
                self.logger.info(f"Batch {batch_number} migration summary: {batch_result}")

                batch_details = batch_result.get("details", {})
                migration_summary['succeeded'].extend(batch_details.get("succeeded", []))
                migration_summary['failed'].extend(batch_details.get("failed", []))
                if "failure_reasons" in batch_details:
                    migration_summary["failure_reasons"].update(batch_details["failure_reasons"])
            except Exception as e:
                self.logger.error(f"Error occurred in batch {batch_number}: {e}")
                continue  # Continue with the next batch even if an error occurs

            if i + batch_size < len(all_datamodel_ids):  # Avoid sleeping after the last batch
                self.logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch.")
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all data models.")
        self.logger.info(f"Total Data Models Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Data Models Failed: {len(migration_summary['failed'])}")
        self.logger.info(migration_summary)

        return {
            "summary": {
                "total_batches": (len(all_datamodel_ids) + batch_size - 1) // batch_size,
                "total_requested": len(all_datamodel_ids),
                "total_succeeded": len(migration_summary["succeeded"]),
                "total_failed": len(migration_summary["failed"])
            },
            "details": migration_summary
        }
