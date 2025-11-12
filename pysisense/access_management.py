from .sisenseclient import SisenseClient


class AccessManagement:

    def __init__(self, api_client=None, debug=False):
        """
        Initializes the AccessManagement class.
        If no Sisense client is provided, creates a SisenseClient internally.

        Parameters:
            api_client (SisenseClient, optional):
            Existing SisenseClient or None.
            debug (bool, optional): Enables debug logging if True.
        """
        # Use provided API client or create a new one
        if api_client is not None:
            self.api_client = api_client
        else:
            self.api_client = SisenseClient(debug=debug)

        # Use the logger from the APIClient instance
        self.logger = self.api_client.logger
        self.logger.debug("AccessManagement class initialized.")

    def get_user(self, user_name):
        """
        Retrieves user details by their email (username) and expands the
        response to include group and role information.

        Parameters:
            user_name (str): The email or username of the user to be retrieved.

        Returns:
            dict: User details on success, or {'error': 'message'} on failure.
        """
        self.logger.debug(
            f"Getting user with username: {user_name}"
        )

        # Parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Fetch all users from the API with the specified query parameters
        response = self.api_client.get("/api/v1/users", params=params)

        # Check if the API request failed
        if not response or not response.ok:
            error_msg = (
                f"Failed to retrieve users from API for username: {user_name}."
            )
            self.logger.error(
                f"{error_msg} Status Code: "
                f"{response.status_code if response else 'No response'}"
            )
            return {"error": error_msg}

        # Parse the response JSON
        try:
            users = response.json()
            self.logger.debug(
                f"Found {len(users)} users in the response."
            )
        except Exception as e:
            self.logger.exception(
                "Error decoding JSON response for user list."
            )
            return {
                "error": f"Failed to decode API response: {str(e)}"
            }

        # Mapping role names
        ROLE_MAPPING = {
            'consumer': 'viewer',
            'super': 'sysAdmin',
            'contributor': 'dashboardDesigner'
        }

        # Iterate over each user in the response to find the one matching the
        # given username
        for user in users:
            try:
                self.logger.debug(
                    f"Checking user: {user.get('email')}"
                )
                if user.get("email") == user_name:
                    self.logger.info(f"Found user: {user_name}")
                    return {
                        'USER_ID': user["_id"],
                        'USER_NAME': user["userName"],
                        'FIRST_NAME': user["firstName"],
                        'LAST_NAME': user.get('lastName', ''),
                        'EMAIL': user["email"],
                        'IS_ACTIVE': user["active"],
                        'ROLE_ID': user["role"]["_id"],
                        'ROLE_NAME': ROLE_MAPPING.get(
                            user["role"]["name"], user["role"]["name"]
                        ),
                        'GROUPS': [g["name"] for g in user.get("groups", [])]
                    }
            except Exception as e:
                self.logger.exception(
                    (
                        f"Error processing user object: {user} | "
                        f"Exception: {str(e)}"
                    )
                )

        self.logger.warning(
            f"User with username '{user_name}' not found.")
        return {"error": f"User '{user_name}' not found."}

    def get_users_all(self):
        """
        Retrieves user details along with tenant, group, and role information.
        Removes "Everyone" group from users if they belong to other groups, but
        keeps the "Everyone" group if it's the only group the user belongs to.

        Returns:
            list: List of user details dicts, or [{'error': ...}] if retrieval
            fails.
        """
        self.logger.debug("Getting all users")

        # Query parameters to expand the response with group and role info
        params = {'expand': 'groups,role'}

        # Fetch user data from the API with the specified query parameters
        response = self.api_client.get("/api/v1/users", params=params)

        # Check if the API request failed
        if not response or not response.ok:
            self.logger.error("Failed to retrieve users from API")
            return [{"error": "Failed to retrieve users from API"}]

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse user response JSON.")
            return [{"error": f"Failed to parse user response: {str(e)}"}]

        # Initialize list to store user information
        data_list = []

        # Mapping role names
        ROLE_MAPPING = {
            'consumer': 'viewer',
            'super': 'sysAdmin',
            'contributor': 'dashboardDesigner'
        }

        # Process the API response to build data_list
        for user in response_data:
            try:
                self.logger.debug(
                    f"Processing user: {user['email']}"
                )
                base_data = {
                    'USER_ID': user["_id"],
                    'USER_NAME': user["userName"],
                    'FIRST_NAME': user["firstName"],
                    'LAST_NAME': user.get('lastName', ''),
                    'EMAIL': user["email"],
                    'IS_ACTIVE': user["active"],
                    'ROLE_ID': user["role"]["_id"],
                    'ROLE_NAME': ROLE_MAPPING.get(
                        user["role"]["name"], user["role"]["name"]
                    ),
                    'GROUPS': []
                }

                # Add all group names to the 'GROUPS' list
                if 'groups' in user and user["groups"]:
                    base_data['GROUPS'] = [
                        group["name"] for group in user["groups"]
                    ]
                if (len(base_data['GROUPS']) > 1 and
                        'Everyone' in base_data['GROUPS']):
                    base_data['GROUPS'].remove('Everyone')
                data_list.append(base_data)
                self.logger.debug(
                    f"Successfully processed user: {user['email']}"
                )
            except Exception as e:
                self.logger.exception(
                    f"Error processing user {user.get('email', 'Unknown')}: "
                    f"{str(e)}"
                )

        # Log the result and return the final data list
        if data_list:
            self.logger.info(f"Found {len(data_list)} users")
        else:
            self.logger.warning("No users found in the response")
            return [{"error": "No users found"}]

        return data_list

    def get_group(self, name):
        """
        Retrieves group details by their name.

        Parameters:
            name (str): The name of the group to be retrieved.

        Returns:
            dict: Group details, or {'error': ...} if retrieval fails or not
            found.
        """
        self.logger.debug(
            f"Starting 'get_group' method for group name: {name}"
        )

        # Make the API call to fetch groups by name
        response = self.api_client.get(f"/api/v1/groups?name={name}")

        if not response or not response.ok:
            status_code = response.status_code if response else 'No response'
            self.logger.error(
                f"Failed to retrieve groups for name '{name}'. "
                f"Status Code: {status_code}"
            )
            return {
                "error": f"Failed to retrieve groups for name '{name}'"
            }

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse group response JSON.")
            return {"error": f"Failed to parse group response JSON: {str(e)}"}

        if not response_data:
            self.logger.warning(
                f"No group found with name '{name}'"
            )
            return {
                "error": f"No group found with name '{name}'"
            }

        group = response_data[0]
        group_id = group.get("_id")
        group_name = group.get("name")

        if not group_id or not group_name:
            self.logger.error(
                f"Incomplete group data for name '{name}'"
            )
            return {
                "error": f"Group '{name}' found but missing expected fields"
            }

        self.logger.debug(f"Group '{name}' found. ID: {group_id}")
        return {
            "GROUP_ID": group_id,
            "GROUP_NAME": group_name,
            "defaultRole": group.get("defaultRole", "")
        }

    def create_user(self, user_data):
        """
        Creates a new user by processing the provided user data to replace role
        names and group names with their corresponding IDs, then sends a POST
        request to create the user.

        Parameters:
            user_data (dict): A dictionary containing user details such as
            email, firstName, lastName, role (role name), groups (list of group
            names), and preferences.

        Returns:
            dict: The response from the API if successful,
                or a dictionary with an 'error' key if the operation fails.
        """
        self.logger.debug(f"Creating user with data: {user_data}")

        # Custom role mapping
        role_alias_mapping = {
            "VIEWER": "CONSUMER",
            "DESIGNER": "CONTRIBUTOR"
        }

        # Convert the role name in the user_data to uppercase for
        # case-insensitive matching
        user_role = user_data.get("role", "").upper()
        mapped_role = role_alias_mapping.get(user_role, user_role)

        # Step 1: Fetch roles from the API
        role_response = self.api_client.get('/api/roles')
        if not role_response or not role_response.ok:
            self.logger.error("Failed to fetch roles from API")
            return {"error": "Failed to fetch roles from API"}

        roles_mapping = [
            {"id": role["_id"], "name": role["name"].upper()}
            for role in role_response.json()
        ]
        self.logger.debug(f"Roles mapping: {roles_mapping}")

        # Step 2: Resolve roleId from role name
        for role in roles_mapping:
            if role["name"] == mapped_role:
                user_data["roleId"] = role["id"]
                break
        else:
            error_msg = (
                f"Role '{user_data.get('role')}' not found in roles_mapping"
            )
            self.logger.error(error_msg)
            return {"error": error_msg}

        user_data.pop("role", None)

        # Step 3: Resolve group IDs from group names (if provided)
        group_names = user_data.get("groups", [])
        if group_names:
            user_data["groups"] = [group.upper() for group in group_names]

            group_response = self.api_client.get('/api/v1/groups')
            if not group_response or not group_response.ok:
                self.logger.error("Failed to fetch groups from API")
                return {"error": "Failed to fetch groups from API"}

            groups_mapping = [
                {"id": group["_id"], "name": group["name"].upper()}
                for group in group_response.json()
            ]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    error_msg = (
                        f"Group '{group_name}' not found in groups_mapping"
                    )
                    self.logger.error(error_msg)
                    return {"error": error_msg}

            user_data["groups"] = updated_groups
        else:
            user_data["groups"] = []

        # Step 4: Send POST request to create the user
        self.logger.debug(f"Final user data for API call: {user_data}")
        response = self.api_client.post("/api/v1/users", data=user_data)

        if response and response.ok:
            response_data = response.json()
            self.logger.info(f"User created successfully: {response_data}")
            return response_data
        else:
            try:
                error_json = response.json()
                if isinstance(error_json, dict) and "error" in error_json:
                    error_message = error_json["error"].get(
                        "message", str(error_json["error"])
                    )
                else:
                    error_message = error_json.get("error", str(error_json))
            except Exception:
                error_message = "No response body or invalid JSON"

            self.logger.error(f"Failed to create user. Error: {error_message}")
            return {"error": error_message}

    def update_user(self, user_name, user_data):
        """
        Updates a user by their User Name.

        Parameters:
            user_name (str): The email or username of the user to be updated.
            user_data (dict): A dictionary containing user details to update,
            such as role, groups, etc.

        Returns:
            dict: The response from the API if successful,
                or a dictionary with an 'error' key if the operation fails.
        """
        self.logger.debug(f"Updating user with username: {user_name}")

        # Reuse the get_user method to fetch user details
        user = self.get_user(user_name)

        # If user is not found, return error
        if not user:
            self.logger.error(f"User '{user_name}' not found.")
            return {"error": f"User '{user_name}' not found."}

        # Custom role mapping
        role_alias_mapping = {
            "VIEWER": "CONSUMER",
            "DESIGNER": "CONTRIBUTOR"
        }

        # Step 1: Resolve role if provided
        if "role" in user_data:
            user_role = user_data["role"].upper()
            mapped_role = role_alias_mapping.get(user_role, user_role)

            role_response = self.api_client.get('/api/roles')
            if not role_response or not role_response.ok:
                if role_response:
                    status = role_response.status_code
                else:
                    status = "No response"

                self.logger.error(
                    "Failed to fetch roles from API. Status Code: %s",
                    status,
                )
                return {"error": "Failed to fetch roles from API."}

            roles_mapping = [
                {"id": role["_id"], "name": role["name"].upper()}
                for role in role_response.json()
            ]
            self.logger.debug(f"Roles mapping: {roles_mapping}")

            for role in roles_mapping:
                if role["name"] == mapped_role:
                    user_data["roleId"] = role["id"]
                    break
            else:
                error_msg = (
                    f"Role '{user_data['role']}' not found in roles_mapping"
                )
                self.logger.error(error_msg)
                return {"error": error_msg}

            user_data.pop("role", None)

        # Step 2: Resolve groups if provided
        group_names = user_data.get("groups", [])
        if group_names:
            user_data["groups"] = [group.upper() for group in group_names]

            group_response = self.api_client.get("/api/v1/groups")
            if not group_response or not group_response.ok:
                status = (
                    group_response.status_code
                    if group_response
                    else "No response"
                )
                self.logger.error(
                    "Failed to fetch groups from API. Status Code: %s",
                    status,
                )
                return {"error": "Failed to fetch groups from API."}

            groups_mapping = [
                {"id": group["_id"], "name": group["name"].upper()}
                for group in group_response.json()
            ]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    error_msg = (
                        f"Group '{group_name}' not found in groups_mapping"
                    )
                    self.logger.error(error_msg)
                    return {"error": error_msg}

            user_data["groups"] = updated_groups
        else:
            user_data["groups"] = []

        # Step 3: Send the PATCH request to update the user
        self.logger.debug(f"Final updated user data for API call: {user_data}")
        response = self.api_client.patch(
            f"/api/v1/users/{user['USER_ID']}",
            data=user_data
        )

        if response and response.ok:
            response_data = response.json()
            self.logger.info(f"User updated successfully: {response_data}")
            return response_data
        else:
            error_message = (
                response.json().get("error", "Unknown error")
                if response else "No response received"
            )
            self.logger.error(f"Failed to update user. Error: {error_message}")
            return {"error": error_message}

    def delete_user(self, user_name):
        """
        Deletes a user by their email (username).

        Parameters:
            user_name (str): The email or username of the user to be deleted.

        Returns:
            dict: Response from the API if successful,
                or an error message dict.
        """
        self.logger.debug(
            f"Starting 'delete_user' method for username: {user_name}"
        )

        # Reuse the get_user method to fetch user details
        self.logger.debug(
            f"Fetching user details for '{user_name}' using 'get_user' method."
        )
        user = self.get_user(user_name)
        self.logger.debug(f"User details fetched: {user}")

        # If user is not found, log and return error
        if not user or 'error' in user:
            error_msg = (
                f"User '{user_name}' not found. "
                "Cannot proceed with deletion."
            )
            self.logger.error(error_msg)
            self.logger.debug(
                f"Completed 'delete_user' method for username: {user_name}"
            )
            return {"error": error_msg}
        # support both formats just in case
        user_id = user.get("_id") or user.get("USER_ID")
        if not user_id:
            self.logger.error(
                f"User object for '{user_name}' is missing ID field. "
                "Cannot proceed."
            )
            return {
                "error": (
                    f"User '{user_name}' found but no ID field present."
                )
            }

        self.logger.debug(
            f"User '{user_name}' found. Proceeding to delete user with ID: "
            f"{user_id}"
        )

        # Send the DELETE request
        response = self.api_client.delete(f"/api/v1/users/{user['USER_ID']}")

        if response and response.status_code == 204:
            self.logger.info(
                (
                    f"User '{user_name}' (ID: {user['USER_ID']}) deleted. "
                    "No content returned."
                )
            )
            self.logger.debug(
                f"Completed 'delete_user' method for username: {user_name}"
            )
            return {"message": "User deleted successfully."}

        elif response and response.ok:
            try:
                response_data = response.json()
            except Exception:
                response_data = {
                    "message": "User deleted, but no JSON body returned."
                }
            self.logger.info(
                f"User '{user_name}' (ID: {user['USER_ID']}) deleted."
            )
            self.logger.debug(f"API response: {response_data}")
            self.logger.debug(
                f"Completed 'delete_user' method for username: {user_name}"
            )
            return response_data

        else:
            try:
                error_message = response.json().get("error", "Unknown error")
            except Exception:
                error_message = "No response body or invalid JSON"
            self.logger.error(
                (
                    f"Failed to delete user '{user_name}' "
                    f"(ID: {user['USER_ID']}). Error: {error_message}"
                )
            )
            self.logger.debug(
                f"Completed 'delete_user' method for username: {user_name}"
            )
            return {"error": error_message}

    def users_per_group(self, group_name):
        """
        Retrieves all users within a specific group by name.

        Parameters:
            group_name (str): The name of the group.

        Returns:
            list or dict: A list of users in the group if successful, or a
            dictionary containing an 'error' key if the operation fails.
        """
        self.logger.debug(
            f"Starting 'users_per_group' method for group: {group_name}"
        )

        # Step 1: Fetch group details
        group = self.get_group(group_name)
        if not group or 'error' in group:
            error_msg = f"Group '{group_name}' not found. Cannot proceed."
            self.logger.error(error_msg)
            return {"error": error_msg}

        group_id = group.get("GROUP_ID")
        self.logger.debug(
            f"Group '{group_name}' found with ID: {group_id}. "
            f"Proceeding to fetch users."
        )

        # Step 2: Fetch users for the group
        url = f'/api/v1/users?groupId={group_id}'
        response = self.api_client.get(url)

        if not response or not response.ok:
            status = response.status_code if response else 'No response'
            error_msg = (
                f"Failed to retrieve users for group '{group_name}'. "
                f"Status Code: {status}"
            )
            self.logger.error(error_msg)
            return {"error": error_msg}

        try:
            users = response.json()
            self.logger.debug(
                f"Found {len(users)} users in group '{group_name}'"
            )
            return users
        except Exception as e:
            error_msg = (
                f"Failed to parse user list for group '{group_name}': {e}"
            )
            self.logger.error(error_msg)
            return {"error": error_msg}

    def users_per_group_all(self):
        """
        Retrieves all groups and maps them with the users belonging to those
        groups.
        Groups like 'Everyone' and 'All users in system' are excluded.
        Users with roles like 'admin', 'dataAdmin', and 'sysAdmin' are mapped
        to the existing 'Admins' group.
        Groups with no users are also included in the final result.

        Returns:
            list: A list of dictionaries, where each dictionary contains a
            group name and the list of usernames in that group.
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
        groups_dict = {
            group['name']: []
            for group in group_data if group['name'] not in EXCLUDED_GROUPS
        }
        if "Admins" not in groups_dict:
            groups_dict["Admins"] = []  # Ensure 'Admins' group exists

        self.logger.debug(
            f"Initialized groups dictionary with {len(groups_dict)} entries "
            f"(excluding excluded groups)."
        )

        # Step 4: Populate group membership from users
        for user in all_users:
            for group in user.get("GROUPS", []):
                if group not in EXCLUDED_GROUPS:
                    groups_dict[group].append(user["USER_NAME"])
                    self.logger.debug(
                        f"Added user '{user['USER_NAME']}' to group '{group}'"
                    )

        # Step 5: Add users with admin-like roles to 'Admins'
        for user in all_users:
            if user.get("ROLE_NAME") in ["sysAdmin", "dataAdmin", "admin"]:
                groups_dict["Admins"].append(user["USER_NAME"])
                self.logger.debug(
                    f"Added user '{user['USER_NAME']}' to Admins group "
                    f"based on role."
                )

        # Step 6: Prepare final result
        result = [
            {"group": group_name, "username": usernames}
            for group_name, usernames in groups_dict.items()
        ]

        if result:
            self.logger.info(f"Resolved {len(result)} group entries.")
        else:
            self.logger.error("No groups or users found.")

        return result

    def change_folder_and_dashboard_ownership(
        self,
        executing_user,
        folder_name,
        new_owner_name,
        original_owner_rule='edit',
        change_dashboard_ownership=True
    ):
        """
        Method to change the ownership of folders and optionally dashboards.
        This method changes the ownership of a target folder and the entire
        tree structure surrounding it, including subfolders, sibling folders,
        and parent folders.
        Optionally, it will also change the ownership of dashboards associated
        with these folders.

        Parameters:
            user_name (str): The user running the tool. This is necessary for
            API access checks.
            folder_name (str): The target folder whose ownership needs to be
            changed.
            new_owner_name (str): The new owner to whom the folder (and
            optionally dashboards) ownership will be transferred.
            original_owner_rule (str, optional): Specifies the ownership rule
            to set original owner after changing ownership('edit' or 'view').
            Default is 'edit'.
            change_dashboard_ownership (bool, optional): Specifies whether to
            also change the ownership of dashboards in the folder tree. Default
            is True.
        """
        folder_details = set()
        dashboard_details = set()
        total_folders_changed = 0
        total_dashboards_changed = 0

        self.logger.info("Starting folder and dashboard traversal...")
        self.logger.debug(
            f"Looking for folder '{folder_name}' to change ownership to "
            f"'{new_owner_name}'"
        )

        matching_folders = []
        oid_to_parent_map = {}

        # Check if the executing user exists and retrieve their USER_ID
        user_info = self.get_user(executing_user)
        if not user_info or "USER_ID" not in user_info:
            error_msg = (
                f"User '{executing_user}' not found or USER_ID missing."
            )
            self.logger.error(error_msg)
            return {"error": error_msg}
        user_id = user_info["USER_ID"]

        # Check if the new owner exists and retrieve their USER_ID
        new_owner = self.get_user(new_owner_name)
        if not new_owner or "USER_ID" not in new_owner:
            error_msg = (
                f"New owner '{new_owner_name}' not found or USER_ID missing."
            )
            self.logger.error(error_msg)
            return {"error": error_msg}
        new_owner_id = new_owner["USER_ID"]

        # Build a parent map and collect matching folders
        def build_folder_map_and_find_matches(folders, parent=None):
            for folder in folders:
                oid_to_parent_map[folder['oid']] = parent
                self.logger.debug(
                    f"Checking folder '{folder['name']}' (ID: {folder['oid']})"
                )

                if folder['name'] == folder_name:
                    self.logger.info(
                        (
                            f"Found target folder: {folder['name']} "
                            f"(ID: {folder['oid']})"
                        )
                    )
                    matching_folders.append(folder)
                    traverse_folder(folder)

                if 'folders' in folder and folder['folders']:
                    build_folder_map_and_find_matches(
                        folder['folders'], folder
                    )

        # Traverse folder and dashboards
        def traverse_folder(folder):
            if (folder['oid'], folder['name']) not in folder_details:
                folder_details.add((folder['oid'], folder['name']))
                self.logger.info(
                    f"Folder found: {folder['name']} (ID: {folder['oid']})"
                )

                for dash in folder.get("dashboards", []):
                    if (dash['oid'], dash['title']) not in dashboard_details:
                        dashboard_details.add((dash['oid'], dash['title']))
                        self.logger.info(
                            (
                                f"Dashboard found: {dash['title']} "
                                f"(ID: {dash['oid']})"
                            )
                        )

                for subfolder in folder.get("folders", []):
                    traverse_folder(subfolder)
            else:
                if not folder.get("folders"):
                    self.logger.debug(
                        f"No subfolders in folder - {folder['name']}"
                    )

        # Traverse a folder’s parent and siblings
        def traverse_parents_and_siblings(folder):
            parent = oid_to_parent_map.get(folder['oid'])
            if parent:
                self.logger.info(
                    f"Parent folder: {parent['name']} (ID: {parent['oid']})"
                )
                traverse_folder(parent)

                for sibling in parent.get("folders", []):
                    if sibling['oid'] != folder['oid']:
                        traverse_folder(sibling)

        # Entry point to fetch and process folders
        def get_folder_details():
            self.logger.debug("Fetching all folders from API")
            response = self.api_client.get('/api/v1/navver')
            response = response.json()

            if not response or 'folders' not in response:
                self.logger.error(
                    "No folders found in the API response or invalid response."
                )
                return False

            self.logger.info(f"Searching for folders named '{folder_name}'...")
            build_folder_map_and_find_matches(response['folders'])

            if not matching_folders:
                self.logger.warning(f"No folders named '{folder_name}' found.")
                return False

            for folder in matching_folders:
                traverse_parents_and_siblings(folder)

            self.logger.info(
                f"Total target folders matched: {len(matching_folders)}"
            )
            return True

        folder_found = get_folder_details()

        if folder_found:
            self.logger.info("Collected Folder Details:")
            for folder_id, folder_name in folder_details:
                self.logger.info(f"Folder: {folder_name} (ID: {folder_id})")

            self.logger.info("Collected Dashboard Details:")
            for dash_id, dash_name in dashboard_details:
                self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
        else:
            self.logger.warning(
                "Folder not found, moving to search dashboards and "
                "grant access step..."
            )
            limit = 50
            skip = 0
            dashboards = []
            while True:
                self.logger.debug(
                    f"Fetching dashboards (limit={limit}, skip={skip})"
                )
                dashboard_response = self.api_client.post(
                    '/api/v1/dashboards/searches',
                    data={
                        "queryParams": {
                            "ownershipType": "allRoot",
                            "search": "",
                            "ownerInfo": True,
                            "asObject": True
                        },
                        "queryOptions": {
                            "sort": {"title": 1},
                            "limit": limit,
                            "skip": skip
                        }
                    }
                )
                dashboard_response = dashboard_response.json()

                if (
                    not dashboard_response or
                    len(dashboard_response.get("items", [])) == 0
                ):
                    self.logger.debug("No more dashboards found.")
                    break
                else:
                    dashboards.extend(dashboard_response["items"])
                    skip += limit

            all_folder_ids = {
                dic["parentFolder"]
                for dic in dashboards
                if "parentFolder" in dic and dic["parentFolder"]
            }
            self.logger.debug(
                f"Collected parent folder IDs from dashboards: "
                f"{all_folder_ids}"
            )

            folder_response = self.api_client.get('/api/v1/folders')
            folder_response = folder_response.json()
            user_folder_ids = {
                folder["oid"]
                for folder in folder_response
                if "oid" in folder
            }
            self.logger.debug(
                f"Collected user-accessible folder IDs: "
                f"{user_folder_ids}"
            )

            diff = all_folder_ids - user_folder_ids
            self.logger.info(
                f"Folders the user does not have access to: "
                f"{diff}"
            )

            for dash in dashboards:
                if (
                    'parentFolder' in dash and
                    dash["parentFolder"] in diff
                ):
                    payload = dash["shares"]
                    payload.append({
                        "shareId": user_id,
                        "type": "user",
                        "rule": "edit",
                        "subscribe": False
                    })
                    self.logger.debug(
                        f"Sharing dashboard {dash['title']} "
                        f"(ID: {dash['oid']}) with {executing_user}"
                    )
                    share_response = self.api_client.post(
                        f'/api/shares/dashboard/{dash["oid"]}?adminAccess=true',
                        data={"sharesTo": payload}
                    )
                    share_response = share_response.json()
                    if share_response:
                        self.logger.info(
                            f"Dashboard '{dash['title']}' shared with "
                            f"{executing_user}"
                        )
                    else:
                        self.logger.error(
                            f"Failed to share dashboard '{dash['title']}': "
                            f"{share_response}"
                        )

            self.logger.info(
                "Retrying folder and dashboard traversal after granting "
                "access..."
            )
            folder_found = get_folder_details()

            if folder_found:
                self.logger.info(
                    "Collected Folder Details after granting access:"
                )
                for folder_id, folder_name in folder_details:
                    self.logger.info(
                        f"Folder: {folder_name} (ID: {folder_id})"
                    )

                self.logger.info(
                    "Collected Dashboard Details after granting access:"
                )
                for dash_id, dash_name in dashboard_details:
                    self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
            else:
                self.logger.warning(
                    f"Folder '{folder_name}' not found after attempting to "
                    f"grant access. Exiting..."
                )
                return

        # Change ownership logic
        if (folder_details or
                (change_dashboard_ownership and dashboard_details)):
            self.logger.info("Changing folder and dashboard owners...")

            # Change folder owners
            self.logger.debug(f"Folders to be changed: {folder_details}")
            self.logger.info(
                f"Changing ownership for {len(folder_details)} folders and "
                f"{len(dashboard_details)} dashboards."
            )
            for folder_id, folder_name in folder_details:
                data = {"owner": new_owner_id}
                self.logger.debug(
                    f"Changing owner for folder {folder_name} (ID: {folder_id}) "
                    f"with data: {data}"
                )

                response = self.api_client.patch(
                    f'/api/v1/folders/{folder_id}',
                    data=data
                )
                response = response.json()

                # Log response
                self.logger.debug(
                    f"API response for folder change: {response}"
                )

                if response and response.get("owner") == new_owner_id:
                    self.logger.info(
                        f"Folder '{folder_name}' owner changed to "
                        f"{new_owner_name}"
                    )
                    total_folders_changed += 1
                else:
                    self.logger.error(
                        f"Failed to change folder owner for '{folder_name}'."
                    )

            # Change dashboard owners if enabled
            if change_dashboard_ownership:
                for dash_id, dash_name in dashboard_details:
                    current_dashboard = self.api_client.get(f'/api/v1/dashboards/{dash_id}')
                    current_dashboard = current_dashboard.json()
                    if not current_dashboard:
                        self.logger.error(f"Dashboard with ID '{dash_id}' not found. Skipping.")
                        continue

                    current_owner_id = current_dashboard.get("owner")

                    if current_owner_id == new_owner_id:
                        self.logger.info(
                            f"Dashboard '{dash_name}' is already owned by "
                            f"{new_owner_name}, no action needed."
                        )
                    else:
                        if current_owner_id == user_id:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(f'/api/v1/dashboards/{dash_id}/change_owner', data=data)
                            response = response.json()
                        else:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(
                                f'/api/v1/dashboards/{dash_id}/change_owner?adminAccess=true',
                                data=data
                            )
                            response = response.json()

                        if response:
                            self.logger.info(f"Dashboard '{dash_name}' owner changed to {new_owner_name}")
                            total_dashboards_changed += 1
                        else:
                            self.logger.error(f"Failed to change dashboard owner for '{dash_name}'.")

            # Log total changes
            self.logger.info(
                f"Ownership changed for {total_folders_changed} folders and "
                f"{total_dashboards_changed} dashboards."
            )
            return {
                "total_folders_changed": total_folders_changed,
                "total_dashboards_changed": total_dashboards_changed
            }
        else:
            self.logger.info("No folders or dashboards to change ownership. Exiting.")
            return None


    def get_datamodel_columns(self, datamodel_name):
        """
        Retrieves columns from a DataModel by collecting them from its datasets and tables.

        Parameters:
            datamodel_name (str): The name of the DataModel from which to extract columns.

        Returns:
            list: A list of dictionaries where each dictionary contains DataModel ID, DataModel name,
            table name, and column name.
        """
        all_columns = []

        self.logger.info(f"Fetching columns for DataModel: {datamodel_name}")

        # Step 1: Get DataModel ID
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(schema_url)

        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return []

        response_data = response.json()

        # Endpoint is already filtered by title; just extract the oid
        if isinstance(response_data, list):
            first_match = next(
                (x for x in response_data if isinstance(x, dict) and x.get("oid")),
                None,
            )
            datamodel_id = first_match.get("oid") if first_match else None
        elif isinstance(response_data, dict):
            datamodel_id = response_data.get("oid")
        else:
            datamodel_id = None

        if not datamodel_id:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        self.logger.info(f"DataModel ID for '{datamodel_name}': {datamodel_id}")

        # Step 2: Get DataSets
        self.logger.debug(f"Fetching DataSets for DataModel ID '{datamodel_id}'")
        dataset_url = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
        response = self.api_client.get(dataset_url)

        if not response or response.status_code != 200:
            self.logger.error(
                f"Failed to fetch DataSet schema for DataModel ID "
                f"'{datamodel_id}'"
            )
            return []

        response_data = response.json()
        dataset_ids = [
            x.get("oid")
            for x in response_data
            if isinstance(x, dict) and "oid" in x
        ]

        if not dataset_ids:
            self.logger.warning(
                f"No datasets found for DataModel '{datamodel_name}' "
                f"(ID: {datamodel_id})."
            )
            return []

        total_datasets = len(dataset_ids)
        self.logger.info(
            f"Found {total_datasets} datasets for DataModel "
            f"'{datamodel_name}': {dataset_ids}"
        )

        # Step 3: Loop through datasets and collect columns from tables
        total_tables = 0
        total_columns = 0

        for dataset_index, dataset_id in enumerate(dataset_ids, start=1):
            self.logger.debug(
                f"Processing DataSet {dataset_index}/{total_datasets}: "
                f"Fetching tables for DataSet ID '{dataset_id}'"
            )

            table_url = f"{dataset_url}/{dataset_id}/tables"
            response = self.api_client.get(table_url)

            if not response or response.status_code != 200:
                self.logger.error(
                    f"Failed to fetch tables for DataSet ID '{dataset_id}'"
                )
                continue

            tables = response.json()
            dataset_table_count = len(tables)
            total_tables += dataset_table_count
            self.logger.info(
                f"Dataset {dataset_index}: Found {dataset_table_count} "
                f"tables in DataSet ID '{dataset_id}'"
            )

            for table in tables:
                table_name = table.get("name")
                if not table_name:
                    self.logger.warning(
                        f"Table in DataSet ID '{dataset_id}' has no name. "
                        "Skipping."
                    )
                    continue

                columns = table.get("columns")
                if not columns or not isinstance(columns, list):
                    self.logger.warning(
                        f"Table '{table_name}' in DataSet ID '{dataset_id}' has no columns. Skipping."
                    )
                    continue

                table_column_count = len(columns)
                total_columns += table_column_count
                self.logger.debug(f"Table '{table_name}' contains {table_column_count} columns")

                for column in columns:
                    column_name = column.get("name")
                    if not column_name:
                        self.logger.warning(
                            f"A column in table '{table_name}' has no name. Skipping."
                        )
                        continue

                    all_columns.append({
                        "datamodel_id": datamodel_id,
                        "datamodel_name": datamodel_name,
                        "table": table_name,
                        "column": column_name
                    })

        # Step 4: Final logging
        self.logger.info(
            f"DataModel '{datamodel_name}': Processed {total_datasets} datasets, "
            f"{total_tables} tables, and {total_columns} columns."
        )
        self.logger.debug(f"Final collected column data: {all_columns}")

        return all_columns


    def get_unused_columns(self, datamodel_name):
        """
        Identify unused columns in a given DataModel by comparing all available columns against the columns
        referenced in associated dashboards.

        Covers:
        - Dashboard Filters: Dashboard-level filters, Widget filters, Dependent Filters.
        - Widget Panels: Includes Row, Values, Column panels, and Measured Filters.

        Parameters:
            datamodel_name (str): The name of the DataModel to analyze.

        Returns:
            list: A list of dictionaries containing unused column details with a "used" field set to True or False.
        """
        self.logger.info(f"Starting analysis for unused columns in DataModel: {datamodel_name}")

        # Step 1: Get all columns from the DataModel
        all_columns = self.get_datamodel_columns(datamodel_name)
        if not all_columns:
            self.logger.warning(f"No columns found for DataModel '{datamodel_name}'. Exiting.")
            return []

        total_datamodel_columns = len(all_columns)
        self.logger.info(f"Retrieved {total_datamodel_columns} columns from DataModel '{datamodel_name}'")

        # Step 2: Fetch dashboards associated with this DataModel
        self.logger.info(f"Fetching dashboards linked to DataModel '{datamodel_name}'")
        dashboard_url = f"/api/v1/dashboards/admin?dashboardType=owner&datasourceTitle={datamodel_name}"
        response = self.api_client.get(dashboard_url)

        if not response or not response.ok:
            self.logger.error(f"Failed to fetch dashboards for DataModel '{datamodel_name}'.")
            return []

        dashboards = response.json()
        if not dashboards:
            self.logger.warning(f"No dashboards found using DataModel '{datamodel_name}' or access is restricted.")
            return []

        dashboard_ids = {dash["oid"] for dash in dashboards}  # Get unique dashboard IDs
        total_dashboards = len(dashboard_ids)
        self.logger.info(f"Found {total_dashboards} dashboards linked to DataModel '{datamodel_name}'")
        self.logger.debug(f"Dashboard IDs: {dashboard_ids}")

        # Step 3: Extract columns from all linked dashboards
        dashboard_columns = []
        total_filters = 0
        total_widgets = 0

        for dashboard_id in dashboard_ids:
            dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
            response = self.api_client.get(dashboard_url)

            if not response or not response.ok:
                self.logger.error(f"Failed to export dashboard with ID '{dashboard_id}'")
                continue

            dashboard = response.json()[0]
            dashboard_name = dashboard["title"]
            self.logger.debug(f"Analyzing Dashboard '{dashboard_name}' (ID: {dashboard_id})")

            # Extract columns from filters
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
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({
                                "dashboard_name": dashboard_name,
                                "source": "filter",
                                "widget_id": "N/A",
                                "table": table,
                                "column": column
                            })

                            self.logger.debug(
                                f"Filter {filter_index}: Extracted from levels - "
                                f"Table: {table}, Column: {column}"
                            )

                    elif "jaql" in filter:
                        dim_value = filter["jaql"].get("dim", "Unknown.Table")
                        if "." in dim_value:
                            table, column = dim_value.strip("[]").split(".", 1)
                        else:
                            table, column = dim_value.strip("[]"), "Unknown Column"

                        dashboard_columns.append({
                            "dashboard_name": dashboard_name,
                            "source": "filter",
                            "widget_id": "N/A",
                            "table": table,
                            "column": column
                        })

                        self.logger.debug(
                            f"Filter {filter_index}: Extracted from JAQL - "
                            f"Table: {table}, Column: {column}"
                        )

            self.logger.info(f"Processed {filter_count} filters for dashboard '{dashboard_name}'")

            # Extract columns from widgets
            widget_count = 0
            column_count = 0
            self.logger.debug(f"Extracting columns from widgets for dashboard '{dashboard_name}'")

            total_widgets_in_dashboard = len(dashboard.get("widgets", []))
            self.logger.debug(f"Total widgets found: {total_widgets_in_dashboard}")

            for widget_index, widget in enumerate(dashboard.get("widgets", []), start=1):
                widget_count += 1
                widget_id = widget.get("oid", "Unknown Widget")
                widget_title = widget.get("title", "Unnamed Widget")

                self.logger.debug(
                    f"Processing widget {widget_index}/{total_widgets_in_dashboard}: "
                    f"'{widget_title}' (ID: {widget_id})"
                )

                for panel in widget.get("metadata", {}).get("panels", []):
                    for item in panel.get("items", []):
                        jaql = item.get("jaql", {})

                        # Extract columns from 'context' (Formula-based columns)
                        if "context" in jaql and isinstance(jaql["context"], dict):
                            if not jaql["context"]:
                                self.logger.info(
                                    f"Widget {widget_index}: 'context' is an empty dict. "
                                    "Skipping context extraction."
                                )
                                continue

                            for _, value in jaql["context"].items():
                                dim_value = value.get("dim", "Unknown.Table")
                                if "." in dim_value:
                                    table, column = dim_value.strip("[]").split(".", 1)
                                else:
                                    table, column = dim_value.strip("[]"), "Unknown Column"

                                dashboard_columns.append({
                                    "datamodel_name": datamodel_name,
                                    "dashboard_name": dashboard_name,
                                    "source": "widget",
                                    "widget_id": widget_id,
                                    "table": table,
                                    "column": column
                                })
                                column_count += 1

                                self.logger.debug(
                                    f"Widget {widget_index}: Extracted from context (Formula) - "
                                    f"Table: {table}, Column: {column}"
                                )

                        # Extract columns from 'dim' (Regular columns)
                        else:
                            dim_value = jaql.get("dim", "Unknown.Table")
                            if not dim_value:
                                self.logger.info(f"Widget {widget_index}: Missing 'dim' in jaql. Skipping item.")
                                continue
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({
                                "datamodel_name": datamodel_name,
                                "dashboard_name": dashboard_name,
                                "source": "widget",
                                "widget_id": widget_id,
                                "table": table,
                                "column": column
                            })
                            column_count += 1

                            self.logger.debug(
                                f"Widget {widget_index}: Extracted from regular source - "
                                f"Table: {table}, Column: {column}"
                            )

            total_widgets += widget_count
            self.logger.info(
                f"Processed {widget_count} widgets and {filter_count} filters "
                f"and extracted {column_count} columns for dashboard "
                f"'{dashboard_name}'"
            )

        self.logger.info(f"Total filters processed: {total_filters}")
        self.logger.info(f"Total widgets processed: {total_widgets}")
        self.logger.info(f"Total dashboard columns extracted: {len(dashboard_columns)}")

        # Step 4: Identify used and unused columns
        dashboard_columns_set = set()

        for entry in dashboard_columns:
            table = entry["table"]
            column = entry["column"]

            # Fix issue: Remove "(Calendar)" from dashboard columns only
            if column.endswith(" (Calendar)"):
                column = column.replace(" (Calendar)", "").strip()

            dashboard_columns_set.add((table, column))

        used_columns_count = 0
        unused_columns_count = 0

        for entry in all_columns:
            table = entry["table"]
            column = entry["column"]

            # Check against cleaned dashboard column names
            entry['used'] = (table, column) in dashboard_columns_set

            if entry['used']:
                used_columns_count += 1
            else:
                unused_columns_count += 1

        self.logger.info(f"Total used columns: {used_columns_count}")
        self.logger.info(f"Total unused columns: {unused_columns_count}")

        return all_columns

    def get_all_dashboard_shares(self):
        """
        Method to retrieve all dashboard shares, including user and group details for each shared dashboard.

        This method uses pagination to retrieve all dashboards and their share information, and it collects
        corresponding user and group details for each share.

        Returns:
            list: A list of dictionaries containing the dashboard title, share type (user or group),
            and share name (email or group name).
        """
        limit = 50
        skip = 0
        dashboards = []

        self.logger.info("Starting to retrieve dashboard shares...")

        # Step 1: Fetch all dashboards with pagination
        while True:
            self.logger.debug(f"Fetching dashboards with limit={limit}, skip={skip}")
            dashboard_response = self.api_client.post(
                '/api/v1/dashboards/searches',
                data={
                    "queryParams": {
                        "ownershipType": "allRoot",
                        "search": "",
                        "ownerInfo": True,
                        "asObject": True
                    },
                    "queryOptions": {
                        "sort": {"title": 1},
                        "limit": limit,
                        "skip": skip
                    }
                }
            )

            if not dashboard_response or dashboard_response.status_code != 200:
                self.logger.error("Failed to fetch dashboards.")
                break

            response_data = dashboard_response.json()
            items = response_data.get("items", [])
            if not items:
                self.logger.info("No more dashboards found.")
                break

            dashboards.extend(items)
            skip += limit
            self.logger.debug(f"Retrieved {len(items)} dashboards, total so far: {len(dashboards)}")

        # Step 2: Fetch all users
        self.logger.info("Fetching all users.")
        users_response = self.api_client.get('/api/v1/users')
        if not users_response or users_response.status_code != 200:
            self.logger.error("Failed to fetch users.")
            return []

        users_data = users_response.json()
        users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]

        # Step 3: Fetch all groups
        self.logger.info("Fetching all groups.")
        groups_response = self.api_client.get('/api/v1/groups')
        if not groups_response or groups_response.status_code != 200:
            self.logger.error("Failed to fetch groups.")
            return []

        groups_data = groups_response.json()
        groups_detail = [{"id": group["_id"], "name": group.get("name", "Unknown Group")} for group in groups_data]

        shared_list = []

        # Step 4: Parse the dashboards to find shared users and groups
        self.logger.debug(f"Parsing {len(dashboards)} dashboards for shared users and groups.")
        for dashboard in dashboards:
            if dashboard.get("shares"):
                for share in dashboard["shares"]:
                    share_info = {"dashboard": dashboard["title"], "type": None, "name": None}

                    if share["type"] == "user":
                        user = next((user for user in users_detail if user["id"] == share["shareId"]), None)
                        if user:
                            share_info["type"] = "user"
                            share_info["name"] = user["email"]
                    elif share["type"] == "group":
                        group = next((group for group in groups_detail if group["id"] == share["shareId"]), None)
                        if group:
                            share_info["type"] = "group"
                            share_info["name"] = group["name"]

                    shared_list.append(share_info)
            else:
                # Add placeholder if there are no shares for the dashboard
                shared_list.append({
                    "dashboard": dashboard["title"],
                    "type": None,
                    "name": None
                })

        self.logger.info(f"Parsed {len(shared_list)} shared dashboards.")

        # Return the result as a list of dictionaries
        return shared_list

    def create_schedule_build(self, datamodel_name, build_type="ACCUMULATE", *, days=None, hour=None, minute=None,
                              interval_days=None, interval_hours=None, interval_minutes=None):
        """
        Method to create a schedule build for a DataModel.

        Supports both cron-based schedules (e.g., every Monday at 9:00 UTC)
        and interval-based schedules (e.g., every 2 days, 1 hour, 30 minutes).

        Parameters:
            datamodel_name (str): The name of the DataModel.
            build_type (str): Optional. Type of the build (e.g., "ACCUMULATE", "FULL",
            "SCHEMA_CHANGES"). Defaults to "ACCUMULATE".
            days (list, optional): List of days for cron schedule. Eg.: ["SUN", "MON", "TUE", "WED", "THU", "FRI",
            "SAT"] or ["*"] for all days.
            hour (int, optional): Hour in 24-hour format (UTC).
            minute (int, optional): Minute of the hour (UTC).
            interval_days (int, optional): Interval in days.
            interval_hours (int, optional): Interval in hours.
            interval_minutes (int, optional): Interval in minutes.

        Returns:
            dict: API response or error.
        """
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(schema_url)

        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return {"error": f"Failed to fetch DataModel schema for '{datamodel_name}'"}

        response_data = response.json()
        if not response_data:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": f"DataModel '{datamodel_name}' not found"}

        # Extract DataModel ID
        datamodel_id = response_data.get("oid")
        self.logger.info(f"DataModel ID for '{datamodel_name}' is {datamodel_id}")

        # Determine schedule type
        if interval_days is not None or interval_hours is not None or interval_minutes is not None:
            self.logger.info("Creating interval-based schedule...")
            days = interval_days or 0
            hours = interval_hours or 0
            minutes = interval_minutes or 0
            interval_seconds = (days * 86400) + (hours * 3600) + (minutes * 60)

            if interval_seconds <= 0:
                self.logger.error("Interval must be greater than 0 seconds.")
                return {"error": "Interval must be greater than 0 seconds."}

            schedule_payload = {
                "scheduleType": "Interval",
                "buildType": build_type,
                "intervalSeconds": interval_seconds
            }
        elif days and hour is not None and minute is not None:
            self.logger.info("Creating cron-based schedule...")
            if days == ["*"]:
                days_string = "0,1,2,3,4,5,6"
            else:
                day_mapping = {
                    "SUN": "0", "MON": "1", "TUE": "2", "WED": "3",
                    "THU": "4", "FRI": "5", "SAT": "6"
                }
                days_string = ",".join([day_mapping[day] for day in days])

            cron_string = f"{minute} {hour} * * {days_string}"
            self.logger.debug(f"Generated cron string: {cron_string}")

            schedule_payload = {
                "cronString": cron_string,
                "buildType": build_type,
                "daysOfWeek": days,
                "hour": hour,
                "minute": minute
            }
        else:
            self.logger.error("Invalid schedule configuration: Provide either interval or full cron config.")
            return {"error": "Invalid schedule configuration: Provide either interval or full cron config."}

        self.logger.info("Creating schedule build with the following details:")
        self.logger.debug(schedule_payload)

        api_url = f"/api/v2/datamodels/{datamodel_id}/schedule"
        response = self.api_client.post(api_url, data=schedule_payload)

        if not response or response.status_code not in [200, 201]:
            self.logger.error(
                "Failed to create schedule build. Response: %s",
                getattr(response, 'text', 'No response text')
            )
            return {"error": "Failed to create schedule build."}

        try:
            response_data = response.json()
            self.logger.info(f"Schedule build created successfully. Response: {response_data}")
            return response_data
        except (AttributeError, ValueError):
            self.logger.warning("Response does not contain valid JSON. Returning raw response.")
            return {
                "message": "Schedule build created successfully",
                "raw_response": getattr(response, 'text', 'No response text')
            }
