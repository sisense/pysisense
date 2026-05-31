from __future__ import annotations

from typing import Any


class UsersMixin:
    def _build_role_and_group_mappings(
        self,
    ) -> dict[str, dict[str, str]] | None:
        """
        Internal helper to fetch and build role and group ID-to-name mappings.

        Returns:
            dict or None: {
                "roles_by_id": {role_id: role_name, ...},
                "groups_by_id": {group_id: group_name, ...},
            }
            or None if any API call fails.
        """
        # Fetch roles
        roles_response = self.api_client.get("/api/roles")
        if not roles_response or not roles_response.ok:
            self.logger.error("Failed to fetch roles from API.")
            return None

        try:
            roles_data = roles_response.json()
            roles_by_id = {role.get("_id"): role.get("name") for role in roles_data if isinstance(role, dict) and role.get("_id")}
        except Exception:
            self.logger.exception("Failed to parse roles response JSON.")
            return None

        # Fetch groups
        groups_response = self.api_client.get("/api/v1/groups")
        if not groups_response or not groups_response.ok:
            self.logger.error("Failed to fetch groups from API.")
            return None

        try:
            groups_data = groups_response.json()
            groups_by_id = {group.get("_id"): group.get("name") for group in groups_data if isinstance(group, dict) and group.get("_id")}
        except Exception:
            self.logger.exception("Failed to parse groups response JSON.")
            return None

        self.logger.debug(f"Built role and group mappings in helper. Roles: {len(roles_by_id)}, Groups: {len(groups_by_id)}")
        return {"roles_by_id": roles_by_id, "groups_by_id": groups_by_id}

    def get_user_with_role_and_group_names(self, user_name: str) -> dict[str, Any]:
        """
        Retrieves a single user by email/username and returns both role and
        group IDs and names.

        Parameters:
            user_name (str): The email or username of the user to be retrieved.

        Returns:
            dict: User details including:
                - USER_ID
                - USER_NAME
                - FIRST_NAME
                - LAST_NAME
                - EMAIL
                - IS_ACTIVE
                - ROLE_ID
                - ROLE_NAME
                - GROUP_IDS (list of group IDs)
                - GROUP_NAMES (list of group names)
            or {"error": "..."} on failure.
        """
        self.logger.debug(f"Getting user with role and group IDs/names for: {user_name}")

        # Reuse expanded users endpoint to get role & group objects
        params = {"expand": "groups,role"}
        response = self.api_client.get("/api/v1/users", params=params)

        if not response or not response.ok:
            error_msg = f"Failed to retrieve users from API for username: {user_name}."
            self.logger.error(f"{error_msg} Status Code: {response.status_code if response else 'No response'}")
            return {"error": error_msg}

        try:
            users = response.json()
        except Exception as exc:
            self.logger.exception("Error decoding JSON response for user list in get_user_with_role_and_group_names.")
            return {"error": f"Failed to decode API response: {exc}"}

        ROLE_MAPPING = {
            "consumer": "viewer",
            "super": "sysAdmin",
            "contributor": "dashboardDesigner",
        }

        for user in users:
            try:
                if user.get("email") != user_name:
                    continue

                role_obj = user.get("role") or {}
                groups_obj = user.get("groups") or []

                role_id = role_obj.get("_id")
                role_name_raw = role_obj.get("name")
                role_name = ROLE_MAPPING.get(role_name_raw, role_name_raw)

                group_ids = []
                group_names = []
                for g in groups_obj:
                    if not isinstance(g, dict):
                        continue
                    gid = g.get("_id")
                    gname = g.get("name")
                    if gid:
                        group_ids.append(gid)
                    if gname:
                        group_names.append(gname)

                result = {
                    "USER_ID": user.get("_id"),
                    "USER_NAME": user.get("userName"),
                    "FIRST_NAME": user.get("firstName"),
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user.get("email"),
                    "IS_ACTIVE": user.get("active"),
                    "ROLE_ID": role_id,
                    "ROLE_NAME": role_name,
                    "GROUP_IDS": group_ids,
                    "GROUP_NAMES": group_names,
                }

                self.logger.info(f"Found user '{user_name}' with role and group IDs/names.")
                return result

            except Exception as exc:
                self.logger.exception(f"Error processing user object in get_user_with_role_and_group_names: {exc}")

        self.logger.warning(f"User with username '{user_name}' not found in get_user_with_role_and_group_names.")
        return {"error": f"User '{user_name}' not found."}

    def get_users_with_role_names_and_group_names(self) -> list[dict[str, Any]]:
        """
        Retrieves all users from Sisense and enriches them with role names and
        group names by resolving the raw role and group IDs via the roles and
        groups APIs.

        This uses the users API (with raw IDs), then looks up:
          - role names from `/api/roles`
          - group names from `/api/v1/groups`

        Returns:
            list[dict]: A list where each entry contains:
                - USER_ID
                - USER_NAME
                - FIRST_NAME
                - LAST_NAME
                - EMAIL
                - IS_ACTIVE
                - ROLE_ID
                - ROLE_NAME
                - GROUP_IDS
                - GROUP_NAMES
            If any API call fails, a single-item list with an ``error`` key is returned.
        """
        self.logger.debug("Fetching users with raw role/group IDs to enrich with names.")

        # Step 1: Fetch users (raw IDs)
        users_response = self.api_client.get("/api/v1/users")
        if not users_response or not users_response.ok:
            self.logger.error("Failed to retrieve users from API.")
            return [{"error": "Failed to retrieve users from API"}]

        try:
            users_raw = users_response.json()
        except Exception as exc:
            self.logger.exception("Failed to parse users response JSON.")
            return [{"error": f"Failed to parse users response JSON: {exc}"}]

        # Step 2/3: Build role and group mappings once
        mappings = self._build_role_and_group_mappings()
        if mappings is None:
            return [{"error": "Failed to build role and group mappings"}]

        roles_by_id = mappings["roles_by_id"]
        groups_by_id = mappings["groups_by_id"]

        # Step 4: Enrich each user with role/group names
        enriched_users: list[dict[str, Any]] = []

        for user in users_raw:
            if not isinstance(user, dict):
                self.logger.warning(f"Skipping unexpected user entry (not a dict): {user}")
                continue

            user_id = user.get("_id")
            role_id = user.get("roleId") or user.get("role", {}).get("_id")
            group_ids = user.get("groups") or []

            # Normalize groups to a list of IDs (in case full objects are returned)
            normalized_group_ids: list[str] = []
            for g in group_ids:
                gid = g.get("_id") if isinstance(g, dict) else g
                if gid:
                    normalized_group_ids.append(gid)

            role_name = roles_by_id.get(role_id, None)
            group_names = [groups_by_id.get(gid, gid) for gid in normalized_group_ids]

            enriched_users.append(
                {
                    "USER_ID": user_id,
                    "USER_NAME": user.get("userName"),
                    "FIRST_NAME": user.get("firstName"),
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user.get("email"),
                    "IS_ACTIVE": user.get("active"),
                    "ROLE_ID": role_id,
                    "ROLE_NAME": role_name,
                    "GROUP_IDS": normalized_group_ids,
                    "GROUP_NAMES": group_names,
                }
            )

        self.logger.info(f"Resolved users with role and group names. Total users processed: {len(enriched_users)}")
        return enriched_users

    def get_user(self, user_email: str) -> dict[str, Any]:
        """
        Retrieve a user's details by email address, expanding group and role information.

        This method fetches users with expanded ``groups`` and ``role`` data and then
        returns the record matching the provided email address.

        Parameters
        ----------
        user_email : str
            Email address of the user to retrieve.

        Returns
        -------
        dict[str, Any]
            User details on success. If the operation fails, returns a dictionary with an
            ``error`` key.
        """
        self.logger.debug("Getting user with email: %s", user_email)

        params = {"expand": "groups,role"}
        response = self.api_client.get("/api/v1/users", params=params)

        if not response or not response.ok:
            status = response.status_code if response else "No response"
            error_msg = f"Failed to retrieve users from API for email: {user_email}."
            self.logger.error("%s Status Code: %s", error_msg, status)
            return {"error": error_msg}

        try:
            users = response.json()
            self.logger.debug("Found %s users in the response.", len(users))
        except Exception as exc:
            self.logger.exception("Error decoding JSON response for user list.")
            return {"error": f"Failed to decode API response: {str(exc)}"}

        role_mapping = {
            "consumer": "viewer",
            "super": "sysAdmin",
            "contributor": "dashboardDesigner",
        }

        for user in users:
            try:
                self.logger.debug("Checking user: %s", user.get("email"))
                if user.get("email") == user_email:
                    self.logger.info("Found user: %s", user_email)
                    return {
                        "USER_ID": user["_id"],
                        "USER_NAME": user.get("userName", ""),
                        "FIRST_NAME": user.get("firstName", ""),
                        "LAST_NAME": user.get("lastName", ""),
                        "EMAIL": user.get("email", ""),
                        "IS_ACTIVE": user.get("active", False),
                        "ROLE_ID": user.get("role", {}).get("_id", ""),
                        "ROLE_NAME": role_mapping.get(
                            user.get("role", {}).get("name", ""),
                            user.get("role", {}).get("name", ""),
                        ),
                        "GROUPS": [g.get("name", "") for g in user.get("groups", [])],
                    }
            except Exception as exc:
                self.logger.exception(
                    "Error processing user object for email %s. Exception: %s",
                    user_email,
                    str(exc),
                )

        self.logger.warning("User with email '%s' not found.", user_email)
        return {"error": f"User '{user_email}' not found."}

    def get_my_user(self) -> dict[str, Any]:
        """Retrieve the currently logged-in user for the API token.

        Sends ``GET /api/users/loggedin``. Use this to resolve migration user
        identity (email, username, internal ID) for the authenticated admin
        token.

        Returns
        -------
        dict[str, Any]
            The logged-in user object from the API, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/users/loggedin"
        self.logger.debug("Fetching logged-in user identity.")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve logged-in user failed: No response received.")
            return {"error": "No response received while retrieving logged-in user."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve logged-in user. Error: {error_message}")
            return {"error": f"Failed to retrieve logged-in user. {error_message}"}

        user = response.json()
        self.logger.info("Successfully retrieved logged-in user identity.")
        return user

    def get_roles(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all Sisense roles.

        Sends ``GET /api/roles``. Returns the raw role list used to build role
        name-to-ID maps (for example in multi-tenant migration workflows).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of role objects on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/roles"
        self.logger.debug("Fetching roles from API.")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve roles failed: No response received.")
            return {"error": "No response received while retrieving roles."}

        if not response.ok:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve roles. Error: {error_message}")
            return {"error": f"Failed to retrieve roles. {error_message}"}

        roles = response.json()
        count = len(roles) if isinstance(roles, list) else 0
        self.logger.info(f"Successfully retrieved {count} roles.")
        return roles

    def change_user_password(self, user_id: str, password: str) -> dict[str, Any]:
        """Change a user's password.

        Sends ``PATCH /api/users/{user_id}`` with only ``password`` in the
        request body. Other user fields are not modified.

        Parameters
        ----------
        user_id : str
            Internal user ID (``_id``) to update.
        password : str
            New password for the user. Must not be empty.

        Returns
        -------
        dict[str, Any]
            The updated user object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not password:
            self.logger.error("Password change rejected: password must not be empty.")
            return {"error": "Password must not be empty."}

        endpoint = f"/api/users/{user_id}"
        self.logger.debug(f"Changing password for user ID {user_id}")
        response = self.api_client.patch(endpoint, data={"password": password})

        if response is None:
            self.logger.error(f"PATCH request to change password for user {user_id} failed: No response received.")
            return {"error": f"No response received while changing password for user ID '{user_id}'"}

        if not response.ok:
            try:
                error_message = response.json().get("error", "Unknown error")
            except Exception:
                error_message = "Unknown error"
            self.logger.error(f"Failed to change password for user {user_id}. Error: {error_message}")
            return {"error": error_message}

        try:
            response_data = response.json()
        except Exception:
            response_data = {"success": True}

        self.logger.info(f"Successfully changed password for user ID {user_id}.")
        return response_data

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
        params = {"expand": "groups,role"}

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
        ROLE_MAPPING = {"consumer": "viewer", "super": "sysAdmin", "contributor": "dashboardDesigner"}

        # Process the API response to build data_list
        for user in response_data:
            try:
                self.logger.debug(f"Processing user: {user['email']}")
                base_data = {
                    "USER_ID": user["_id"],
                    "USER_NAME": user["userName"],
                    "FIRST_NAME": user["firstName"],
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user["email"],
                    "IS_ACTIVE": user["active"],
                    "ROLE_ID": user["role"]["_id"],
                    "ROLE_NAME": ROLE_MAPPING.get(user["role"]["name"], user["role"]["name"]),
                    "GROUPS": [],
                }

                # Add all group names to the 'GROUPS' list
                if "groups" in user and user["groups"]:
                    base_data["GROUPS"] = [group["name"] for group in user["groups"]]
                if len(base_data["GROUPS"]) > 1 and "Everyone" in base_data["GROUPS"]:
                    base_data["GROUPS"].remove("Everyone")
                data_list.append(base_data)
                self.logger.debug(f"Successfully processed user: {user['email']}")
            except Exception as e:
                self.logger.exception(f"Error processing user {user.get('email', 'Unknown')}: {str(e)}")

        # Log the result and return the final data list
        if data_list:
            self.logger.info(f"Found {len(data_list)} users")
        else:
            self.logger.warning("No users found in the response")
            return [{"error": "No users found"}]

        return data_list

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
        role_alias_mapping = {"VIEWER": "CONSUMER", "DESIGNER": "CONTRIBUTOR"}

        # Convert the role name in the user_data to uppercase for
        # case-insensitive matching
        user_role = user_data.get("role", "").upper()
        mapped_role = role_alias_mapping.get(user_role, user_role)

        # Step 1: Fetch roles from the API
        role_response = self.api_client.get("/api/roles")
        if not role_response or not role_response.ok:
            self.logger.error("Failed to fetch roles from API")
            return {"error": "Failed to fetch roles from API"}

        roles_mapping = [{"id": role["_id"], "name": role["name"].upper()} for role in role_response.json()]
        self.logger.debug(f"Roles mapping: {roles_mapping}")

        # Step 2: Resolve roleId from role name
        for role in roles_mapping:
            if role["name"] == mapped_role:
                user_data["roleId"] = role["id"]
                break
        else:
            error_msg = f"Role '{user_data.get('role')}' not found in roles_mapping"
            self.logger.error(error_msg)
            return {"error": error_msg}

        user_data.pop("role", None)

        # Step 3: Resolve group IDs from group names (if provided)
        group_names = user_data.get("groups", [])
        if group_names:
            user_data["groups"] = [group.upper() for group in group_names]

            group_response = self.api_client.get("/api/v1/groups")
            if not group_response or not group_response.ok:
                self.logger.error("Failed to fetch groups from API")
                return {"error": "Failed to fetch groups from API"}

            groups_mapping = [{"id": group["_id"], "name": group["name"].upper()} for group in group_response.json()]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    error_msg = f"Group '{group_name}' not found in groups_mapping"
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
                error_message = error_json["error"].get("message", str(error_json["error"])) if isinstance(error_json, dict) and "error" in error_json else error_json.get("error", str(error_json))
            except Exception:
                error_message = "No response body or invalid JSON"

            self.logger.error(f"Failed to create user. Error: {error_message}")
            return {"error": error_message}

    def update_user(self, user_email: str, user_data: dict[str, Any]) -> dict[str, Any]:
        """
        Update an existing Sisense user identified by their email address.

        This method finds the user by email and performs a partial update (PATCH).
        All update fields MUST be provided inside the ``user_data`` dictionary. Do not
        pass update fields at the top level.

        Parameters
        ----------
        user_email : str
            Email address of the user to update (used to locate the user).
        user_data : dict[str, Any]
            Dictionary of fields to update. Only include fields you want to change.

            Supported fields
            ----------------
            - email : str
                Update the user's email address.
            - userName : str
                Update the user's username/login name.
            - firstName : str
                Update the user's first name.
            - lastName : str
                Update the user's last name.
            - role : str
                Role name (e.g., "viewer", "designer"). This is resolved to ``roleId`` before
                sending the API request.
            - groups : list[str]
                List of group names to apply. Group names are resolved to group IDs before
                sending the API request. If ``groups`` is explicitly provided as an empty
                list (``[]``), group memberships are cleared (tenant defaults may still apply).

        Returns
        -------
        dict[str, Any]
            The updated user payload when successful. If the operation fails, returns a
            dictionary with an ``error`` key.
        """
        self.logger.debug("Updating user with email: %s", user_email)

        user = self.get_user(user_email)
        if not user:
            self.logger.error("User with email '%s' not found.", user_email)
            return {"error": f"User with email '{user_email}' not found."}

        role_alias_mapping = {
            "VIEWER": "CONSUMER",
            "DESIGNER": "CONTRIBUTOR",
        }

        # Step 1: Resolve role if provided
        if "role" in user_data:
            user_role = str(user_data["role"]).upper()
            mapped_role = role_alias_mapping.get(user_role, user_role)

            role_response = self.api_client.get("/api/roles")
            if not role_response or not role_response.ok:
                status = role_response.status_code if role_response else "No response"
                self.logger.error(
                    "Failed to fetch roles from API. Status Code: %s",
                    status,
                )
                return {"error": "Failed to fetch roles from API."}

            roles_mapping = [{"id": role["_id"], "name": str(role["name"]).upper()} for role in role_response.json()]
            self.logger.debug("Roles mapping: %s", roles_mapping)

            for role in roles_mapping:
                if role["name"] == mapped_role:
                    user_data["roleId"] = role["id"]
                    break
            else:
                error_msg = f"Role '{user_data['role']}' not found in roles_mapping"
                self.logger.error(error_msg)
                return {"error": error_msg}

            user_data.pop("role", None)

        # Step 2: Resolve groups only if explicitly provided
        if "groups" in user_data:
            group_names = user_data.get("groups") or []

            # If caller explicitly passed an empty list, they intend to clear groups
            if not group_names:
                user_data["groups"] = []
            else:
                normalized_group_names = [str(g).upper() for g in group_names]

                group_response = self.api_client.get("/api/v1/groups")
                if not group_response or not group_response.ok:
                    status = group_response.status_code if group_response else "No response"
                    self.logger.error(
                        "Failed to fetch groups from API. Status Code: %s",
                        status,
                    )
                    return {"error": "Failed to fetch groups from API."}

                groups_mapping = [{"id": group["_id"], "name": str(group["name"]).upper()} for group in group_response.json()]
                self.logger.debug("Groups mapping: %s", groups_mapping)

                updated_groups = []
                for group_name in normalized_group_names:
                    for group in groups_mapping:
                        if group["name"] == group_name:
                            updated_groups.append(group["id"])
                            break
                    else:
                        error_msg = f"Group '{group_name}' not found in groups_mapping"
                        self.logger.error(error_msg)
                        return {"error": error_msg}

                user_data["groups"] = updated_groups

        self.logger.debug("Final updated user data for API call: %s", user_data)
        response = self.api_client.patch(
            f"/api/v1/users/{user['USER_ID']}",
            data=user_data,
        )

        if response and response.ok:
            response_data = response.json()
            self.logger.info("User updated successfully: %s", response_data)
            return response_data

        error_message = response.json().get("error", "Unknown error") if response else "No response received"
        self.logger.error("Failed to update user. Error: %s", error_message)
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
        self.logger.debug(f"Starting 'delete_user' method for username: {user_name}")

        # Reuse the get_user method to fetch user details
        self.logger.debug(f"Fetching user details for '{user_name}' using 'get_user' method.")
        user = self.get_user(user_name)
        self.logger.debug(f"User details fetched: {user}")

        # If user is not found, log and return error
        if not user or "error" in user:
            error_msg = f"User '{user_name}' not found. Cannot proceed with deletion."
            self.logger.error(error_msg)
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"error": error_msg}
        # support both formats just in case
        user_id = user.get("_id") or user.get("USER_ID")
        if not user_id:
            self.logger.error(f"User object for '{user_name}' is missing ID field. Cannot proceed.")
            return {"error": (f"User '{user_name}' found but no ID field present.")}

        self.logger.debug(f"User '{user_name}' found. Proceeding to delete user with ID: {user_id}")

        # Send the DELETE request
        response = self.api_client.delete(f"/api/v1/users/{user['USER_ID']}")

        if response and response.status_code == 204:
            self.logger.info(f"User '{user_name}' (ID: {user['USER_ID']}) deleted. No content returned.")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"message": "User deleted successfully."}

        elif response and response.ok:
            try:
                response_data = response.json()
            except Exception:
                response_data = {"message": "User deleted, but no JSON body returned."}
            self.logger.info(f"User '{user_name}' (ID: {user['USER_ID']}) deleted.")
            self.logger.debug(f"API response: {response_data}")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return response_data

        else:
            try:
                error_message = response.json().get("error", "Unknown error")
            except Exception:
                error_message = "No response body or invalid JSON"
            self.logger.error(f"Failed to delete user '{user_name}' (ID: {user['USER_ID']}). Error: {error_message}")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"error": error_message}
