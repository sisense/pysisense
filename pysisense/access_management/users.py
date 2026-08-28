from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class UsersMixin:
    def _fetch_expanded_users(self) -> Any:
        """Fetch the users list with ``groups`` and ``role`` expanded; returns the raw response."""
        return self.api_client.get("/api/v1/users", params={"expand": "groups,role"})

    def _map_user_role_and_groups(self, user: dict[str, Any], apply_role_alias: bool = True) -> tuple[str | None, str | None, list[str], list[str]]:
        """Resolve a single expanded user's role/group IDs and names.

        Parameters
        ----------
        user : dict[str, Any]
            An expanded user object (``role`` and ``groups`` as objects, not
            raw IDs).
        apply_role_alias : bool, optional
            When ``True`` (default), the raw Sisense role name (e.g.
            ``"consumer"``) is mapped to its public alias (``"viewer"``).
            When ``False``, the raw role name is returned unchanged.

        Returns ``(role_id, role_name, group_ids, group_names)``.
        """
        role_mapping = {
            "consumer": "viewer",
            "super": "sysAdmin",
            "contributor": "dashboardDesigner",
        }

        role_obj = user.get("role") or {}
        groups_obj = user.get("groups") or []

        role_id = role_obj.get("_id")
        role_name_raw = role_obj.get("name")
        role_name = role_mapping.get(role_name_raw, role_name_raw) if apply_role_alias else role_name_raw

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

        return role_id, role_name, group_ids, group_names

    def get_user_with_role_and_group_names(self, user_name: str) -> dict[str, Any]:
        """Retrieve a single user by email/username with role and group details.

        Fetches the expanded users list and returns the matching user enriched
        with both the role and group IDs and their resolved names.

        Parameters
        ----------
        user_name : str
            The email or username of the user to be retrieved. (format: email)

        Returns
        -------
        dict[str, Any]
            User details including ``USER_ID``, ``USER_NAME``, ``FIRST_NAME``,
            ``LAST_NAME``, ``EMAIL``, ``IS_ACTIVE``, ``ROLE_ID``, ``ROLE_NAME``,
            ``GROUP_IDS`` (list of group IDs), and ``GROUP_NAMES`` (list of group
            names), or ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Getting user with role and group IDs/names for: {user_name}")

        # Reuse expanded users endpoint to get role & group objects
        response = self._fetch_expanded_users()

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to retrieve users from API for username: {user_name}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            users = response.json()
        except Exception as exc:
            self.logger.exception("Error decoding JSON response for user list in get_user_with_role_and_group_names.")
            return {"error": f"Failed to decode API response: {exc}"}

        for user in users:
            try:
                if user.get("email") != user_name:
                    continue

                role_id, role_name, group_ids, group_names = self._map_user_role_and_groups(user)

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
        """Retrieve all users enriched with role names and group names.

        Fetches users with ``groups`` and ``role`` expanded in a single call
        and resolves each user's role/group IDs and names from the expanded
        objects. Role names are returned exactly as stored by Sisense (e.g.
        ``"consumer"``), not the public alias — use
        ``get_user_with_role_and_group_names`` for a single user if the
        aliased name (``"viewer"``) is needed instead.

        Returns
        -------
        list[dict[str, Any]]
            A list where each entry contains ``USER_ID``, ``USER_NAME``,
            ``FIRST_NAME``, ``LAST_NAME``, ``EMAIL``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME``, ``GROUP_IDS``, and ``GROUP_NAMES``. If any API call
            fails, a single-item list with an ``error`` key is returned.
        """
        self.logger.debug("Fetching users with expanded role/group objects to enrich with names.")

        response = self._fetch_expanded_users()
        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve users from API", self.api_client)
            self.logger.error(failure["error"])
            return [failure]

        try:
            users_raw = response.json()
        except Exception as exc:
            self.logger.exception("Failed to parse users response JSON.")
            return [{"error": f"Failed to parse users response JSON: {exc}"}]

        enriched_users: list[dict[str, Any]] = []

        for user in users_raw:
            if not isinstance(user, dict):
                self.logger.warning(f"Skipping unexpected user entry (not a dict): {user}")
                continue

            role_id, role_name, group_ids, group_names = self._map_user_role_and_groups(user, apply_role_alias=False)

            enriched_users.append(
                {
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
            )

        self.logger.info(f"Resolved users with role and group names. Total users processed: {len(enriched_users)}")
        return enriched_users

    def get_users_expanded(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all users with raw, unmodified role and group objects.

        Fetches ``GET /api/v1/users`` with ``groups`` and ``role`` expanded.
        Unlike ``get_users_all`` and ``get_user_with_role_and_group_names``,
        role and group names are returned exactly as stored (no display-name
        aliasing), which is required when resolving role/group mappings
        across two separate Sisense environments.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The raw list of user objects, or ``{"error": "..."}`` if
            retrieval fails.
        """
        self.logger.debug("Starting 'get_users_expanded' method.")

        response = self._fetch_expanded_users()

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve users", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            users = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse users response JSON.")
            return {"error": f"Failed to parse users response JSON: {str(e)}"}

        self.logger.debug(f"Retrieved {len(users or [])} user(s).")
        return users or []

    def create_users_bulk(self, users: list[dict[str, Any]]) -> list[dict[str, Any]] | dict[str, Any]:
        """Create multiple users in a single bulk request.

        Sends the provided user definitions to the bulk user creation
        endpoint. Each entry must already carry a resolved ``roleId`` and
        ``groups`` (list of group IDs) — no name-to-ID resolution is
        performed by this method.

        Parameters
        ----------
        users : list[dict[str, Any]]
            User definitions to create. Each dictionary should use canonical
            Sisense user fields (at minimum ``email``, ``firstName``, and
            ``roleId``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The list of created user objects on success, or
            ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Starting 'create_users_bulk' method for {len(users)} user(s).")

        response = self.api_client.post("/api/v1/users/bulk", data=users)

        if response is None:
            self.logger.error("No response received while creating users in bulk.")
            return {"error": "No response received while creating users in bulk."}

        if response.status_code != 201:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "Unknown error"
            self.logger.error(f"Failed to create users in bulk. Error: {error_message}")
            return {"error": f"Failed to create users in bulk. {error_message}"}

        try:
            created_users = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse bulk user creation response JSON.")
            return {"error": f"Failed to parse bulk user creation response JSON: {str(e)}"}

        self.logger.info(f"Successfully created {len(created_users or [])} user(s).")
        return created_users or []

    def get_user(self, user_email: str) -> dict[str, Any]:
        """
        Retrieve a user's details by email address, expanding group and role information.

        This method fetches users with expanded ``groups`` and ``role`` data and then
        returns the record matching the provided email address.

        Parameters
        ----------
        user_email : str
            Email address of the user to retrieve. (format: email)

        Returns
        -------
        dict[str, Any]
            User details on success. If the operation fails, returns a dictionary with an
            ``error`` key.
        """
        self.logger.debug("Getting user with email: %s", user_email)

        response = self._fetch_expanded_users()

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to retrieve users from API for email: {user_email}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            users = response.json()
            self.logger.debug("Found %s users in the response.", len(users))
        except Exception as exc:
            self.logger.exception("Error decoding JSON response for user list.")
            return {"error": f"Failed to decode API response: {str(exc)}"}

        for user in users:
            try:
                self.logger.debug("Checking user: %s", user.get("email"))
                if user.get("email") == user_email:
                    self.logger.info("Found user: %s", user_email)
                    role_id, role_name, _, _ = self._map_user_role_and_groups(user)
                    return {
                        "USER_ID": user["_id"],
                        "USER_NAME": user.get("userName", ""),
                        "FIRST_NAME": user.get("firstName", ""),
                        "LAST_NAME": user.get("lastName", ""),
                        "EMAIL": user.get("email", ""),
                        "IS_ACTIVE": user.get("active", False),
                        "ROLE_ID": role_id or "",
                        "ROLE_NAME": role_name or "",
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

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to retrieve logged-in user", self.api_client)
            self.logger.error(failure["error"])
            return failure

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

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve roles", self.api_client)
            self.logger.error(failure["error"])
            return failure

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

    def get_users_all(self) -> list[dict[str, Any]]:
        """Retrieve all users with group and role information.

        Retrieves user details along with group and role information. Removes
        the "Everyone" group from users if they belong to other groups, but
        keeps the "Everyone" group if it is the only group the user belongs to.

        Returns
        -------
        list[dict[str, Any]]
            List of user details dicts, or ``[{"error": "..."}]`` if retrieval
            fails.
        """
        self.logger.debug("Getting all users")

        # Fetch user data from the API with group and role info expanded
        response = self._fetch_expanded_users()

        # Check if the API request failed
        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve users from API", self.api_client)
            self.logger.error(failure["error"])
            return [failure]

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse user response JSON.")
            return [{"error": f"Failed to parse user response: {str(e)}"}]

        # Initialize list to store user information
        data_list = []

        # Process the API response to build data_list
        for user in response_data:
            try:
                self.logger.debug(f"Processing user: {user['email']}")
                role_id, role_name, _, _ = self._map_user_role_and_groups(user)
                if role_id is None or role_name is None:
                    # Preserve the original KeyError-on-missing-role behavior: a user
                    # with no role, or a role missing "_id"/"name", is skipped below.
                    raise KeyError("role")
                base_data = {
                    "USER_ID": user["_id"],
                    "USER_NAME": user["userName"],
                    "FIRST_NAME": user["firstName"],
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user["email"],
                    "IS_ACTIVE": user["active"],
                    "ROLE_ID": role_id,
                    "ROLE_NAME": role_name,
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

    def create_user(self, user_data: dict[str, Any]) -> dict[str, Any]:
        """Create a new user in Sisense.

        Processes the provided user data to resolve the role name and group
        names to their corresponding IDs, then sends a POST request to create
        the user. The ``role`` field is matched case-insensitively (with
        ``"VIEWER"`` mapped to ``"CONSUMER"`` and ``"DESIGNER"`` to
        ``"CONTRIBUTOR"``) and replaced with the resolved ``roleId``; group
        names in ``groups`` are resolved to group IDs.

        Parameters
        ----------
        user_data : dict[str, Any]
            Dictionary containing the user details. Supported fields use
            canonical Sisense payload field names:

            - ``email`` : str — the user's email address.
            - ``firstName`` : str — the user's first name.
            - ``lastName`` : str — the user's last name.
            - ``role`` : str — role name to assign (resolved to ``roleId``).
            - ``groups`` : list[str] — group names to assign (resolved to IDs).
            - ``preferences`` : dict — user preference settings.

        Returns
        -------
        dict[str, Any]
            The created user object returned by the API if successful, or a
            dictionary with an ``error`` key if the operation fails.
        """
        self.logger.debug(f"Creating user with data: {user_data}")

        # Custom role mapping
        role_alias_mapping = {"VIEWER": "CONSUMER", "DESIGNER": "CONTRIBUTOR"}

        # Convert the role name in the user_data to uppercase for
        # case-insensitive matching
        user_role = str(user_data.get("role", "")).upper()
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
            Email address of the user to update (used to locate the user). (format: email)
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

        failure = _extract_error_message(response, "Failed to update user", self.api_client)
        self.logger.error(failure["error"])
        return failure

    def delete_user(self, user_name: str) -> dict[str, Any]:
        """Delete a user by their email (username).

        Resolves the user by email/username and sends a DELETE request to remove
        the account.

        Parameters
        ----------
        user_name : str
            The email or username of the user to be deleted. (format: email)

        Returns
        -------
        dict[str, Any]
            A success message dict from the API if successful, or an
            ``{"error": "..."}`` dict on failure.
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
