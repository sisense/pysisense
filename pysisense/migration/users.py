from __future__ import annotations

from collections.abc import Callable
from typing import Any


class UsersMigrationMixin:
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
        params = {"expand": "groups,role"}

        # Step 1: Get all users from the source environment
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve users from the source environment.")
            return [{"message": ("Failed to retrieve users from the source environment. Please check the logs for more details.")}]
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
            return [{"message": ("Failed to retrieve roles from the target environment. Please check the logs for details.")}]

        if not target_groups_response or target_groups_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the target environment.")
            return [{"message": ("Failed to retrieve groups from the target environment. Please check the logs for details.")}]

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
                    "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),
                    "groups": [group["_id"] for group in target_groups if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS],
                    "preferences": user.get("preferences", {"localeId": "en-US"}),  # Default to English language.
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
        status_code = response.status_code if response else "No response"
        self.logger.debug(f"Target environment response status code: {status_code}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 5: Early exit if response is missing or empty
        if response is None:
            self.logger.error("No response received from the migration API.")
            return {"results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data], "raw_error": "No response received from the migration API."}
        elif not response.text.strip():
            self.logger.error(f"Empty response body received. Status code: {response.status_code}")
            return {"results": [{"name": user["email"], "status": "Failed"} for user in bulk_user_data], "raw_error": f"Empty response body. Status code: {response.status_code}"}

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
        self.logger.info(f"Finished migrating users. Successfully migrated {success_count} out of {len(bulk_user_data)} users.")

        # Step 8: Return structured result
        return {"results": migration_results, "total_count": len(bulk_user_data), "raw_error": raw_error}

    def migrate_all_users(
        self,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """
        Migrate all eligible users from the source environment to the target environment using the bulk endpoint.

        This method is designed to support MCP-style streaming by optionally emitting structured
        progress events via the ``emit`` callback. It remains synchronous, but callers can run it
        in a worker thread and forward events to an event stream.

        The migration performs the following steps:
        1) Fetch users from the source environment (expanded with groups and role).
        2) Fetch roles and groups from the target environment for ID mapping.
        3) Build a bulk payload by mapping role names to role IDs and group names to group IDs.
        4) Submit the payload to the target bulk endpoint.
        5) Return a structured summary and per-user status list.

        Parameters
        ----------
        emit : Callable[[Dict[str, Any]], None], optional
            Optional callback invoked with structured progress events. If not provided, the method
            emits no events and only returns a final result.

            Event payloads follow a consistent shape:
            - ``type``: str ("started" | "progress" | "warning" | "error" | "completed")
            - ``step``: str logical step identifier
            - ``message``: str human-readable message
            - Additional fields depending on the step (counts, status_code, etc.)

        Returns
        -------
        Dict[str, Any]
            Structured result payload:
            - ``ok``: bool
            - ``status``: str ("success" | "failed" | "noop")
            - ``results``: List[Dict[str, str]] per-user statuses (name=email, status)
            - ``source_count``: int number of users retrieved from source
            - ``eligible_count``: int number of users included in the bulk payload
            - ``skipped_super_count``: int number of sysadmin users skipped
            - ``missing_role_mappings_count``: int number of users with unresolved role mapping
            - ``missing_group_mappings_count``: int number of group memberships not found in target
            - ``success_count``: int
            - ``failed_count``: int
            - ``raw_error``: Any error payload if the bulk request fails, else None
            - ``warnings``: List[str]
        """
        warnings: list[str] = []
        migration_results: list[dict[str, str]] = []
        raw_error: Any = None

        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full user migration from source to target."})
        self.logger.info("Starting full user migration from source to target.")

        # Query parameters to expand group and role information
        params: dict[str, str] = {"expand": "groups,role"}

        # Step 1: Get all users from the source environment
        self._emit(emit, {"type": "progress", "step": "fetch_source_users", "message": "Fetching users from the source environment."})
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)

        if not source_response or source_response.status_code != 200:
            status_code = self._safe_status_code(source_response)
            raw_error = self._safe_error_payload(source_response, context="fetch_source_users")
            self.logger.error("Failed to retrieve users from the source environment.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_source_users",
                    "message": "Failed to retrieve users from the source environment.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )
            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "Failed to retrieve users from the source environment. Please check logs."}],
                "source_count": 0,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        self.logger.debug("Source environment response status code: %s", source_response.status_code)
        self.logger.debug("Source environment response body: %s", source_response.text)

        source_users = source_response.json() or []
        source_count = len(source_users)

        if source_count == 0:
            self.logger.info("No users found in the source environment. Ending process.")
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "fetch_source_users",
                    "message": "No users found in the source environment. Nothing to migrate.",
                    "status": "noop",
                    "source_count": 0,
                },
            )
            return {
                "ok": True,
                "status": "noop",
                "results": [{"message": "No users found in the source environment. Nothing to migrate."}],
                "source_count": 0,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": None,
                "warnings": warnings,
            }

        self.logger.info("Retrieved %s users from the source environment.", source_count)
        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_source_users",
                "message": "Retrieved users from the source environment.",
                "source_count": source_count,
            },
        )

        # Step 1.5: Resolve the system tenant id (multi-tenant safe filtering)
        self._emit(emit, {"type": "progress", "step": "fetch_system_tenant", "message": "Fetching tenants from the source environment to resolve system tenant."})
        self.logger.debug("Fetching tenants from the source environment.")
        tenants_response = self.source_client.get("/api/v1/tenants")

        if not tenants_response or tenants_response.status_code != 200:
            status_code = self._safe_status_code(tenants_response)
            raw_error = self._safe_error_payload(tenants_response, context="fetch_system_tenant")
            self.logger.error("Failed to retrieve tenants from the source environment.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_system_tenant",
                    "message": "Failed to retrieve tenants from the source environment.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )
            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "Failed to retrieve tenants from the source environment. Please check logs."}],
                "source_count": source_count,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        tenants = tenants_response.json() or []
        system_tenant_id: str | None = None
        for t in tenants:
            if isinstance(t, dict) and t.get("name") == "system":
                system_tenant_id = t.get("_id")
                break

        if not system_tenant_id:
            raw_error = {
                "message": "System tenant was not found in /api/v1/tenants response.",
                "hint": "Expected a tenant object with name='system'.",
            }
            self.logger.error("System tenant not found in source tenants response.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_system_tenant",
                    "message": "System tenant not found in source environment.",
                    "status_code": self._safe_status_code(tenants_response),
                    "raw_error": raw_error,
                },
            )
            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "System tenant not found in source environment. Please check logs."}],
                "source_count": source_count,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_system_tenant",
                "message": "Resolved system tenant id from source environment.",
                "system_tenant_id": system_tenant_id,
            },
        )

        # Step 2: Get roles and groups from the target for ID mapping
        self._emit(emit, {"type": "progress", "step": "fetch_target_mappings", "message": "Fetching roles and groups from the target environment."})
        self.logger.debug("Fetching roles and groups from the target environment.")
        target_roles_response = self.target_client.get("/api/roles")
        target_groups_response = self.target_client.get("/api/v1/groups")

        if not target_roles_response or target_roles_response.status_code != 200:
            status_code = self._safe_status_code(target_roles_response)
            raw_error = self._safe_error_payload(target_roles_response, context="fetch_target_roles")
            self.logger.error("Failed to retrieve roles from the target environment.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_target_roles",
                    "message": "Failed to retrieve roles from the target environment.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )
            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "Failed to retrieve roles from the target environment. Please check logs."}],
                "source_count": source_count,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        if not target_groups_response or target_groups_response.status_code != 200:
            status_code = self._safe_status_code(target_groups_response)
            raw_error = self._safe_error_payload(target_groups_response, context="fetch_target_groups")
            self.logger.error("Failed to retrieve groups from the target environment.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_target_groups",
                    "message": "Failed to retrieve groups from the target environment.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )
            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "Failed to retrieve groups from the target environment. Please check logs."}],
                "source_count": source_count,
                "eligible_count": 0,
                "skipped_super_count": 0,
                "skipped_multi_tenant_count": 0,
                "missing_role_mappings_count": 0,
                "missing_group_mappings_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        target_roles = target_roles_response.json() or []
        target_groups = target_groups_response.json() or []

        self.logger.debug("Retrieved %s roles from the target environment.", len(target_roles))
        self.logger.debug("Retrieved %s groups from the target environment.", len(target_groups))

        # Build mapping dicts for faster lookups (and deterministic behavior)
        role_name_to_id: dict[str, Any] = {}
        for r in target_roles:
            name = r.get("name")
            rid = r.get("_id")
            if name and rid:
                role_name_to_id[name] = rid

        group_name_to_id: dict[str, Any] = {}
        for g in target_groups:
            name = g.get("name")
            gid = g.get("_id")
            if name and gid:
                group_name_to_id[name] = gid

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_target_mappings",
                "message": "Loaded role and group mappings from target environment.",
                "roles_count": len(role_name_to_id),
                "groups_count": len(group_name_to_id),
            },
        )

        # Step 3: Build payload
        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        bulk_user_data: list[dict[str, Any]] = []

        skipped_super_count = 0
        skipped_multi_tenant_count = 0
        missing_role_mappings_count = 0
        missing_group_mappings_count = 0

        self._emit(emit, {"type": "progress", "step": "build_payload", "message": "Preparing bulk user payload."})

        for user in source_users:
            tenant_id = user.get("tenantId")
            if tenant_id != system_tenant_id:
                self.logger.debug("Skipping multi-tenant user: %s", user.get("email"))
                skipped_multi_tenant_count += 1
                continue

            role = user.get("role") or {}
            role_name = role.get("name")

            if role_name == "super":
                skipped_super_count += 1
                continue

            email = user.get("email")
            first_name = user.get("firstName")
            if not email or not first_name:
                # Keep behavior: skip badly-formed records, but warn.
                warnings.append("Skipped a user record with missing required fields (email/firstName).")
                continue

            role_id = role_name_to_id.get(role_name)
            if not role_id:
                missing_role_mappings_count += 1

            user_group_names: list[str] = []
            try:
                user_group_names = [g.get("name") for g in (user.get("groups") or []) if g and g.get("name")]
            except Exception:
                user_group_names = []

            mapped_group_ids: list[Any] = []
            for gname in user_group_names:
                if gname in EXCLUDED_GROUPS:
                    continue
                gid = group_name_to_id.get(gname)
                if gid:
                    mapped_group_ids.append(gid)
                else:
                    missing_group_mappings_count += 1

            user_data = {
                "email": email,
                "firstName": first_name,
                "lastName": user.get("lastName", ""),
                "roleId": role_id,
                "groups": mapped_group_ids,
                "preferences": user.get("preferences", {"localeId": "en-US"}),
            }
            bulk_user_data.append(user_data)
            self.logger.debug("Prepared data for user: %s", email)

        eligible_count = len(bulk_user_data)

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "build_payload",
                "message": "Prepared bulk user payload.",
                "eligible_count": eligible_count,
                "skipped_super_count": skipped_super_count,
                "skipped_multi_tenant_count": skipped_multi_tenant_count,
                "missing_role_mappings_count": missing_role_mappings_count,
                "missing_group_mappings_count": missing_group_mappings_count,
            },
        )

        if eligible_count == 0:
            self.logger.info("No users to migrate. Ending process.")
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "build_payload",
                    "message": "No eligible users to migrate. Nothing to process.",
                    "status": "noop",
                    "source_count": source_count,
                    "eligible_count": 0,
                    "skipped_super_count": skipped_super_count,
                    "skipped_multi_tenant_count": skipped_multi_tenant_count,
                },
            )
            return {
                "ok": True,
                "status": "noop",
                "results": [],
                "source_count": source_count,
                "eligible_count": 0,
                "skipped_super_count": skipped_super_count,
                "skipped_multi_tenant_count": skipped_multi_tenant_count,
                "missing_role_mappings_count": missing_role_mappings_count,
                "missing_group_mappings_count": missing_group_mappings_count,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": None,
                "warnings": warnings,
            }

        # Step 4: POST bulk users
        self.logger.info("Sending bulk migration request for %s users", eligible_count)
        self.logger.debug("Payload for bulk user migration: %s", bulk_user_data)

        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Sending bulk user migration request.", "eligible_count": eligible_count})
        response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)

        status_code = self._safe_status_code(response)
        self.logger.debug("Target environment response status code: %s", status_code if status_code is not None else "No response")
        self.logger.debug("Target environment response body: %s", response.text if response is not None and hasattr(response, "text") else "No response body")

        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Received response from target bulk endpoint.", "status_code": status_code})

        # Step 5: Process response
        if response is not None and status_code == 201:
            try:
                response_data = response.json()
                self.logger.info("Bulk user migration succeeded.")
                self._emit(emit, {"type": "progress", "step": "process_response", "message": "Processing bulk user migration response.", "status_code": status_code})

                for u in response_data:
                    user_email = u.get("email", "Unknown User")
                    migration_results.append({"name": user_email, "status": "Success"})
            except Exception:
                warn = "Bulk response was not valid JSON; assuming migration succeeded based on status code."
                warnings.append(warn)
                self.logger.warning(warn)
                self._emit(emit, {"type": "warning", "step": "process_response", "message": warn})
                migration_results = [{"name": u.get("email", "Unknown User"), "status": "Success"} for u in bulk_user_data]
        else:
            raw_error = self._safe_error_payload(response, context="bulk_post_users")
            self.logger.error(
                "Bulk user migration failed. Status code: %s",
                status_code if status_code is not None else "No response",
            )
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "bulk_post",
                    "message": "Bulk user migration failed.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )
            migration_results = [{"name": u.get("email", "Unknown User"), "status": "Failed"} for u in bulk_user_data]

        success_count = sum(1 for r in migration_results if r.get("status") == "Success")
        failed_count = sum(1 for r in migration_results if r.get("status") == "Failed")

        ok = (eligible_count > 0) and (success_count == eligible_count)
        status = "success" if ok else "failed"

        self.logger.info(
            "Finished migrating users. Successfully migrated %s out of %s users.",
            success_count,
            eligible_count,
        )

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished user migration.",
                "status": status,
                "source_count": source_count,
                "eligible_count": eligible_count,
                "skipped_super_count": skipped_super_count,
                "skipped_multi_tenant_count": skipped_multi_tenant_count,
                "missing_role_mappings_count": missing_role_mappings_count,
                "missing_group_mappings_count": missing_group_mappings_count,
                "success_count": success_count,
                "failed_count": failed_count,
                "warnings_count": len(warnings),
            },
        )

        return {
            "ok": ok,
            "status": status,
            "results": migration_results,
            "source_count": source_count,
            "eligible_count": eligible_count,
            "skipped_super_count": skipped_super_count,
            "skipped_multi_tenant_count": skipped_multi_tenant_count,
            "missing_role_mappings_count": missing_role_mappings_count,
            "missing_group_mappings_count": missing_group_mappings_count,
            "success_count": success_count,
            "failed_count": failed_count,
            "raw_error": raw_error,
            "warnings": warnings,
        }
