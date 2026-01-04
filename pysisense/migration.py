from typing import Any, Callable, Dict, List, Optional, Literal, Tuple, Union

from .sisenseclient import SisenseClient
import time


class Migration:
    def __init__(
        self,
        source_yaml: Optional[str] = None,
        target_yaml: Optional[str] = None,
        debug: bool = False,
        *,
        source_client: Optional[SisenseClient] = None,
        target_client: Optional[SisenseClient] = None,
    ):
        """
        Initializes the Migration class with API clients for both source and
        target environments.

        Supported patterns:

        1) YAML-based (existing behavior, fully supported):
            migration = Migration(
                source_yaml="source.yaml",
                target_yaml="target.yaml",
                debug=False,
            )

        2) Client-based (for agent app / inline connections):
            src_client = SisenseClient.from_connection(
                domain="https://source.sisense.com",
                token="SRC_TOKEN",
                is_ssl=True,
                debug=True,
            )
            tgt_client = SisenseClient.from_connection(
                domain="https://target.sisense.com",
                token="TGT_TOKEN",
                is_ssl=True,
                debug=True,
            )
            migration = Migration(
                source_client=src_client,
                target_client=tgt_client,
                debug=True,
            )

        Exactly one mode must be provided:
        - Either both source_client and target_client
        - Or both source_yaml and target_yaml
        """
        # Prefer explicit clients if provided (agent / runtime connections)
        if source_client is not None and target_client is not None:
            self.source_client = source_client
            self.target_client = target_client

        # Otherwise fall back to YAML-based configuration (legacy / scripts)
        elif source_yaml is not None and target_yaml is not None:
            self.source_client = SisenseClient(
                config_file=source_yaml,
                debug=debug,
            )
            self.target_client = SisenseClient(
                config_file=target_yaml,
                debug=debug,
            )

        else:
            raise ValueError(
                "Migration requires either (source_client and target_client) "
                "OR (source_yaml and target_yaml)."
            )

        # Use the logger from the source client for consistency
        self.logger = self.source_client.logger

    # -------------------------------------------------------------------------
    # Shared helpers (for all migration methods)
    # -------------------------------------------------------------------------

    def _emit(self, emit: Optional[Callable[[Dict[str, Any]], None]], event: Dict[str, Any],) -> None:
        """
        Safely emit a progress event to the provided callback.

        Parameters
        ----------
        emit : Callable[[Dict[str, Any]], None] or None
            Callback provided by the caller. If None, emission is a no-op.
        event : Dict[str, Any]
            Event payload to send.
        """
        if emit is None:
            return
        try:
            emit(event)
        except Exception:
            # Never let progress reporting break the actual migration.
            self.logger.debug("Progress emitter raised; ignoring.", exc_info=True)

    def _safe_status_code(self, resp: Any) -> Optional[int]:
        """
        Safely extract an HTTP status code from a response-like object.
        """
        try:
            return int(getattr(resp, "status_code"))
        except Exception:
            return None

    def _truncate(self, text: str, limit: int = 500) -> str:
        if text is None:
            return ""
        return text if len(text) <= limit else (text[:limit] + "...")

    def _safe_json(self, resp: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Returns (json_dict, error_reason).
        """
        if not resp:
            return None, "No response from server"
        try:
            return resp.json(), None
        except Exception:
            return None, f"Non-JSON response: {self._truncate(getattr(resp, 'text', '') or '')}"

    def _safe_error_payload(self, resp: Any, *, context: str) -> Any:
        """
        Best-effort extraction of an error payload.

        Parameters
        ----------
        resp : Any
            Response-like object (typically requests.Response) or None.
        context : str
            Short string to identify where this extraction was triggered (used in payload).

        Returns
        -------
        Any
            Parsed JSON payload if available, else response text, else a helpful dict.
            This function never returns None.
        """
        if resp is None:
            return {
                "message": "No response object returned by the HTTP client.",
                "context": context,
                "hint": "This usually means the HTTP client returned None on non-2xx or hit an exception. Check client logs.",
            }

        try:
            return resp.json()
        except Exception:
            pass

        try:
            txt = getattr(resp, "text", None)
            if txt:
                return txt
        except Exception:
            pass

        return {"message": "Failed to extract error payload from response.", "context": context}

    def _extract_error_detail(self, resp: Any) -> str:
        payload, err = self._safe_json(resp)
        if err:
            return err
        if isinstance(payload, dict):
            if isinstance(payload.get("detail"), str):
                return payload["detail"]
            if isinstance(payload.get("message"), str):
                return payload["message"]
            if isinstance(payload.get("error"), dict) and isinstance(payload["error"].get("message"), str):
                return payload["error"]["message"]
            if isinstance(payload.get("title"), str):
                return payload["title"]
        return self._truncate(getattr(resp, "text", "") or "") or "Unknown error"    

    def _export_dashboard(self, oid: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Export dashboard from source. Tries adminAccess=true then falls back without it.
        Returns (exported_json, error_reason).
        """
        # Primary: adminAccess=true
        resp = self.source_client.get(f"/api/dashboards/{oid}/export?adminAccess=true")
        if resp and resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, dict):
                    return data, None
                return None, "Export returned non-dict JSON"
            except Exception:
                return None, f"Export returned invalid JSON: {self._truncate(resp.text or '')}"

        # Fallback: without adminAccess (old failsafe)
        resp2 = self.source_client.get(f"/api/dashboards/{oid}/export")
        if resp2 and resp2.status_code == 200:
            try:
                data = resp2.json()
                if isinstance(data, dict):
                    return data, None
                return None, "Export returned non-dict JSON (fallback path)"
            except Exception:
                return None, f"Export returned invalid JSON (fallback path): {self._truncate(resp2.text or '')}"

        status = resp.status_code if resp else None
        status2 = resp2.status_code if resp2 else None
        return None, f"Export failed (adminAccess={status}, fallback={status2})"

    # -------------------------------------------------------------------------
    # Migration methods
    # -------------------------------------------------------------------------

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

    def migrate_all_groups(
        self,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Migrate groups from the source environment to the target environment using the bulk endpoint.

        This method supports optional progress emission via the ``emit`` callback. If provided,
        the method will publish structured progress events at key milestones so a caller (for
        example an MCP server) can stream updates to a UI while the migration is running.

        Parameters
        ----------
        emit : Callable[[Dict[str, Any]], None], optional
            Optional callback invoked with structured progress events. If not provided, the
            method behaves like a standard synchronous call and only returns a final result.

            Each emitted event is a dictionary that typically includes:
            - ``type``: str, event type (e.g., "started", "progress", "completed", "error")
            - ``step``: str, logical step name
            - ``message``: str, human-readable message
            - Additional fields depending on the event (counts, status_code, etc.)

        Returns
        -------
        Dict[str, Any]
            Structured result payload with:
            - ``ok``: bool
            - ``status``: str ("success" | "failed" | "noop")
            - ``results``: List[Dict[str, str]] per-group statuses
            - ``source_count``: int
            - ``eligible_count``: int
            - ``success_count``: int
            - ``failed_count``: int
            - ``raw_error``: Any
            - ``warnings``: List[str]
        """
        warnings: List[str] = []
        migration_results: List[Dict[str, str]] = []
        raw_error: Any = None

        self._emit(emit, {"type": "started", "step": "init", "message": "Starting group migration from source to target."})
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Fetching groups from the source environment."})
        self.logger.debug("Fetching groups from the source environment.")
        source_response = self.source_client.get("/api/v1/groups")

        if not source_response or source_response.status_code != 200:
            status_code = self._safe_status_code(source_response)
            raw_error = self._safe_error_payload(source_response, context="fetch_source_groups")
            self.logger.error("Failed to retrieve groups from the source environment.")
            self.logger.error("Raw error response: %s", raw_error)

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "fetch_source_groups",
                    "message": "Failed to retrieve groups from the source environment.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )

            return {
                "ok": False,
                "status": "failed",
                "results": [{"message": "Failed to retrieve groups from the source environment. Please check logs."}],
                "source_count": 0,
                "eligible_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        self.logger.debug("Source environment response status code: %s", source_response.status_code)
        self.logger.debug("Source environment response body: %s", source_response.text)

        source_groups = source_response.json() or []
        source_count = len(source_groups)

        if source_count == 0:
            self.logger.info("No groups found in the source environment. Ending process.")
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "fetch_source_groups",
                    "message": "No groups found in the source environment. Nothing to migrate.",
                    "status": "noop",
                    "source_count": 0,
                },
            )
            return {
                "ok": True,
                "status": "noop",
                "results": [{"message": "No groups found in the source environment. Nothing to migrate."}],
                "source_count": 0,
                "eligible_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": None,
                "warnings": warnings,
            }

        self.logger.info("Retrieved %s groups from the source environment.", source_count)
        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Retrieved groups from the source environment.", "source_count": source_count},)

        # NEW: Resolve system tenant id so we only migrate system-tenant groups
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
                "success_count": 0,
                "failed_count": 0,
                "raw_error": raw_error,
                "warnings": warnings,
            }

        tenants = tenants_response.json() or []
        system_tenant_id: Optional[str] = None
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

        # Step 2: Filter out specific groups
        excluded_names = {"Admins", "All users in system", "Everyone"}
        bulk_group_data: List[Dict[str, Any]] = []
        skipped_count = 0
        skipped_multi_tenant_count = 0

        self._emit(emit, {"type": "progress", "step": "filter_groups", "message": "Filtering groups and preparing bulk payload.", "excluded_names": sorted(excluded_names)},)

        for group in source_groups:
            name = group.get("name")
            if not name:
                skipped_count += 1
                continue
            if name in excluded_names:
                self.logger.debug("Skipping excluded group: %s", name)
                skipped_count += 1
                continue

            tenant_id = group.get("tenantId")
            if tenant_id != system_tenant_id:
                self.logger.debug("Skipping multi-tenant group: %s", name)
                skipped_multi_tenant_count += 1
                continue

            group_data = {k: v for k, v in group.items() if k not in {"created", "lastUpdated", "tenantId", "_id"}}
            bulk_group_data.append(group_data)
            self.logger.debug("Prepared data for group: %s", name)

        eligible_count = len(bulk_group_data)

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "filter_groups",
                "message": "Prepared bulk payload for eligible groups.",
                "eligible_count": eligible_count,
                "skipped_count": skipped_count,
                "skipped_multi_tenant_count": skipped_multi_tenant_count,
                "source_count": source_count,
            },
        )

        if eligible_count == 0:
            self.logger.info("No eligible groups found for migration. Ending process.")
            self._emit(emit, {"type": "completed", "step": "filter_groups", "message": "No eligible groups found for migration.", "status": "noop", "eligible_count": 0, "skipped_count": skipped_count},)
            return {
                "ok": True,
                "status": "noop",
                "results": [{"message": "No eligible groups found for migration. Please verify the group list and try again."}],
                "source_count": source_count,
                "eligible_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "raw_error": None,
                "warnings": warnings,
            }

        # Step 3: Make the bulk POST request with the group data
        self.logger.info("Sending bulk migration request for %s groups", eligible_count)
        self.logger.debug("Payload for bulk migration: %s", bulk_group_data)
        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Sending bulk migration request.", "eligible_count": eligible_count},)

        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        status_code = self._safe_status_code(response)
        self.logger.debug("Target environment response status code: %s", status_code if status_code is not None else "No response")
        self.logger.debug("Target environment response body: %s", response.text if response is not None and hasattr(response, "text") else "No response body")

        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Received response from target bulk endpoint.", "status_code": status_code},)
        # Step 4: Handle the response from the bulk API call
        if response is not None and status_code == 201:
            try:
                response_data = response.json()
                self.logger.info("Bulk migration succeeded.")
                self._emit(emit, {"type": "progress", "step": "process_response", "message": "Processing bulk migration response.", "status_code": status_code},)

                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    migration_results.append({"name": group_name, "status": "Success"})
            except Exception:
                warn = "Bulk response was not valid JSON; assuming migration succeeded based on status code."
                warnings.append(warn)
                self.logger.warning(warn)
                self._emit(emit, {"type": "warning", "step": "process_response", "message": warn},)
                migration_results = [{"name": gd.get("name", "Unknown Group"), "status": "Success"} for gd in bulk_group_data]
        else:
            raw_error = self._safe_error_payload(response, context="bulk_post")
            self.logger.error("Bulk migration failed. Status code: %s", status_code if status_code is not None else "No response")
            self.logger.error("Raw error response: %s", raw_error)

            # Optional: extract existingGroups when present (Sisense bulk error shape)
            existing_groups: List[str] = []
            try:
                # Expected: {"error": {"moreInfo": {"existingGroups": [...]}}}
                existing_groups = raw_error.get("error", {}).get("moreInfo", {}).get("existingGroups", [])  # type: ignore[union-attr]
            except Exception:
                existing_groups = []

            if existing_groups:
                warnings.append(f"{len(existing_groups)} groups already exist in the target environment.")
                self._emit(
                    emit,
                    {
                        "type": "warning",
                        "step": "bulk_post",
                        "message": "Some groups already exist in the target environment.",
                        "existing_groups_count": len(existing_groups),
                        "existing_groups": existing_groups,
                    },
                )

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "bulk_post",
                    "message": "Bulk migration failed.",
                    "status_code": status_code,
                    "raw_error": raw_error,
                },
            )

            migration_results = [{"name": gd.get("name", "Unknown Group"), "status": "Failed"} for gd in bulk_group_data]

        success_count = sum(1 for r in migration_results if r.get("status") == "Success")
        failed_count = sum(1 for r in migration_results if r.get("status") == "Failed")

        ok = (eligible_count > 0) and (success_count == eligible_count)
        status = "success" if ok else ("noop" if eligible_count == 0 else "failed")

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished group migration.",
                "status": status,
                "source_count": source_count,
                "eligible_count": eligible_count,
                "success_count": success_count,
                "failed_count": failed_count,
                "warnings_count": len(warnings),
                "skipped_multi_tenant_count": skipped_multi_tenant_count,
                "skipped_count": skipped_count,
            },
        )

        return {
            "ok": ok,
            "status": status,
            "results": migration_results,
            "source_count": source_count,
            "eligible_count": eligible_count,
            "success_count": success_count,
            "skipped_multi_tenant_count": skipped_multi_tenant_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "raw_error": raw_error,
            "warnings": warnings,
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

    def migrate_all_users(
        self,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
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
        warnings: List[str] = []
        migration_results: List[Dict[str, str]] = []
        raw_error: Any = None

        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full user migration from source to target."})
        self.logger.info("Starting full user migration from source to target.")

        # Query parameters to expand group and role information
        params: Dict[str, str] = {"expand": "groups,role"}

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
        system_tenant_id: Optional[str] = None
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
        role_name_to_id: Dict[str, Any] = {}
        for r in target_roles:
            name = r.get("name")
            rid = r.get("_id")
            if name and rid:
                role_name_to_id[name] = rid

        group_name_to_id: Dict[str, Any] = {}
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
        bulk_user_data: List[Dict[str, Any]] = []

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

            user_group_names: List[str] = []
            try:
                user_group_names = [g.get("name") for g in (user.get("groups") or []) if g and g.get("name")]
            except Exception:
                user_group_names = []

            mapped_group_ids: List[Any] = []
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

    def migrate_dashboards(
        self,
        dashboard_ids: Optional[List[str]] = None,
        dashboard_names: Optional[List[str]] = None,
        action: Optional[Literal["skip", "overwrite", "duplicate"]] = None,
        republish: bool = False,
        migrate_share: bool = False,
        change_ownership: bool = False,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Migrate dashboards from the source to the target environment using Sisense bulk import.

        This method exports dashboards from the source and imports them into the target in a single
        bulk request. It then parses the bulk import response to determine which dashboards were
        succeeded, skipped, and failed. Finally, it optionally migrates shares/ownership for
        dashboards that were actually created in the target (depending on `migrate_share`, `action`).

        Parameters
        ----------
        dashboard_ids : list[str] or None, default None
            List of dashboard OIDs to migrate. Provide either `dashboard_ids` or `dashboard_names`.
        dashboard_names : list[str] or None, default None
            List of dashboard titles to migrate. Provide either `dashboard_ids` or `dashboard_names`.
        action : {"skip", "overwrite", "duplicate"} or None, default None
            Determines how the target handles conflicts.
            - None: default Sisense behavior (typically skip existing).
            - "skip": skip if exists.
            - "overwrite": overwrite if exists (shares/ownership migration is skipped).
            - "duplicate": create duplicate (shares/ownership migration is skipped).
        republish : bool, default False
            Whether to republish dashboards after import.
        migrate_share : bool, default False
            If True, attempts to migrate shares (and optionally ownership) after dashboards are created.
        change_ownership : bool, default False
            If True and `migrate_share=True`, attempts to change ownership on the target dashboards.

        Returns
        -------
        dict
            Migration summary with the following keys:
            - "succeeded": list[dict]
                Each item includes: {"title": str, "source_id": str | None, "target_id": str | None}
            - "skipped": list[dict]
                Each item includes: {"title": str, "source_id": str | None, "target_id": str | None, "reason": str | None}
            - "failed": list[dict]
                Each item includes: {"title": str | None, "source_id": str | None, "reason": str}
            - "meta": dict
                Helpful metadata about request-level status.

        Notes
        -----
        Response parsing strategy:
        1) Primary (source of truth): when bulk import returns a success status (typically 201),
        parse the structured response fields:
        - "succeded" / "succeeded" (Sisense sometimes uses the misspelling)
        - "skipped"
        - "failed" (often grouped by error category)
        2) Fallbacks (old failsafes): if the response is missing expected fields, not JSON,
        or indicates a request-level error via keys like "message" / "error", we treat the
        entire batch as failed and attach the best-available reason from:
        - response JSON "message" / "error" / "error.message"
        - response.text (truncated)
        """

        self._emit(
            emit,
            {"type": "started", "step": "init", "message": "Starting dashboard migration from source to target."},
        )

        # -------------------------
        # Validation
        # -------------------------
        if dashboard_ids and dashboard_names:
            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "validation",
                    "message": "Provide either 'dashboard_ids' or 'dashboard_names', not both.",
                },
            )
            raise ValueError("Provide either 'dashboard_ids' or 'dashboard_names', not both.")

        if not dashboard_ids and not dashboard_names:
            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "validation",
                    "message": "Provide either 'dashboard_ids' or 'dashboard_names'.",
                },
            )
            raise ValueError("Provide either 'dashboard_ids' or 'dashboard_names'.")

        if not migrate_share and change_ownership:
            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "validation",
                    "message": "The 'change_ownership' parameter requires 'migrate_share=True'.",
                },
            )
            raise ValueError("The 'change_ownership' parameter requires 'migrate_share=True'.")

        self.logger.info("Starting dashboard migration from source to target.")

        summary: Dict[str, Any] = {
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "meta": {
                "export_requested": 0,
                "export_succeeded": 0,
                "export_failed": 0,
                "bulk_status_code": None,
                "bulk_request_failed": False,
                "bulk_request_reason": None,
            },
        }

        # -------------------------
        # Step 1: Resolve + export dashboards from source
        # -------------------------
        bulk_dashboard_data: List[Dict[str, Any]] = []
        source_id_to_title: Dict[str, str] = {}

        if dashboard_ids:
            self.logger.info("Processing dashboard migration by IDs.")

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "export_dashboards",
                    "message": "Exporting dashboards from source by IDs.",
                    "requested_count": len(dashboard_ids),
                },
            )

            for idx, oid in enumerate(dashboard_ids, start=1):
                summary["meta"]["export_requested"] += 1

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "export_dashboards",
                        "message": "Exporting dashboard.",
                        "current": idx,
                        "total": len(dashboard_ids),
                        "source_id": oid,
                    },
                )

                # Export dashboard from source. Tries adminAccess=true then falls back without it.
                exported, err = self._export_dashboard(oid)

                if exported:
                    title = exported.get("title")
                    source_id_to_title[oid] = title if isinstance(title, str) else oid
                    bulk_dashboard_data.append(exported)
                    summary["meta"]["export_succeeded"] += 1
                    self.logger.debug(f"Exported dashboard '{source_id_to_title[oid]}' (source_id={oid}).")
                else:
                    summary["meta"]["export_failed"] += 1
                    self.logger.error(f"Failed to export dashboard (source_id={oid}). Reason: {err}")
                    summary["failed"].append({"title": None, "source_id": oid, "reason": err or "Export failed"})

                    self._emit(
                        emit,
                        {
                            "type": "error",
                            "step": "export_dashboards",
                            "message": "Failed to export dashboard.",
                            "source_id": oid,
                            "reason": err or "Export failed",
                        },
                    )

        else:
            # dashboard_names
            self.logger.info("Processing dashboard migration by names.")
            limit = 50
            skip = 0
            dashboards: List[Dict[str, Any]] = []

            # Use admin endpoint instead of searches (smaller payload via fields)
            dashboard_columns = ["oid", "title"]

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "resolve_source_dashboards",
                    "message": "Resolving dashboards by name from source environment.",
                    "requested_names_count": len(dashboard_names or []),
                    "limit": limit,
                    "skip": skip,
                },
            )

            pages_fetched = 0
            while True:
                resp = self.source_client.get(
                    "/api/v1/dashboards/admin",
                    params={
                        "dashboardType": "owner",
                        "asObject": "false",
                        "fields": ",".join(dashboard_columns),
                        "limit": limit,
                        "skip": skip,
                    },
                )
                if not resp or resp.status_code != 200:
                    self._emit(
                        emit,
                        {
                            "type": "warning",
                            "step": "resolve_source_dashboards",
                            "message": "Stopping dashboard pagination due to non-200 response. Proceeding with dashboards already retrieved.",
                            "status_code": getattr(resp, "status_code", None),
                            "pages_fetched": pages_fetched,
                            "retrieved_so_far": len(dashboards),
                        },
                    )
                    break

                payload, _ = self._safe_json(resp)

                items: List[Dict[str, Any]] = []
                if isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict):
                    for key in ("items", "dashboards", "results", "data"):
                        v = payload.get(key)
                        if isinstance(v, list):
                            items = v
                            break

                if not items:
                    break

                dashboards.extend(items)
                skip += limit
                pages_fetched += 1

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "resolve_source_dashboards",
                    "message": "Finished resolving dashboards from source environment.",
                    "pages_fetched": pages_fetched,
                    "retrieved_total": len(dashboards),
                },
            )

            # Deduplicate by oid
            unique = {d.get("oid"): d for d in dashboards if d.get("oid")}
            dashboards = list(unique.values())
            wanted = set(dashboard_names or [])

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "export_dashboards",
                    "message": "Exporting dashboards from source by names.",
                    "requested_names_count": len(dashboard_names or []),
                },
            )

            matched = 0
            for d in dashboards:
                title = d.get("title")
                oid = d.get("oid")
                if not oid or not isinstance(title, str):
                    continue
                if title not in wanted:
                    continue

                matched += 1
                summary["meta"]["export_requested"] += 1

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "export_dashboards",
                        "message": "Exporting dashboard.",
                        "source_id": oid,
                        "title": title,
                    },
                )

                # Export dashboard from source. Tries adminAccess=true then falls back without it.
                exported, err = self._export_dashboard(oid)

                if exported:
                    source_id_to_title[oid] = title
                    bulk_dashboard_data.append(exported)
                    summary["meta"]["export_succeeded"] += 1
                    self.logger.debug(f"Exported dashboard '{title}' (source_id={oid}).")
                else:
                    summary["meta"]["export_failed"] += 1
                    self.logger.error(f"Failed to export dashboard '{title}' (source_id={oid}). Reason: {err}")
                    summary["failed"].append({"title": title, "source_id": oid, "reason": err or "Export failed"})

                    self._emit(
                        emit,
                        {
                            "type": "error",
                            "step": "export_dashboards",
                            "message": "Failed to export dashboard.",
                            "source_id": oid,
                            "title": title,
                            "reason": err or "Export failed",
                        },
                    )

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "resolve_source_dashboards",
                    "message": "Finished matching dashboards by name.",
                    "matched_count": matched,
                    "requested_names_count": len(dashboard_names or []),
                },
            )

        if not bulk_dashboard_data:
            self.logger.info("No dashboards were exported successfully. Skipping bulk import.")
            self.logger.info("Dashboard migration completed.")

            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "No dashboards were exported successfully. Skipping bulk import.",
                    "status": "noop",
                    "export_requested": summary["meta"]["export_requested"],
                    "export_succeeded": summary["meta"]["export_succeeded"],
                    "export_failed": summary["meta"]["export_failed"],
                },
            )
            return summary

        # -------------------------
        # Step 2: Bulk import into target
        # -------------------------
        url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
        if action:
            url += f"&action={action}"

        self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
        self._emit(
            emit,
            {
                "type": "progress",
                "step": "bulk_import",
                "message": "Sending bulk dashboard import request to target.",
                "count": len(bulk_dashboard_data),
                "republish": republish,
                "action": action,
            },
        )

        resp = self.target_client.post(url, data=bulk_dashboard_data)
        summary["meta"]["bulk_status_code"] = resp.status_code if resp else None
        self.logger.debug(f"Response for bulk migration: {getattr(resp, 'text', None) if resp else 'No response'}")

        # Always attempt to parse JSON (even for non-201) to capture old fail-safe messages
        resp_json, json_err = self._safe_json(resp)

        # Decide if this is a request-level success vs failure
        request_success = bool(resp and resp.status_code in (200, 201) and isinstance(resp_json, dict))

        # Old failsafe signals (request-level error payloads)
        # Examples: {"message": "..."} or {"error": {"message": "..."}} etc.
        message_from_payload: Optional[str] = None
        if isinstance(resp_json, dict):
            if isinstance(resp_json.get("message"), str):
                message_from_payload = resp_json["message"]
            elif isinstance(resp_json.get("error"), dict) and isinstance(resp_json["error"].get("message"), str):
                message_from_payload = resp_json["error"]["message"]

        if not request_success:
            # Request-level failure: mark everything as failed (but keep export failures already captured)
            reason = (
                message_from_payload
                or (json_err if json_err else None)
                or self._truncate(getattr(resp, "text", "") or "")
                or f"Bulk import failed (status_code={summary['meta']['bulk_status_code']})"
            )
            summary["meta"]["bulk_request_failed"] = True
            summary["meta"]["bulk_request_reason"] = reason

            # Add failures for dashboards that made it into the bulk payload
            for exported in bulk_dashboard_data:
                src_id = exported.get("oid") if isinstance(exported, dict) else None
                title = exported.get("title") if isinstance(exported, dict) else None
                summary["failed"].append(
                    {
                        "title": title if isinstance(title, str) else None,
                        "source_id": src_id if isinstance(src_id, str) else None,
                        "reason": reason,
                    }
                )

            self.logger.error(f"Bulk migration request failed. Reason: {reason}")
            self.logger.info("Dashboard migration completed.")

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "bulk_import",
                    "message": "Bulk dashboard import request failed.",
                    "status_code": summary["meta"]["bulk_status_code"],
                    "reason": reason,
                },
            )
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "Finished migrating dashboards.",
                    "status": "failed",
                    "succeeded_count": len(summary["succeeded"]),
                    "skipped_count": len(summary["skipped"]),
                    "failed_count": len(summary["failed"]),
                },
            )
            return summary

        # Request-level success: parse structured fields as source of truth
        succeeded_key = "succeded" if "succeded" in resp_json else "succeeded"

        succeeded_items = resp_json.get(succeeded_key, []) if isinstance(resp_json.get(succeeded_key, []), list) else []
        skipped_items = resp_json.get("skipped", []) if isinstance(resp_json.get("skipped", []), list) else []
        failed_obj = resp_json.get("failed", {}) if isinstance(resp_json.get("failed", {}), dict) else {}

        # If none of the expected fields exist but we do have an old-style message, treat as failure
        if not succeeded_items and not skipped_items and not failed_obj and message_from_payload:
            summary["meta"]["bulk_request_failed"] = True
            summary["meta"]["bulk_request_reason"] = message_from_payload
            for exported in bulk_dashboard_data:
                src_id = exported.get("oid") if isinstance(exported, dict) else None
                title = exported.get("title") if isinstance(exported, dict) else None
                summary["failed"].append(
                    {
                        "title": title if isinstance(title, str) else None,
                        "source_id": src_id if isinstance(src_id, str) else None,
                        "reason": message_from_payload,
                    }
                )
            self.logger.error(f"Bulk migration returned message without per-item results: {message_from_payload}")
            self.logger.info("Dashboard migration completed.")

            self._emit(
                emit,
                {
                    "type": "error",
                    "step": "bulk_import",
                    "message": "Bulk import returned a message without per-item results.",
                    "reason": message_from_payload,
                },
            )
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "Finished migrating dashboards.",
                    "status": "failed",
                    "succeeded_count": len(summary["succeeded"]),
                    "skipped_count": len(summary["skipped"]),
                    "failed_count": len(summary["failed"]),
                },
            )
            return summary

        # Build a target-title map to support share/ownership migration
        # Note: we still match by title because the bulk response does not reliably include a source id.
        target_id_to_title: Dict[str, str] = {}

        # Build a title -> [source_id, ...] lookup from what we exported (best effort).
        # This lets us attach source_id to succeeded/skipped items
        source_ids_by_title: Dict[str, List[str]] = {}
        for exported in bulk_dashboard_data:
            if not isinstance(exported, dict):
                continue
            t = exported.get("title")
            soid = exported.get("oid")
            if isinstance(t, str) and isinstance(soid, str):
                source_ids_by_title.setdefault(t, []).append(soid)

        # Parse succeeded
        for item in succeeded_items:
            if not isinstance(item, dict):
                continue

            title = item.get("title")
            target_id = item.get("oid")

            source_id: Optional[str] = None
            if isinstance(title, str):
                ids = source_ids_by_title.get(title) or []
                if ids:
                    source_id = ids.pop(0)

            if isinstance(target_id, str) and isinstance(title, str):
                target_id_to_title[target_id] = title

            summary["succeeded"].append(
                {
                    "title": title if isinstance(title, str) else None,
                    "source_id": source_id,
                    "target_id": target_id if isinstance(target_id, str) else None,
                }
            )

        # Parse skipped
        for item in skipped_items:
            if not isinstance(item, dict):
                continue

            title = item.get("title")
            target_id = item.get("oid")

            source_id: Optional[str] = None
            if isinstance(title, str):
                ids = source_ids_by_title.get(title) or []
                if ids:
                    source_id = ids.pop(0)

            summary["skipped"].append(
                {
                    "title": title if isinstance(title, str) else None,
                    "source_id": source_id,
                    "target_id": target_id if isinstance(target_id, str) else None,
                    "reason": "skipped_by_target",
                }
            )

        # Parse failed (grouped by category)
        if isinstance(failed_obj, dict):
            for category, errors in failed_obj.items():
                if not isinstance(errors, list):
                    continue
                for e in errors:
                    if not isinstance(e, dict):
                        continue
                    title = e.get("title")
                    src_id = e.get("oid")
                    err_detail = e.get("error")
                    # Old/new variations:
                    # - error: {"message": "..."}
                    # - error: {"code":..., "keyValue":...}
                    # - sometimes only a string
                    reason: str
                    if isinstance(err_detail, dict) and isinstance(err_detail.get("message"), str):
                        reason = err_detail["message"]
                    elif isinstance(err_detail, str):
                        reason = err_detail
                    else:
                        reason = f"{category}: {str(err_detail)}"
                    summary["failed"].append(
                        {
                            "title": title if isinstance(title, str) else None,
                            "source_id": src_id if isinstance(src_id, str) else None,
                            "reason": reason,
                        }
                    )

        self.logger.info(
            f"Bulk import parsed results: "
            f"succeeded={len(summary['succeeded'])}, skipped={len(summary['skipped'])}, failed={len(summary['failed'])}."
        )

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "bulk_import",
                "message": "Bulk import parsed results.",
                "succeeded_count": len(summary["succeeded"]),
                "skipped_count": len(summary["skipped"]),
                "failed_count": len(summary["failed"]),
                "status_code": summary["meta"]["bulk_status_code"],
            },
        )

        # -------------------------
        # Step 3: Optional shares/ownership migration
        # -------------------------
        if not migrate_share:
            self.logger.info("migrate_share=False. Skipping shares and ownership migration.")

            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "Finished migrating dashboards.",
                    "status": "success" if len(summary["failed"]) == 0 else "failed",
                    "succeeded_count": len(summary["succeeded"]),
                    "skipped_count": len(summary["skipped"]),
                    "failed_count": len(summary["failed"]),
                    "shares_migrated": 0,
                },
            )
            return summary

        if action in ("duplicate", "overwrite"):
            self.logger.info(f"action='{action}'. Skipping shares and ownership migration.")

            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "Finished migrating dashboards.",
                    "status": "success" if len(summary["failed"]) == 0 else "failed",
                    "succeeded_count": len(summary["succeeded"]),
                    "skipped_count": len(summary["skipped"]),
                    "failed_count": len(summary["failed"]),
                    "shares_migrated": 0,
                },
            )
            return summary

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "migrate_shares",
                "message": "Starting share/ownership migration for dashboards.",
                "change_ownership": change_ownership,
            },
        )

        # Build source->target mapping by matching titles (best effort).
        # Warn on collisions.
        source_to_target: Dict[str, str] = {}
        title_to_targets: Dict[str, List[str]] = {}
        for tid, ttitle in target_id_to_title.items():
            title_to_targets.setdefault(ttitle, []).append(tid)

        for src_id, src_title in source_id_to_title.items():
            targets = title_to_targets.get(src_title, [])
            if not targets:
                self.logger.warning(
                    f"Source dashboard '{src_title}' (source_id={src_id}) not found among succeeded target dashboards."
                )
                continue
            if len(targets) > 1:
                self.logger.warning(
                    f"Multiple target dashboards share the same title '{src_title}'. "
                    f"Using the first one for shares/ownership migration. target_ids={targets}"
                )
            source_to_target[src_id] = targets[0]

        if not source_to_target:
            self.logger.info(
                "No dashboards eligible for shares/ownership migration (no source->target mapping could be formed)."
            )

            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "Finished migrating dashboards.",
                    "status": "success" if len(summary["failed"]) == 0 else "failed",
                    "succeeded_count": len(summary["succeeded"]),
                    "skipped_count": len(summary["skipped"]),
                    "failed_count": len(summary["failed"]),
                    "shares_migrated": 0,
                },
            )
            return summary

        self.logger.info(f"Starting share/ownership migration for {len(source_to_target)} dashboards.")
        self.migrate_dashboard_shares(
            source_dashboard_ids=list(source_to_target.keys()),
            target_dashboard_ids=list(source_to_target.values()),
            change_ownership=change_ownership,
        )
        self.logger.info("Share and ownership migration completed.")

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "migrate_shares",
                "message": "Completed share/ownership migration for dashboards.",
                "migrated_count": len(source_to_target),
            },
        )

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished migrating dashboards.",
                "status": "success" if len(summary["failed"]) == 0 else "failed",
                "succeeded_count": len(summary["succeeded"]),
                "skipped_count": len(summary["skipped"]),
                "failed_count": len(summary["failed"]),
            },
        )

        return summary

    def migrate_all_dashboards(
        self,
        action: Optional[str] = None,
        republish: bool = False,
        migrate_share: bool = False,
        change_ownership: bool = False,
        batch_size: int = 10,
        sleep_time: int = 10,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Migrates all dashboards from the source to the target environment in batches.

        Parameters
        ----------
        action : str, optional
            Determines how to handle existing dashboards in the target environment.
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
        republish : bool, optional
            Whether to republish dashboards after migration. Default: False.
        migrate_share : bool, optional
            Whether to migrate shares for the dashboards. If `True`, shares will be
            migrated, and ownership migration will be controlled by the `change_ownership` parameter.
            If `False`, both shares and ownership migration will be skipped. Default: False.
        change_ownership : bool, optional
            Whether to change ownership of the target dashboards.
            Effective only if `migrate_share` is True. Default: False.
        batch_size : int, optional
            Number of dashboards to process in each batch. Default: 10.
        sleep_time : int, optional
            Time (in seconds) to sleep between batches. Default: 10 seconds.
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
        dict
            A summary of the migration results for all batches, containing lists of succeeded, skipped,
            and failed dashboards.

        Notes
        -----
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
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting dashboard migration from source to target."})

        self.logger.info("Fetching all dashboards from the source environment.")
        all_dashboard_ids: set = set()

        # Step 1: Fetch all dashboards
        limit = 50
        skip = 0
        pages_fetched = 0

        total_items_seen = 0
        total_missing_oid = 0
        total_duplicate_oid = 0
        duplicate_oids_sample_global: List[str] = []

        while True:
            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "fetch_source_dashboards",
                    "message": "Fetching dashboards page from source environment.",
                    "limit": limit,
                    "skip": skip,
                    "pages_fetched": pages_fetched,
                    "total_unique_so_far": len(all_dashboard_ids),
                },
            )

            dashboard_response = self.source_client.get(
                "/api/v1/dashboards/admin",
                params={
                    "fields": "oid,title",
                    "dashboardType": "owner",
                    "asObject": "false",
                    "limit": limit,
                    "skip": skip,
                },
            )

            if not dashboard_response or dashboard_response.status_code != 200:
                status_code = self._safe_status_code(dashboard_response)
                raw_error = self._safe_error_payload(dashboard_response, context="fetch_source_dashboards")

                # If we fail on the very first page, treat as hard failure.
                if pages_fetched == 0:
                    self.logger.error("Failed to retrieve dashboards from the source environment.")
                    self.logger.error("Raw error response: %s", raw_error)

                    self._emit(
                        emit,
                        {
                            "type": "error",
                            "step": "fetch_source_dashboards",
                            "message": "Failed to retrieve dashboards from the source environment.",
                            "status_code": status_code,
                            "raw_error": raw_error,
                        },
                    )

                    return {
                        "ok": False,
                        "status": "failed",
                        "succeeded": [],
                        "skipped": [],
                        "failed": [],
                        "total_count": 0,
                        "succeeded_count": 0,
                        "skipped_count": 0,
                        "failed_count": 0,
                        "pages_fetched": pages_fetched,
                        "batches_total": 0,
                        "batch_errors_count": 0,
                        "batch_errors": [],
                        "raw_error": raw_error,
                    }

                # If we fail after at least one page, emit warning and stop paginating.
                self.logger.warning("Stopping pagination due to non-200 response. Status=%s", status_code)
                self._emit(
                    emit,
                    {
                        "type": "warning",
                        "step": "fetch_source_dashboards",
                        "message": "Stopping pagination due to a non-200 response. Proceeding with dashboards already retrieved.",
                        "status_code": status_code,
                        "raw_error": raw_error,
                        "pages_fetched": pages_fetched,
                        "retrieved_so_far": len(all_dashboard_ids),
                    },
                )
                break

            body = dashboard_response.json()

            items: List[Dict[str, Any]] = []
            if isinstance(body, list):
                items = body
            elif isinstance(body, dict):
                for key in ("items", "dashboards", "results", "data", "values", "rows"):
                    v = body.get(key)
                    if isinstance(v, list):
                        items = v
                        break

            if not items:
                self.logger.debug("No more items in response; breaking pagination loop.")
                break

            pages_fetched += 1
            total_items_seen += len(items)
            self.logger.debug("Fetched %s dashboards in this page.", len(items))

            added_this_page = 0
            missing_oid_count = 0
            duplicate_oid_count = 0
            duplicate_oids_sample_page: List[str] = []

            for dash in items:
                oid = None
                try:
                    oid = dash.get("oid")
                except Exception:
                    oid = None

                if not oid:
                    missing_oid_count += 1
                    continue

                if oid in all_dashboard_ids:
                    duplicate_oid_count += 1
                    if len(duplicate_oids_sample_page) < 10:
                        duplicate_oids_sample_page.append(oid)
                    if len(duplicate_oids_sample_global) < 50 and oid not in duplicate_oids_sample_global:
                        duplicate_oids_sample_global.append(oid)
                    continue

                all_dashboard_ids.add(oid)
                added_this_page += 1

            total_missing_oid += missing_oid_count
            total_duplicate_oid += duplicate_oid_count

            if duplicate_oid_count > 0 or missing_oid_count > 0:
                self.logger.warning(
                    "Dashboard pagination anomalies detected: items=%s added_unique=%s duplicates=%s missing_oid=%s duplicate_oids_sample=%s skip=%s limit=%s",
                    len(items),
                    added_this_page,
                    duplicate_oid_count,
                    missing_oid_count,
                    duplicate_oids_sample_page,
                    skip,
                    limit,
                )
            else:
                self.logger.debug(
                    "Pagination page summary: items=%s added_unique=%s total_unique_so_far=%s skip=%s limit=%s",
                    len(items),
                    added_this_page,
                    len(all_dashboard_ids),
                    skip,
                    limit,
                )

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "fetch_source_dashboards",
                    "message": "Fetched dashboards page.",
                    "items_count": len(items),
                    "added_unique_count": added_this_page,
                    "duplicate_oid_count": duplicate_oid_count,
                    "missing_oid_count": missing_oid_count,
                    "duplicate_oids_sample": duplicate_oids_sample_page,
                    "total_unique_count": len(all_dashboard_ids),
                    "total_items_seen": total_items_seen,
                    "pages_fetched": pages_fetched,
                },
            )

            skip += limit

        self.logger.info("Total unique dashboards retrieved: %s.", len(all_dashboard_ids))
        self.logger.info(
            "Dashboard fetch totals: total_items_seen=%s total_unique=%s total_duplicates=%s total_missing_oid=%s duplicate_oids_sample_global=%s",
            total_items_seen,
            len(all_dashboard_ids),
            total_duplicate_oid,
            total_missing_oid,
            duplicate_oids_sample_global[:20],
        )
        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_source_dashboards",
                "message": "Finished fetching dashboards from source environment.",
                "total_unique_count": len(all_dashboard_ids),
                "pages_fetched": pages_fetched,
                "total_items_seen": total_items_seen,
                "total_duplicate_oid": total_duplicate_oid,
                "total_missing_oid": total_missing_oid,
            },
        )

        # Step 2: Migrate dashboards in batches
        all_dashboard_ids_list: List[Any] = sorted(list(all_dashboard_ids))
        migration_summary: Dict[str, Any] = {"succeeded": [], "skipped": [], "failed": []}

        total_count = len(all_dashboard_ids_list)
        batches_total = (total_count + batch_size - 1) // batch_size if batch_size > 0 else 0
        batch_errors: List[Dict[str, Any]] = []

        if total_count == 0:
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "No dashboards found to migrate.",
                    "status": "noop",
                    "total_count": 0,
                    "pages_fetched": pages_fetched,
                },
            )
            return {
                "ok": True,
                "status": "noop",
                "succeeded": [],
                "skipped": [],
                "failed": [],
                "total_count": 0,
                "succeeded_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "pages_fetched": pages_fetched,
                "batches_total": 0,
                "batch_errors_count": 0,
                "batch_errors": [],
                "raw_error": None,
            }

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "batch_migration",
                "message": "Starting batch dashboard migration.",
                "total_count": total_count,
                "batch_size": batch_size,
                "batches_total": batches_total,
                "action": action,
                "republish": republish,
                "migrate_share": migrate_share,
                "change_ownership": change_ownership,
            },
        )

        for i in range(0, total_count, batch_size):
            batch_ids = all_dashboard_ids_list[i : i + batch_size]
            batch_number = (i // batch_size) + 1

            self.logger.info("Processing batch %s with %s dashboards: %s", batch_number, len(batch_ids), batch_ids)
            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "batch_migration",
                    "message": "Starting dashboard migration batch.",
                    "batch_number": batch_number,
                    "batches_total": batches_total,
                    "batch_size": len(batch_ids),
                    "processed_so_far": i,
                    "total_count": total_count,
                },
            )

            try:
                batch_summary = self.migrate_dashboards(
                    dashboard_ids=batch_ids,
                    action=action,
                    republish=republish,
                    migrate_share=migrate_share,
                    change_ownership=change_ownership,
                )
                self.logger.info("Batch %s migration summary: %s", batch_number, batch_summary)

                # Aggregate batch results into the overall summary
                migration_summary["succeeded"].extend(batch_summary.get("succeeded", []))
                migration_summary["skipped"].extend(batch_summary.get("skipped", []))
                migration_summary["failed"].extend(batch_summary.get("failed", []))

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "batch_migration",
                        "message": "Completed dashboard migration batch.",
                        "batch_number": batch_number,
                        "succeeded_total": len(migration_summary["succeeded"]),
                        "skipped_total": len(migration_summary["skipped"]),
                        "failed_total": len(migration_summary["failed"]),
                    },
                )
            except Exception as e:
                self.logger.error("Error occurred in batch %s: %s", batch_number, e)

                # Keep going, but record the batch error. Do not assume the entire batch failed.
                batch_errors.append({"batch_number": batch_number, "dashboard_ids": batch_ids, "error": str(e)})

                self._emit(
                    emit,
                    {
                        "type": "error",
                        "step": "batch_migration",
                        "message": "Error occurred during dashboard migration batch.",
                        "batch_number": batch_number,
                        "error": str(e),
                    },
                )

                # Salvage mode: re-run dashboards one-by-one using safest action="skip" to avoid duplicating already-created dashboards.
                self.logger.warning(
                    "Entering salvage mode for batch %s. Retrying dashboards individually with action='skip' to avoid duplications.",
                    batch_number,
                )
                self._emit(
                    emit,
                    {
                        "type": "warning",
                        "step": "batch_migration",
                        "message": "Entering salvage mode: retrying dashboards individually with action='skip'.",
                        "batch_number": batch_number,
                        "dashboards_in_batch": len(batch_ids),
                    },
                )

                for did in batch_ids:
                    try:
                        single_summary = self.migrate_dashboards(
                            dashboard_ids=[did],
                            action="skip",
                            republish=republish,
                            migrate_share=migrate_share,
                            change_ownership=change_ownership,
                        )

                        migration_summary["succeeded"].extend(single_summary.get("succeeded", []))
                        migration_summary["skipped"].extend(single_summary.get("skipped", []))
                        migration_summary["failed"].extend(single_summary.get("failed", []))
                    except Exception as e2:
                        self.logger.error("Salvage retry failed for dashboard %s in batch %s: %s", did, batch_number, e2)
                        migration_summary["failed"].append(
                            {"title": None, "source_id": did, "reason": f"Salvage retry failed: {str(e2)}"}
                        )

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "batch_migration",
                        "message": "Completed salvage mode for dashboard batch.",
                        "batch_number": batch_number,
                        "succeeded_total": len(migration_summary["succeeded"]),
                        "skipped_total": len(migration_summary["skipped"]),
                        "failed_total": len(migration_summary["failed"]),
                    },
                )

            if i + batch_size < total_count:  # Avoid sleeping after the last batch
                self.logger.info("Sleeping for %s seconds before processing the next batch.", sleep_time)
                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "sleep",
                        "message": "Sleeping before next dashboard batch.",
                        "sleep_time_seconds": sleep_time,
                        "next_batch_number": batch_number + 1,
                    },
                )
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all dashboards.")
        self.logger.info("Total Dashboards Migrated: %s", len(migration_summary["succeeded"]))
        self.logger.info("Total Dashboards Skipped: %s", len(migration_summary["skipped"]))
        self.logger.info("Total Dashboards Failed: %s", len(migration_summary["failed"]))
        self.logger.info(migration_summary)

        succeeded_count = len(migration_summary["succeeded"])
        skipped_count = len(migration_summary["skipped"])
        failed_count = len(migration_summary["failed"])
        ok = (total_count > 0) and (failed_count == 0)
        status = "success" if ok else "failed"

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished migrating all dashboards.",
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "pages_fetched": pages_fetched,
                "batches_total": batches_total,
                "batch_errors_count": len(batch_errors),
                "total_items_seen": total_items_seen,
                "total_duplicate_oid": total_duplicate_oid,
                "total_missing_oid": total_missing_oid,
            },
        )

        # Backward compatible: keep the original keys, but add counts/metadata for MCP and callers.
        migration_summary.update(
            {
                "ok": ok,
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "duplicate_count": total_duplicate_oid,
                "pages_fetched": pages_fetched,
                "batches_total": batches_total,
                "batch_errors_count": len(batch_errors),
                "batch_errors": batch_errors,
                "raw_error": None,
            }
        )
        return migration_summary

    def migrate_datamodels(
        self,
        datamodel_ids: Optional[List[str]] = None,
        datamodel_names: Optional[List[str]] = None,
        provider_connection_map: Optional[Dict[str, str]] = None,
        dependencies: Optional[Union[List[str], str]] = None,
        shares: bool = False,
        action: Optional[str] = None,
        new_title: Optional[str] = None,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Migrates specific data models from the source environment to the target environment.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            A list of data model IDs to migrate. Either `datamodel_ids` or `datamodel_names` must be provided.
        datamodel_names : list[str] or None, default None
            A list of data model names to migrate. Either `datamodel_ids` or `datamodel_names` must be provided.
        provider_connection_map : dict[str, str] or None, default None
            A dictionary mapping provider names to connection IDs. This allows specifying different connections
            per provider.
            Example:
            {
                "Databricks": "Connection ID",
                "GoogleBigQuery": "Connection ID"
            }
        dependencies : list[str] or str or None, default None
            A list of dependencies to include in the migration. If not provided or if "all" is passed, all
            dependencies are selected by default.

            Possible values:
            - "dataSecurity" (includes both Data Security and Scope Configuration)
            - "formulas" (for Formulas)
            - "hierarchies" (for Drill Hierarchies)
            - "perspectives" (for Perspectives)

            If left blank or set to "all", all dependencies are included by default.
        shares : bool, default False
            Whether to also migrate the data model's shares.
        action : str or None, default None
            Strategy to handle existing data models in the target environment.

                - "overwrite": Attempts to overwrite existing model using its original ID via the datamodelId parameter.
                If the model is not found in target environment, it will automatically fall back and create the model.
                - "duplicate": Creates a new model by passing a `new_title` to the `newTitle` parameter of the import API.
                If `new_title` is not provided, the original title will be used with " (Duplicate)" appended.
        new_title : str or None, default None
            New name for the duplicated data model. Used only when `action='duplicate'`.
        emit : Callable[[dict], None] or None, default None
            Optional callback invoked with structured progress events. If not provided, the method emits no events
            and only returns a final result.

            Event payloads follow a consistent shape:
            - ``type``: str ("started" | "progress" | "warning" | "error" | "completed")
            - ``step``: str logical step identifier
            - ``message``: str human-readable message
            - Additional fields depending on the step (counts, status_code, etc.)

        Returns
        -------
        dict
            A summary of the migration results containing lists of succeeded, skipped, and failed data models,
            plus counts/metadata in a dashboard-consistent format.
        """
        self._emit(
            emit,
            {"type": "started", "step": "init", "message": "Starting datamodel migration from source to target."},
        )

        # Mapping user-friendly terms to API parameters
        dependency_mapping: Dict[str, List[str]] = {
            "dataSecurity": ["dataContext", "scopeConfiguration"],
            "formulas": ["formulaManagement"],
            "hierarchies": ["drillHierarchies"],
            "perspectives": ["perspectives"],
        }

        # Set default dependencies if none are provided
        if dependencies is None or dependencies == "all":
            dependencies = list(dependency_mapping.keys())

        if isinstance(dependencies, str):
            dependencies = [dependencies]

        api_dependencies = list({dep for key in (dependencies or []) for dep in dependency_mapping.get(key, [])})

        # Validate input parameters
        if datamodel_ids and datamodel_names:
            raise ValueError("Please provide either 'datamodel_ids' or 'datamodel_names', not both.")
        if not datamodel_ids and not datamodel_names:
            raise ValueError("You must provide either 'datamodel_ids' or 'datamodel_names'.")

        self.logger.info("Starting data model migration from source to target.")
        self.logger.debug(
            "Input Parameters: datamodel_ids=%s, datamodel_names=%s, dependencies=%s, shares=%s, action=%s",
            datamodel_ids,
            datamodel_names,
            dependencies,
            shares,
            action,
        )

        # Initialize migration summary in a dashboard-consistent shape
        result: Dict[str, Any] = {
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "meta": {
                "dependencies": dependencies,
                "api_dependencies": api_dependencies,
                "requested_count": 0,
                "resolved_count": 0,
                "export_requested": 0,
                "export_succeeded": 0,
                "export_failed": 0,
                "import_succeeded": 0,
                "import_failed": 0,
                "shares_requested": shares,
                "share_success_count": 0,
                "share_fail_count": 0,
                "share_details": {},
                "failure_reasons": {},
            },
        }

        # -------------------------
        # Step 1: Resolve datamodel IDs (if names provided)
        # -------------------------
        resolved_ids: List[str] = []
        id_to_title: Dict[str, str] = {}

        requested_items = datamodel_ids or datamodel_names or []
        result["meta"]["requested_count"] = len(requested_items)

        if datamodel_ids:
            resolved_ids = list(datamodel_ids)
            result["meta"]["resolved_count"] = len(resolved_ids)
        else:
            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "resolve_source_datamodels",
                    "message": "Resolving datamodel IDs from source by name.",
                    "requested_names_count": len(datamodel_names or []),
                },
            )

            self.logger.debug("Fetching all data models to filter by names.")

            wanted = set(datamodel_names or [])
            found_title_to_oid: Dict[str, str] = {}
            found_title_to_type: Dict[str, Any] = {}
            duplicate_titles: Dict[str, int] = {}

            limit = 100
            skip = 0
            pages_fetched = 0
            total_items_seen = 0

            fields = ["oid", "title", "type", "lastBuildTime", "relationType", "creator", "tenant"]

            while True:
                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "resolve_source_datamodels",
                        "message": "Fetching datamodels page from source environment.",
                        "limit": limit,
                        "skip": skip,
                        "pages_fetched": pages_fetched,
                        "found_so_far": len(found_title_to_oid),
                        "wanted_count": len(wanted),
                    },
                )

                response = self.source_client.get(
                    "/api/v2/datamodels/schema",
                    params={"fields": ",".join(fields), "limit": limit, "skip": skip},
                )

                # Keep existing behavior, but cover edge-case where Response is falsy for 4xx/5xx.
                if response is None or response.status_code != 200:
                    reason = self._extract_error_detail(response)
                    status_code = getattr(response, "status_code", None)

                    if pages_fetched == 0:
                        self.logger.error("Failed to fetch data models. Error: %s", reason)
                        self._emit(
                            emit,
                            {
                                "type": "error",
                                "step": "resolve_source_datamodels",
                                "message": "Failed to fetch datamodel list from source.",
                                "status_code": status_code,
                                "raw_error": reason,
                            },
                        )
                        result.update(
                            {
                                "ok": False,
                                "status": "failed",
                                "total_count": 0,
                                "succeeded_count": 0,
                                "skipped_count": 0,
                                "failed_count": 0,
                                "raw_error": reason,
                            }
                        )
                        return result

                    self.logger.warning(
                        "Stopping pagination due to non-200 response. Status=%s Error=%s", status_code, reason
                    )
                    self._emit(
                        emit,
                        {
                            "type": "warning",
                            "step": "resolve_source_datamodels",
                            "message": "Stopping pagination due to a non-200 response. Proceeding with datamodels already retrieved.",
                            "status_code": status_code,
                            "raw_error": reason,
                            "pages_fetched": pages_fetched,
                            "found_so_far": len(found_title_to_oid),
                        },
                    )
                    break

                payload, _ = self._safe_json(response)

                items: List[Dict[str, Any]] = []
                if isinstance(payload, list):
                    items = payload
                elif isinstance(payload, dict):
                    for key in ("items", "datamodels", "results", "data"):
                        v = payload.get(key)
                        if isinstance(v, list):
                            items = v
                            break

                if not items:
                    break

                pages_fetched += 1
                total_items_seen += len(items)

                for dm in items:
                    if not isinstance(dm, dict):
                        continue
                    t = dm.get("title")
                    oid = dm.get("oid")
                    if not (isinstance(t, str) and isinstance(oid, str)):
                        continue
                    if t not in wanted:
                        continue

                    if t in found_title_to_oid:
                        duplicate_titles[t] = duplicate_titles.get(t, 1) + 1
                        continue

                    found_title_to_oid[t] = oid
                    found_title_to_type[t] = dm.get("type")

                if len(items) < limit:
                    break

                if len(found_title_to_oid) >= len(wanted) and wanted:
                    break

                skip += limit

            if duplicate_titles:
                self.logger.warning("Duplicate datamodel titles detected during resolve: %s", duplicate_titles)
                self._emit(
                    emit,
                    {
                        "type": "warning",
                        "step": "resolve_source_datamodels",
                        "message": "Duplicate datamodel titles detected in source. Using the first match per title.",
                        "duplicates": duplicate_titles,
                    },
                )

            self.logger.info(
                "Retrieved datamodels from source environment. pages_fetched=%s total_items_seen=%s",
                pages_fetched,
                total_items_seen,
            )

            for name in (datamodel_names or []):
                if name not in found_title_to_oid:
                    reason = f"Datamodel '{name}' not found in source."
                    result["failed"].append({"title": name, "source_id": None, "reason": reason})
                    result["meta"]["failure_reasons"][name] = reason
                    self.logger.error(reason)
                    self._emit(
                        emit,
                        {
                            "type": "warning",
                            "step": "resolve_source_datamodels",
                            "message": reason,
                            "title": name,
                        },
                    )

            for title, oid in found_title_to_oid.items():
                resolved_ids.append(oid)
                id_to_title[oid] = title

            result["meta"]["resolved_count"] = len(resolved_ids)

        if not resolved_ids:
            self.logger.warning("No datamodels resolved for migration.")
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "No datamodels found to migrate.",
                    "status": "noop",
                    "total_count": 0,
                },
            )
            result.update(
                {
                    "ok": True,
                    "status": "noop",
                    "total_count": 0,
                    "succeeded_count": 0,
                    "skipped_count": 0,
                    "failed_count": len(result["failed"]),
                    "raw_error": None,
                }
            )
            return result

        # -------------------------
        # Step 2: Export schemas from source
        # -------------------------
        all_datamodel_data: List[Dict[str, Any]] = []

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "export_datamodels",
                "message": "Exporting datamodel schemas from source.",
                "resolved_count": len(resolved_ids),
            },
        )

        for idx, datamodel_id in enumerate(resolved_ids, start=1):
            result["meta"]["export_requested"] += 1

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "export_datamodels",
                    "message": "Exporting datamodel schema.",
                    "current": idx,
                    "total": len(resolved_ids),
                    "source_id": datamodel_id,
                    "title": id_to_title.get(datamodel_id),
                },
            )

            response = self.source_client.get(
                "/api/v2/datamodel-exports/schema",
                params={
                    "datamodelId": datamodel_id,
                    "type": "schema-latest",
                    "dependenciesIdsToInclude": ",".join(api_dependencies),
                },
            )

            # Keep existing behavior, but cover edge-case where Response is falsy for 4xx/5xx.
            if response is not None and response.status_code == 200:
                data_model_json, _ = self._safe_json(response)
                if not isinstance(data_model_json, dict):
                    reason = "Export returned non-dict JSON"
                    self.logger.error("Failed to export datamodel_id=%s. Reason: %s", datamodel_id, reason)
                    result["meta"]["export_failed"] += 1
                    title = id_to_title.get(datamodel_id)
                    result["failed"].append({"title": title, "source_id": datamodel_id, "reason": reason})
                    result["meta"]["failure_reasons"][title or datamodel_id] = reason
                    continue

                title = data_model_json.get("title", id_to_title.get(datamodel_id, "Unknown Title"))
                self.logger.info("Successfully fetched data model name %s.", title)
                self.logger.debug("Successfully fetched data model ID %s.", datamodel_id)

                all_datamodel_data.append(data_model_json)
                result["meta"]["export_succeeded"] += 1
                if isinstance(title, str):
                    id_to_title[datamodel_id] = title
            else:
                reason = self._extract_error_detail(response)
                self.logger.error("Failed to fetch data model ID %s. Error: %s", datamodel_id, reason)
                result["meta"]["export_failed"] += 1
                title = id_to_title.get(datamodel_id)
                result["failed"].append({"title": title, "source_id": datamodel_id, "reason": reason})
                result["meta"]["failure_reasons"][title or datamodel_id] = reason

        if not all_datamodel_data:
            self.logger.warning("No data models were successfully retrieved for migration.")
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "No datamodels were exported successfully. Nothing to import.",
                    "status": "failed",
                    "total_count": 0,
                },
            )
            result.update(
                {
                    "ok": False,
                    "status": "failed",
                    "total_count": 0,
                    "succeeded_count": 0,
                    "skipped_count": 0,
                    "failed_count": len(result["failed"]),
                    "raw_error": None,
                }
            )
            return result

        # -------------------------
        # Step 3: Import schemas into target (one-by-one)
        # -------------------------
        self.logger.info("Migrating '%s' datamodels one by one to the target environment.", len(all_datamodel_data))

        successfully_migrated_datamodels: List[Dict[str, Any]] = []
        source_to_target_id: Dict[str, str] = {}

        import_url = "/api/v2/datamodel-imports/schema"

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "import_datamodels",
                "message": "Importing datamodel schemas into target.",
                "exported_count": len(all_datamodel_data),
            },
        )

        for idx, data_model in enumerate(all_datamodel_data, start=1):
            src_id = data_model.get("oid") if isinstance(data_model, dict) else None
            title = data_model.get("title") if isinstance(data_model, dict) else None
            src_id_str = src_id if isinstance(src_id, str) else None
            title_str = title if isinstance(title, str) else None

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "import_datamodels",
                    "message": "Preparing datamodel for import.",
                    "current": idx,
                    "total": len(all_datamodel_data),
                    "source_id": src_id_str,
                    "title": title_str,
                },
            )

            for dataset in data_model.get("datasets", []):
                connection = dataset.get("connection")

                if connection and isinstance(connection, dict):
                    provider = connection.get("provider")

                    if provider_connection_map and provider in provider_connection_map:
                        dataset["connection"] = {"oid": provider_connection_map[provider], "provider": provider}
                    else:
                        if "parameters" in connection:
                            connection["parameters"] = ""

            self.logger.debug("Data model after processing connections: %s", data_model)
            datasets_log = data_model.get("datasets", [])
            if datasets_log:
                self.logger.debug("Connection object: %s", datasets_log[0].get("connection", {}))
            else:
                self.logger.warning("No datasets found in data model: %s", data_model.get("title", "Unknown Title"))

            query_string = ""
            if action == "overwrite":
                query_string = f"?datamodelId={data_model.get('oid')}"
            elif action == "duplicate":
                new_model_title = new_title or f"{data_model.get('title', 'Untitled')} (Duplicate)"
                query_string = f"?newTitle={new_model_title}"

            try:
                response = self.target_client.post(f"{import_url}{query_string}", data=data_model)

                target_id: Optional[str] = None
                resp_payload, _ = self._safe_json(response)
                if isinstance(resp_payload, dict):
                    for k in ("oid", "id", "datamodelId"):
                        v = resp_payload.get(k)
                        if isinstance(v, str):
                            target_id = v
                            break

                # Keep existing behavior, but cover edge-case where Response is falsy for 4xx/5xx.
                if response is not None and response.status_code == 201:
                    self.logger.info("Successfully migrated data model: %s", data_model.get("title"))
                    result["succeeded"].append(
                        {"title": title_str, "source_id": src_id_str, "target_id": target_id, "reason": None}
                    )
                    successfully_migrated_datamodels.append(data_model)
                    result["meta"]["import_succeeded"] += 1
                    if src_id_str and target_id:
                        source_to_target_id[src_id_str] = target_id

                    self._emit(
                        emit,
                        {
                            "type": "progress",
                            "step": "import_datamodels",
                            "message": "Datamodel imported successfully.",
                            "source_id": src_id_str,
                            "target_id": target_id,
                            "title": title_str,
                        },
                    )

                elif response is not None and response.status_code == 404 and action == "overwrite":
                    fallback_reason = (
                        f"Data model '{data_model.get('title')}' not found in target for overwrite. "
                        f"Retrying without overwrite option."
                    )
                    self.logger.warning(fallback_reason)

                    self._emit(
                        emit,
                        {
                            "type": "warning",
                            "step": "import_datamodels",
                            "message": "Overwrite failed with 404. Retrying without overwrite.",
                            "source_id": src_id_str,
                            "title": title_str,
                        },
                    )

                    fallback_response = self.target_client.post(import_url, data=data_model)

                    fb_target_id: Optional[str] = None
                    fb_payload, _ = self._safe_json(fallback_response)
                    if isinstance(fb_payload, dict):
                        for k in ("oid", "id", "datamodelId"):
                            v = fb_payload.get(k)
                            if isinstance(v, str):
                                fb_target_id = v
                                break

                    if fallback_response is not None and fallback_response.status_code == 201:
                        self.logger.info("Successfully migrated datamodel without overwrite: %s", data_model.get("title"))
                        result["succeeded"].append(
                            {"title": title_str, "source_id": src_id_str, "target_id": fb_target_id, "reason": None}
                        )
                        successfully_migrated_datamodels.append(data_model)
                        result["meta"]["import_succeeded"] += 1
                        if src_id_str and fb_target_id:
                            source_to_target_id[src_id_str] = fb_target_id

                    elif (
                        fallback_response is not None
                        and fallback_response.status_code == 400
                        and isinstance(fb_payload, dict)
                        and fb_payload.get("title") == "ElasticubeAlreadyExists"
                    ):
                        final_reason = (
                            f"Datamodel '{data_model.get('title')}' already exists on the target with a different ID. "
                            "Consider using action='duplicate' with a new title, or delete the existing model manually."
                        )
                        self.logger.error(final_reason)
                        result["failed"].append({"title": title_str, "source_id": src_id_str, "reason": final_reason})
                        result["meta"]["failure_reasons"][title_str or (src_id_str or "unknown")] = final_reason
                        result["meta"]["import_failed"] += 1

                    else:
                        error_message = self._extract_error_detail(fallback_response)
                        self.logger.error(
                            "Fallback failed to migrate data model: %s. Error: %s",
                            data_model.get("title"),
                            error_message,
                        )
                        result["failed"].append({"title": title_str, "source_id": src_id_str, "reason": error_message})
                        result["meta"]["failure_reasons"][title_str or (src_id_str or "unknown")] = error_message
                        result["meta"]["import_failed"] += 1

                else:
                    error_message = self._extract_error_detail(response)
                    self.logger.error(
                        "Failed to migrate data model: %s. Error: %s", data_model.get("title"), error_message
                    )
                    result["failed"].append({"title": title_str, "source_id": src_id_str, "reason": error_message})
                    result["meta"]["failure_reasons"][title_str or (src_id_str or "unknown")] = error_message
                    result["meta"]["import_failed"] += 1

            except Exception as e:
                reason = f"Exception occurred: {str(e)}"
                self.logger.error("Exception while migrating data model '%s': %s", data_model.get("title"), reason)
                result["failed"].append({"title": title_str, "source_id": src_id_str, "reason": reason})
                result["meta"]["failure_reasons"][title_str or (src_id_str or "unknown")] = reason
                result["meta"]["import_failed"] += 1

                self._emit(
                    emit,
                    {
                        "type": "error",
                        "step": "import_datamodels",
                        "message": "Exception while importing datamodel.",
                        "source_id": src_id_str,
                        "title": title_str,
                        "error": reason,
                    },
                )

        self.logger.info(
            "Data model migration completed. Success: %s, Failed: %s",
            result["meta"]["import_succeeded"],
            result["meta"]["import_failed"],
        )

        # -------------------------
        # Step 4: Shares migration (optional)
        # -------------------------
        if shares:
            self._emit(
                emit,
                {"type": "progress", "step": "migrate_shares", "message": "Starting datamodel shares migration."},
            )

            self.logger.info("Processing shares for the migrated datamodels.")

            self.logger.debug("Fetching userIds from source system")
            source_user_resp = self.source_client.get("/api/v1/users")
            source_user_ids: Dict[str, str] = {}
            if source_user_resp is not None and source_user_resp.status_code == 200:
                payload, _ = self._safe_json(source_user_resp)
                if isinstance(payload, list):
                    source_user_ids = {
                        user.get("email"): user.get("_id")
                        for user in payload
                        if isinstance(user, dict) and user.get("email")
                    }
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")

            self.logger.debug("Fetching userIds from target system")
            target_user_resp = self.target_client.get("/api/v1/users")
            target_user_ids: Dict[str, str] = {}
            if target_user_resp is not None and target_user_resp.status_code == 200:
                payload, _ = self._safe_json(target_user_resp)
                if isinstance(payload, list):
                    target_user_ids = {
                        user.get("email"): user.get("_id")
                        for user in payload
                        if isinstance(user, dict) and user.get("email")
                    }
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")

            user_mapping = {source_user_ids[email]: target_user_ids.get(email) for email in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_resp = self.source_client.get("/api/v1/groups")
            source_group_ids: Dict[str, str] = {}
            if source_group_resp is not None and source_group_resp.status_code == 200:
                payload, _ = self._safe_json(source_group_resp)
                if isinstance(payload, list):
                    source_group_ids = {
                        group.get("name"): group.get("_id")
                        for group in payload
                        if isinstance(group, dict)
                        and group.get("name") not in ["Everyone", "All users in system"]
                        and group.get("_id")
                    }
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")

            self.logger.debug("Fetching groups from target system")
            target_group_resp = self.target_client.get("/api/v1/groups")
            target_group_ids: Dict[str, str] = {}
            if target_group_resp is not None and target_group_resp.status_code == 200:
                payload, _ = self._safe_json(target_group_resp)
                if isinstance(payload, list):
                    target_group_ids = {
                        group.get("name"): group.get("_id")
                        for group in payload
                        if isinstance(group, dict)
                        and group.get("name") not in ["Everyone", "All users in system"]
                        and group.get("_id")
                    }
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")

            group_mapping = {source_group_ids[name]: target_group_ids.get(name) for name in source_group_ids}

            target_models_by_title: Dict[str, Dict[str, Any]] = {}
            try:
                t_limit = 100
                t_skip = 0
                while True:
                    target_list_resp = self.target_client.get(
                        "/api/v2/datamodels/schema",
                        params={"fields": "oid,title,type", "limit": t_limit, "skip": t_skip},
                    )
                    if target_list_resp is None or target_list_resp.status_code != 200:
                        break

                    payload, _ = self._safe_json(target_list_resp)

                    items: List[Dict[str, Any]] = []
                    if isinstance(payload, list):
                        items = payload
                    elif isinstance(payload, dict):
                        for key in ("items", "datamodels", "results", "data"):
                            v = payload.get(key)
                            if isinstance(v, list):
                                items = v
                                break

                    if not items:
                        break

                    for dm in items:
                        if not isinstance(dm, dict):
                            continue
                        t = dm.get("title")
                        if isinstance(t, str):
                            target_models_by_title[t] = dm

                    if len(items) < t_limit:
                        break

                    t_skip += t_limit
            except Exception:
                target_models_by_title = {}

            if successfully_migrated_datamodels:
                for datamodel in successfully_migrated_datamodels:
                    src_id = datamodel.get("oid")
                    title = datamodel.get("title")
                    dm_type = datamodel.get("type")

                    src_id_str = src_id if isinstance(src_id, str) else None
                    title_str = title if isinstance(title, str) else None

                    target_id: Optional[str] = None
                    if src_id_str and src_id_str in source_to_target_id:
                        target_id = source_to_target_id[src_id_str]
                    elif title_str and title_str in target_models_by_title:
                        oid = target_models_by_title[title_str].get("oid")
                        if isinstance(oid, str):
                            target_id = oid

                    if dm_type == "extract":
                        datamodel_shares_response = self.source_client.get(
                            f"/api/elasticubes/localhost/{title_str}/permissions"
                        )
                        shares_payload, _ = self._safe_json(datamodel_shares_response)
                        datamodel_shares = (
                            shares_payload.get("shares", [])
                            if isinstance(shares_payload, dict) and isinstance(shares_payload.get("shares"), list)
                            else []
                        )
                    elif dm_type == "live" and src_id_str:
                        datamodel_shares_response = self.source_client.get(
                            f"/api/v1/elasticubes/live/{src_id_str}/permissions"
                        )
                        shares_payload, _ = self._safe_json(datamodel_shares_response)
                        datamodel_shares = shares_payload if isinstance(shares_payload, list) else []
                    else:
                        self.logger.warning("Unknown datamodel type for: %s", title_str)
                        continue

                    if datamodel_shares_response is None or datamodel_shares_response.status_code != 200:
                        err = self._extract_error_detail(datamodel_shares_response)
                        self.logger.error(
                            "Failed to fetch shares for datamodel: '%s' (ID: %s). Error: %s",
                            title_str,
                            src_id_str,
                            err,
                        )
                        result["meta"]["share_fail_count"] += 1
                        continue

                    if datamodel_shares:
                        new_shares: List[Dict[str, Any]] = []
                        for share in datamodel_shares:
                            if not isinstance(share, dict):
                                continue

                            if share.get("type") == "user":
                                new_share_user_id = user_mapping.get(share.get("partyId"))
                                if new_share_user_id:
                                    new_shares.append(
                                        {
                                            "partyId": new_share_user_id,
                                            "type": "user",
                                            "permission": share.get("permission", "a"),
                                        }
                                    )
                            elif share.get("type") == "group":
                                new_share_group_id = group_mapping.get(share.get("partyId"))
                                if new_share_group_id:
                                    new_shares.append(
                                        {
                                            "partyId": new_share_group_id,
                                            "type": "group",
                                            "permission": share.get("permission", "a"),
                                        }
                                    )

                        share_count = len(new_shares)
                        if share_count > 0:
                            response = None

                            if dm_type == "extract":
                                response = self.target_client.put(
                                    f"/api/elasticubes/localhost/{title_str}/permissions", data=new_shares
                                )

                            elif dm_type == "live":
                                if not target_id:
                                    self.logger.error(
                                        "Cannot update shares for live datamodel '%s' because target_id could not be resolved.",
                                        title_str,
                                    )
                                    result["meta"]["share_fail_count"] += 1
                                    continue

                                self.logger.info("Publishing datamodel '%s' to update shares.", title_str)
                                publish_response = self.target_client.post(
                                    "/api/v2/builds", data={"datamodelId": target_id, "buildType": "publish"}
                                )

                                if publish_response is not None and publish_response.status_code == 201:
                                    self.logger.info(
                                        "Datamodel '%s' published successfully. Now updating shares.", title_str
                                    )
                                    response = self.target_client.patch(
                                        f"/api/v1/elasticubes/live/{target_id}/permissions", data=new_shares
                                    )
                                else:
                                    self.logger.error(
                                        "Failed to publish datamodel '%s'. Error: %s",
                                        title_str,
                                        self._extract_error_detail(publish_response),
                                    )
                                    response = None

                            if response is not None and response.status_code in [200, 201]:
                                self.logger.info("Datamodel '%s' shares migrated successfully.", title_str)
                                result["meta"]["share_success_count"] += share_count
                                result["meta"]["share_details"][title_str] = share_count
                            else:
                                self.logger.error(
                                    "Failed to migrate shares for datamodel: %s. Error: %s",
                                    title_str,
                                    self._extract_error_detail(response),
                                )
                                result["meta"]["share_fail_count"] += 1
                        else:
                            self.logger.warning("No valid shares found for datamodel: %s.", title_str)

            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "migrate_shares",
                    "message": "Completed datamodel shares migration.",
                    "shares_migrated": result["meta"]["share_success_count"],
                    "shares_failed": result["meta"]["share_fail_count"],
                },
            )

        # -------------------------
        # Finalize return (dashboard-consistent)
        # -------------------------
        succeeded_count = len(result["succeeded"])
        skipped_count = len(result["skipped"])
        failed_count = len(result["failed"])

        total_count = result["meta"]["resolved_count"]
        if total_count == 0 and failed_count == 0:
            ok = True
            status = "noop"
        else:
            ok = (failed_count == 0)
            status = "success" if ok else "failed"

        self.logger.info("Finished data model migration.")
        self.logger.info(result)

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished migrating datamodels.",
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "shares_migrated": result["meta"].get("share_success_count", 0),
                "shares_failed": result["meta"].get("share_fail_count", 0),
            },
        )

        result.update(
            {
                "ok": ok,
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "raw_error": None,
            }
        )
        return result

    def migrate_all_datamodels(
        self,
        dependencies: Optional[Union[List[str], str]] = None,
        shares: bool = False,
        batch_size: int = 10,
        sleep_time: int = 5,
        action: Optional[str] = None,
        emit: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Migrates all data models from the source environment to the target environment in batches.

        Parameters
        ----------
        dependencies : list[str] or str or None, default None
            Dependencies to include in the migration. If None or "all", all supported dependencies
            are included by default.

            Supported values:
            - "dataSecurity" (includes both Data Security and Scope Configuration)
            - "formulas" (for Formulas)
            - "hierarchies" (for Drill Hierarchies)
            - "perspectives" (for Perspectives)

        shares : bool, default False
            Whether to also migrate data model shares after the schema import.

        batch_size : int, default 10
            Number of data models to migrate per batch.

        sleep_time : int, default 5
            Time (in seconds) to sleep between batches.

        action : str or None, default None
            Strategy to handle existing data models in the target environment.

            - "overwrite": Attempts to overwrite an existing model using its original ID via the
            ``datamodelId`` parameter. If the model is not found in the target environment, it
            falls back to creating the model.
            - "duplicate": Creates a new model by appending " (Duplicate)" to the original name.

        emit : Callable[[dict], None] or None, default None
            Optional callback invoked with structured progress events. If not provided, the method
            emits no events and only returns a final result.

            Event payloads follow a consistent shape:
            - ``type``: str ("started" | "progress" | "warning" | "error" | "completed")
            - ``step``: str logical step identifier
            - ``message``: str human-readable message
            - Additional fields depending on the step (counts, status_code, etc.)

        Returns
        -------
        dict
            A migration summary containing:
            - ``succeeded``: list
            - ``skipped``: list
            - ``failed``: list
            - Counts/metadata fields (for example: ``total_count``, ``batches_total``, etc.)
        """
        self._emit(
            emit,
            {"type": "started", "step": "init", "message": "Starting datamodel migration from source to target."},
        )

        self.logger.info("Starting migration of all data models from source to target.")
        self.logger.debug(
            "Input Parameters: dependencies=%s, shares=%s, batch_size=%s, sleep_time=%s, action=%s",
            dependencies,
            shares,
            batch_size,
            sleep_time,
            action,
        )

        # Step 1: Fetch all data models
        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_source_datamodels",
                "message": "Fetching all datamodels from source environment.",
                "fields": "oid,title",
            },
        )

        all_datamodel_ids: List[str] = []
        limit = 100
        skip = 0
        pages_fetched = 0
        total_items_seen = 0
        missing_oid_count = 0
        duplicate_oid_count = 0
        duplicate_oids_sample: List[str] = []
        seen_ids: set = set()

        while True:
            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "fetch_source_datamodels",
                    "message": "Fetching datamodels page from source environment.",
                    "limit": limit,
                    "skip": skip,
                    "pages_fetched": pages_fetched,
                    "total_unique_so_far": len(all_datamodel_ids),
                },
            )

            response = self.source_client.get(
                "/api/v2/datamodels/schema",
                params={"fields": "oid,title", "limit": limit, "skip": skip},
            )

            # Important: requests.Response is falsy for 4xx/5xx. Use `is None` checks instead of truthiness.
            if response is None or response.status_code != 200:
                status_code = getattr(response, "status_code", None)
                raw_error = self._extract_error_detail(response)

                self.logger.error("Failed to fetch data models. Status=%s", status_code)
                self.logger.error("Raw error response: %s", raw_error)

                self._emit(
                    emit,
                    {
                        "type": "error" if pages_fetched == 0 else "warning",
                        "step": "fetch_source_datamodels",
                        "message": "Failed to retrieve datamodels from the source environment."
                        if pages_fetched == 0
                        else "Stopping pagination due to a non-200 response. Proceeding with datamodels already retrieved.",
                        "status_code": status_code,
                        "raw_error": raw_error,
                        "pages_fetched": pages_fetched,
                        "retrieved_so_far": len(all_datamodel_ids),
                    },
                )

                if pages_fetched == 0:
                    return {
                        "ok": False,
                        "status": "failed",
                        "succeeded": [],
                        "skipped": [],
                        "failed": [],
                        "total_count": 0,
                        "succeeded_count": 0,
                        "skipped_count": 0,
                        "failed_count": 0,
                        "batches_total": 0,
                        "batch_errors_count": 0,
                        "batch_errors": [],
                        "raw_error": raw_error,
                    }
                break

            payload, _ = self._safe_json(response)

            items: List[Dict[str, Any]] = []
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                for key in ("items", "datamodels", "results", "data"):
                    v = payload.get(key)
                    if isinstance(v, list):
                        items = v
                        break

            if not items:
                break

            pages_fetched += 1
            total_items_seen += len(items)

            for dm in items:
                oid = None
                try:
                    oid = dm.get("oid") if isinstance(dm, dict) else None
                except Exception:
                    oid = None

                if not oid or not isinstance(oid, str):
                    missing_oid_count += 1
                    continue

                if oid in seen_ids:
                    duplicate_oid_count += 1
                    if len(duplicate_oids_sample) < 20:
                        duplicate_oids_sample.append(oid)
                    continue

                seen_ids.add(oid)
                all_datamodel_ids.append(oid)

            if len(items) < limit:
                break

            skip += limit

        total_count = len(all_datamodel_ids)
        self.logger.info("Retrieved %s data models from the source environment.", total_count)

        if missing_oid_count > 0 or duplicate_oid_count > 0:
            self.logger.warning(
                "Datamodel fetch anomalies: total_items_seen=%s missing_oid_count=%s duplicate_oid_count=%s duplicate_sample=%s",
                total_items_seen,
                missing_oid_count,
                duplicate_oid_count,
                duplicate_oids_sample,
            )

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "fetch_source_datamodels",
                "message": "Finished fetching datamodels from source environment.",
                "total_count": total_count,
                "pages_fetched": pages_fetched,
                "total_items_seen": total_items_seen,
                "missing_oid_count": missing_oid_count,
                "duplicate_oid_count": duplicate_oid_count,
            },
        )

        if total_count == 0:
            self._emit(
                emit,
                {
                    "type": "completed",
                    "step": "done",
                    "message": "No datamodels found to migrate.",
                    "status": "noop",
                    "total_count": 0,
                },
            )
            return {
                "ok": True,
                "status": "noop",
                "succeeded": [],
                "skipped": [],
                "failed": [],
                "total_count": 0,
                "succeeded_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
                "batches_total": 0,
                "batch_errors_count": 0,
                "batch_errors": [],
                "raw_error": None,
            }

        # Step 2: Migrate datamodels in batches
        migration_summary: Dict[str, Any] = {"succeeded": [], "skipped": [], "failed": []}
        batch_errors: List[Dict[str, Any]] = []

        batches_total = (total_count + batch_size - 1) // batch_size if batch_size > 0 else 0

        self._emit(
            emit,
            {
                "type": "progress",
                "step": "batch_migration",
                "message": "Starting batch datamodel migration.",
                "total_count": total_count,
                "batch_size": batch_size,
                "batches_total": batches_total,
                "dependencies": dependencies,
                "shares": shares,
                "action": action,
            },
        )

        for i in range(0, total_count, batch_size):
            batch_ids = all_datamodel_ids[i : i + batch_size]
            batch_number = (i // batch_size) + 1

            self.logger.info("Processing batch %s with %s datamodels: %s", batch_number, len(batch_ids), batch_ids)
            self._emit(
                emit,
                {
                    "type": "progress",
                    "step": "batch_migration",
                    "message": "Starting datamodel migration batch.",
                    "batch_number": batch_number,
                    "batches_total": batches_total,
                    "batch_size": len(batch_ids),
                    "processed_so_far": i,
                    "total_count": total_count,
                },
            )

            try:
                batch_result = self.migrate_datamodels(
                    datamodel_ids=batch_ids,
                    dependencies=dependencies,
                    shares=shares,
                    action=action,
                    emit=emit,
                )

                self.logger.info("Batch %s migration summary: %s", batch_number, batch_result)

                migration_summary["succeeded"].extend(batch_result.get("succeeded", []))
                migration_summary["skipped"].extend(batch_result.get("skipped", []))
                migration_summary["failed"].extend(batch_result.get("failed", []))

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "batch_migration",
                        "message": "Completed datamodel migration batch.",
                        "batch_number": batch_number,
                        "succeeded_total": len(migration_summary["succeeded"]),
                        "skipped_total": len(migration_summary["skipped"]),
                        "failed_total": len(migration_summary["failed"]),
                    },
                )

            except Exception as e:
                self.logger.error("Error occurred in batch %s: %s", batch_number, e)

                batch_errors.append({"batch_number": batch_number, "datamodel_ids": batch_ids, "error": str(e)})

                self._emit(
                    emit,
                    {
                        "type": "error",
                        "step": "batch_migration",
                        "message": "Error occurred during datamodel migration batch.",
                        "batch_number": batch_number,
                        "error": str(e),
                    },
                )

                # Salvage mode: retry one-by-one to salvage remaining datamodels in this batch.
                self.logger.warning(
                    "Entering salvage mode for batch %s. Retrying datamodels individually.",
                    batch_number,
                )
                self._emit(
                    emit,
                    {
                        "type": "warning",
                        "step": "batch_migration",
                        "message": "Entering salvage mode: retrying datamodels individually.",
                        "batch_number": batch_number,
                        "datamodels_in_batch": len(batch_ids),
                    },
                )

                for dm_id in batch_ids:
                    try:
                        single_result = self.migrate_datamodels(
                            datamodel_ids=[dm_id],
                            dependencies=dependencies,
                            shares=shares,
                            action=action,
                            emit=emit,
                        )
                        migration_summary["succeeded"].extend(single_result.get("succeeded", []))
                        migration_summary["skipped"].extend(single_result.get("skipped", []))
                        migration_summary["failed"].extend(single_result.get("failed", []))
                    except Exception as e2:
                        self.logger.error(
                            "Salvage retry failed for datamodel %s in batch %s: %s",
                            dm_id,
                            batch_number,
                            e2,
                        )
                        migration_summary["failed"].append(
                            {"title": None, "source_id": dm_id, "reason": f"Salvage retry failed: {str(e2)}"}
                        )

                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "batch_migration",
                        "message": "Completed salvage mode for datamodel batch.",
                        "batch_number": batch_number,
                        "succeeded_total": len(migration_summary["succeeded"]),
                        "skipped_total": len(migration_summary["skipped"]),
                        "failed_total": len(migration_summary["failed"]),
                    },
                )

            if i + batch_size < total_count:
                self.logger.info("Sleeping for %s seconds before processing the next batch.", sleep_time)
                self._emit(
                    emit,
                    {
                        "type": "progress",
                        "step": "sleep",
                        "message": "Sleeping before next datamodel batch.",
                        "sleep_time_seconds": sleep_time,
                        "next_batch_number": batch_number + 1,
                    },
                )
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all data models.")
        self.logger.info("Total Data Models Migrated: %s", len(migration_summary["succeeded"]))
        self.logger.info("Total Data Models Skipped: %s", len(migration_summary["skipped"]))
        self.logger.info("Total Data Models Failed: %s", len(migration_summary["failed"]))
        self.logger.info(migration_summary)

        succeeded_count = len(migration_summary["succeeded"])
        skipped_count = len(migration_summary["skipped"])
        failed_count = len(migration_summary["failed"])
        ok = (total_count > 0) and (failed_count == 0)
        status = "success" if ok else "failed"

        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished migrating all datamodels.",
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "batches_total": batches_total,
                "batch_errors_count": len(batch_errors),
                "missing_oid_count": missing_oid_count,
            },
        )

        migration_summary.update(
            {
                "ok": ok,
                "status": status,
                "total_count": total_count,
                "succeeded_count": succeeded_count,
                "skipped_count": skipped_count,
                "failed_count": failed_count,
                "batches_total": batches_total,
                "batch_errors_count": len(batch_errors),
                "batch_errors": batch_errors,
                "raw_error": None,
            }
        )
        return migration_summary
