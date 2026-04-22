from __future__ import annotations

from collections.abc import Callable
from typing import Any


class GroupsMigrationMixin:
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
                group_data = {key: value for key, value in group.items() if key not in ["created", "lastUpdated", "tenantId", "_id"]}
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # If no groups match, log an info message and exit early
        if not bulk_group_data:
            self.logger.info("No matching groups found for migration. Ending process.")
            return [{"message": ("No matching groups found for migration. Ending process. Please verify the group names and try again.")}]

        # Step 3: Make the bulk POST request with the group data
        self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
        self.logger.debug(f"Payload for bulk migration: {bulk_group_data}")
        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        # Log the full response at debug level
        self.logger.debug(f"Target environment response status code: {response.status_code if response else 'No response'}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # If response is missing or empty
        if response is None:
            self.logger.error("No response received from the migration API.")
            return {"results": [{"name": group["name"], "status": "Failed"} for group in bulk_group_data], "raw_error": "No response received from the migration API."}
        elif not response.text.strip():
            self.logger.error(f"Empty response body received. Status code: {response.status_code}")
            return {"results": [{"name": group["name"], "status": "Failed"} for group in bulk_group_data], "raw_error": f"Empty response body. Status code: {response.status_code}"}

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
        self.logger.info(f"Finished migrating groups. Successfully migrated {success_count} out of {len(bulk_group_data)} groups.")

        # Return results and raw error if any
        return {"results": migration_results, "total_count": len(bulk_group_data), "raw_error": raw_error}

    def migrate_all_groups(
        self,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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
        warnings: list[str] = []
        migration_results: list[dict[str, str]] = []
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
        self._emit(
            emit,
            {"type": "progress", "step": "fetch_source_groups", "message": "Retrieved groups from the source environment.", "source_count": source_count},
        )

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
        bulk_group_data: list[dict[str, Any]] = []
        skipped_count = 0
        skipped_multi_tenant_count = 0

        self._emit(
            emit,
            {"type": "progress", "step": "filter_groups", "message": "Filtering groups and preparing bulk payload.", "excluded_names": sorted(excluded_names)},
        )

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
            self._emit(
                emit, {"type": "completed", "step": "filter_groups", "message": "No eligible groups found for migration.", "status": "noop", "eligible_count": 0, "skipped_count": skipped_count}
            )

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
        self._emit(
            emit,
            {"type": "progress", "step": "bulk_post", "message": "Sending bulk migration request.", "eligible_count": eligible_count},
        )

        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        status_code = self._safe_status_code(response)
        self.logger.debug("Target environment response status code: %s", status_code if status_code is not None else "No response")
        self.logger.debug("Target environment response body: %s", response.text if response is not None and hasattr(response, "text") else "No response body")

        self._emit(
            emit,
            {"type": "progress", "step": "bulk_post", "message": "Received response from target bulk endpoint.", "status_code": status_code},
        )
        # Step 4: Handle the response from the bulk API call
        if response is not None and status_code == 201:
            try:
                response_data = response.json()
                self.logger.info("Bulk migration succeeded.")
                self._emit(
                    emit,
                    {"type": "progress", "step": "process_response", "message": "Processing bulk migration response.", "status_code": status_code},
                )

                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    migration_results.append({"name": group_name, "status": "Success"})
            except Exception:
                warn = "Bulk response was not valid JSON; assuming migration succeeded based on status code."
                warnings.append(warn)
                self.logger.warning(warn)
                self._emit(
                    emit,
                    {"type": "warning", "step": "process_response", "message": warn},
                )
                migration_results = [{"name": gd.get("name", "Unknown Group"), "status": "Success"} for gd in bulk_group_data]
        else:
            raw_error = self._safe_error_payload(response, context="bulk_post")
            self.logger.error("Bulk migration failed. Status code: %s", status_code if status_code is not None else "No response")
            self.logger.error("Raw error response: %s", raw_error)

            # Optional: extract existingGroups when present (Sisense bulk error shape)
            existing_groups: list[str] = []
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
