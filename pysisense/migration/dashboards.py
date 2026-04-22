from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Literal


class DashboardsMigrationMixin:
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

        share_migration_summary = {"new_share_success_count": 0, "share_fail_count": 0, "failed_dashboards": []}

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
        for source_id, target_id in zip(source_dashboard_ids, target_dashboard_ids, strict=False):
            self.logger.info(f"Processing shares for dashboard: Source ID {source_id}, Target ID {target_id}")

            # Fetch shares from the source environment
            dashboard_shares_response = self.source_client.get(f"/api/shares/dashboard/{source_id}?adminAccess=true")
            response_text = dashboard_shares_response.text if dashboard_shares_response else "No response"
            self.logger.debug(f"Response for shares of source dashboard ID {source_id}: {response_text}")
            if not dashboard_shares_response or dashboard_shares_response.status_code != 200:
                self.logger.error(f"Failed to fetch shares for source dashboard ID: {source_id}.")
                share_migration_summary["failed_dashboards"].append({"source_id": source_id, "target_id": target_id})
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
                        new_shares.append(
                            {
                                "shareId": new_share_user_id,
                                "type": "user",
                                "rule": rule,
                                "subscribe": share.get("subscribe", False),
                                "userName": user_email,  # Add email for later duplicate check
                            }
                        )
                        self.logger.debug(f"Prepared user share for migration: {user_email} (Rule: {rule})")
                elif share["type"] == "group":
                    new_share_group_id = group_mapping.get(share["shareId"])
                    group_name = source_group_map.get(share["shareId"], "Unknown Group")
                    if new_share_group_id:
                        new_shares.append(
                            {
                                "shareId": new_share_group_id,
                                "type": "group",
                                "rule": share.get("rule", "viewer"),
                                "subscribe": share.get("subscribe", False),
                                "name": group_name,  # Add group name for later duplicate check
                            }
                        )
                        self.logger.debug(f"Prepared group share for migration: {group_name} (Rule: {share.get('rule', 'viewer')})")

            # Combine new shares with existing ones
            self.logger.debug(f"Fetching shares for target dashboard ID {target_id} with adminAccess=true.")
            target_dashboard_shares_url = f"/api/shares/dashboard/{target_id}?adminAccess=true"
            target_dashboard_shares_response = self.target_client.get(target_dashboard_shares_url)

            if target_dashboard_shares_response is not None:
                if target_dashboard_shares_response.status_code == 403:
                    self.logger.warning(f"Access denied for target dashboard ID {target_id} with adminAccess. Retrying without adminAccess.")
                    target_dashboard_shares_response = self.target_client.get(f"/api/shares/dashboard/{target_id}")
                    if target_dashboard_shares_response and target_dashboard_shares_response.status_code == 200:
                        self.logger.debug(f"Successfully fetched shares for target dashboard ID {target_id} without adminAccess.")
                    else:
                        self.logger.error(f"Retry without adminAccess also failed for target dashboard ID {target_id}. Ending processing for this dashboard.")
                        share_migration_summary["failed_dashboards"].append({"source_id": source_id, "target_id": target_id})
                        share_migration_summary["share_fail_count"] += len(new_shares)
                        dashboard_results.append({"source_id": source_id, "target_id": target_id, "shares_added": 0, "status": "Skipped", "reason": "Target dashboard not found or inaccessible"})
                        continue
                elif target_dashboard_shares_response.status_code == 200:
                    self.logger.debug(f"Shares fetched with adminAccess for target dashboard ID {target_id}.")
                else:
                    self.logger.error(f"Unexpected status code when accessing target dashboard ID {target_id}: {target_dashboard_shares_response.status_code}")
                    share_migration_summary["failed_dashboards"].append({"source_id": source_id, "target_id": target_id})
                    share_migration_summary["share_fail_count"] += len(new_shares)
                    continue
            else:
                self.logger.error(f"Failed to fetch shares for target dashboard ID {target_id}. Response is None. Ending processing for this dashboard.")
                share_migration_summary["failed_dashboards"].append({"source_id": source_id, "target_id": target_id})
                share_migration_summary["share_fail_count"] += len(new_shares)
                continue

            existing_shares = target_dashboard_shares_response.json().get("sharesTo", [])
            # Log simplified existing shares
            simplified_existing = []
            for share in existing_shares:
                if share.get("type") == "user":
                    simplified_existing.append({"type": "user", "userName": share.get("userName", "Unknown")})
                elif share.get("type") == "group":
                    simplified_existing.append({"type": "group", "name": share.get("name", "Unknown Group")})

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
            simplified_filtered = [{"type": share.get("type"), "shareId": share.get("shareId"), "rule": share.get("rule"), "subscribe": share.get("subscribe", False)} for share in filtered_new_shares]
            self.logger.debug(f"Filtered new shares to be added: {simplified_filtered}")

            # Prepare filtered_new_shares for API by removing comparison-only keys
            final_new_shares = []
            for share in filtered_new_shares:
                final_new_shares.append({"shareId": share["shareId"], "type": share["type"], "rule": share["rule"], "subscribe": share.get("subscribe", False)})

            # Combine with existing shares
            all_shares = existing_shares + final_new_shares
            self.logger.debug(f"Total shares to be posted: {len(all_shares)}")
            self.logger.debug(f"Final shares payload: {all_shares}")

            if not all_shares:
                self.logger.warning(f"No valid shares found for source dashboard ID {source_id}. Ensure users and groups exist in the target environment.")
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
                        self.logger.error(f"Retry without adminAccess also failed for POST request to dashboard ID {target_id}. Status Code: {response.status_code if response else 'No response'}")
                elif response.status_code not in [200, 201]:
                    self.logger.error(f"Unexpected status code for POST request to {post_url}: {response.status_code}.")
            else:
                self.logger.error(f"POST request to {post_url} failed. No response received.")

            # Handle the response or fallback logic
            if response and response.status_code in [200, 201]:
                self.logger.info(f"Shares migrated successfully to target dashboard ID {target_id}.")
                share_migration_summary["new_share_success_count"] += len(filtered_new_shares)
            else:
                self.logger.error(f"Failed to migrate shares for target dashboard ID {target_id}. Status Code: {response.status_code if response else 'No response'}")
                share_migration_summary["share_fail_count"] += len(filtered_new_shares)
                share_migration_summary["failed_dashboards"].append({"source_id": source_id, "target_id": target_id})
            dashboard_results.append(
                {"source_id": source_id, "target_id": target_id, "shares_added": len(filtered_new_shares), "status": "Success" if response and response.status_code in [200, 201] else "Failed"}
            )

            # Step 3: Handle ownership change if requested
            self.logger.debug("Starting ownership change process.")

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
                    self.logger.info(f"Target dashboard ID {target_id} already owned by user ID {potential_owner_id}. Skipping ownership change.")
                else:
                    self.logger.info(f"Changing ownership of target dashboard ID {target_id} to user: {potential_owner_name} (ID: {potential_owner_id}).")

                    ownership_url = f"/api/v1/dashboards/{target_id}/change_owner?adminAccess=true"
                    self.logger.debug(f"Making POST request to {ownership_url} for ownership change.")

                    owner_change_response = self.target_client.post(ownership_url, data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"})

                    # Check for 403 and retry without adminAccess
                    if owner_change_response is None or owner_change_response.status_code == 403:
                        self.logger.warning(f"Access denied for ownership change at {ownership_url}. Retrying without adminAccess.")
                        ownership_url_without_admin = f"/api/v1/dashboards/{target_id}/change_owner"
                        self.logger.debug(f"Retrying ownership change POST request to {ownership_url_without_admin}.")
                        owner_change_response = self.target_client.post(ownership_url_without_admin, data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"})

                    # Handle the response after retry logic
                    if owner_change_response and owner_change_response.status_code in [200, 201]:
                        self.logger.info(f"Ownership changed successfully for dashboard ID {target_id}.")
                    else:
                        self.logger.error(f"Failed to change ownership for dashboard ID {target_id}. Status Code: {owner_change_response.status_code if owner_change_response else 'No response'}.")

        self.logger.info("Finished share migration.")
        self.logger.info(share_migration_summary)
        return {
            "summary": {
                "total_dashboard_count": len(source_dashboard_ids),
                "total_share_success_count": share_migration_summary["new_share_success_count"],
                "total_share_fail_count": share_migration_summary["share_fail_count"],
            },
            "dashboard_results": dashboard_results,
        }

    def migrate_dashboards(
        self,
        dashboard_ids: list[str] | None = None,
        dashboard_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] | None = None,
        republish: bool = False,
        migrate_share: bool = False,
        change_ownership: bool = False,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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

        summary: dict[str, Any] = {
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
        bulk_dashboard_data: list[dict[str, Any]] = []
        source_id_to_title: dict[str, str] = {}

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
            dashboards: list[dict[str, Any]] = []

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

                items: list[dict[str, Any]] = []
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
        message_from_payload: str | None = None
        if isinstance(resp_json, dict):
            if isinstance(resp_json.get("message"), str):
                message_from_payload = resp_json["message"]
            elif isinstance(resp_json.get("error"), dict) and isinstance(resp_json["error"].get("message"), str):
                message_from_payload = resp_json["error"]["message"]

        if not request_success:
            # Request-level failure: mark everything as failed (but keep export failures already captured)
            reason = (
                message_from_payload or (json_err if json_err else None) or self._truncate(getattr(resp, "text", "") or "") or f"Bulk import failed (status_code={summary['meta']['bulk_status_code']})"
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
        target_id_to_title: dict[str, str] = {}

        # Build a title -> [source_id, ...] lookup from what we exported (best effort).
        # This lets us attach source_id to succeeded/skipped items
        source_ids_by_title: dict[str, list[str]] = {}
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

            source_id: str | None = None
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

            source_id: str | None = None
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

        self.logger.info(f"Bulk import parsed results: succeeded={len(summary['succeeded'])}, skipped={len(summary['skipped'])}, failed={len(summary['failed'])}.")

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
        source_to_target: dict[str, str] = {}
        title_to_targets: dict[str, list[str]] = {}
        for tid, ttitle in target_id_to_title.items():
            title_to_targets.setdefault(ttitle, []).append(tid)

        for src_id, src_title in source_id_to_title.items():
            targets = title_to_targets.get(src_title, [])
            if not targets:
                self.logger.warning(f"Source dashboard '{src_title}' (source_id={src_id}) not found among succeeded target dashboards.")
                continue
            if len(targets) > 1:
                self.logger.warning(f"Multiple target dashboards share the same title '{src_title}'. Using the first one for shares/ownership migration. target_ids={targets}")
            source_to_target[src_id] = targets[0]

        if not source_to_target:
            self.logger.info("No dashboards eligible for shares/ownership migration (no source->target mapping could be formed).")

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
        action: str | None = None,
        republish: bool = False,
        migrate_share: bool = False,
        change_ownership: bool = False,
        batch_size: int = 10,
        sleep_time: int = 10,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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
        duplicate_oids_sample_global: list[str] = []

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

            items: list[dict[str, Any]] = []
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
            duplicate_oids_sample_page: list[str] = []

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
        all_dashboard_ids_list: list[Any] = sorted(list(all_dashboard_ids))
        migration_summary: dict[str, Any] = {"succeeded": [], "skipped": [], "failed": []}

        total_count = len(all_dashboard_ids_list)
        batches_total = (total_count + batch_size - 1) // batch_size if batch_size > 0 else 0
        batch_errors: list[dict[str, Any]] = []

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
                        migration_summary["failed"].append({"title": None, "source_id": did, "reason": f"Salvage retry failed: {str(e2)}"})

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
