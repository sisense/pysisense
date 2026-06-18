from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


class DatamodelsMigrationMixin:
    def migrate_datamodels(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        provider_connection_map: dict[str, str] | None = None,
        dependencies: list[str] | str | None = None,
        shares: bool = False,
        action: str | None = None,
        new_title: str | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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
        dependency_mapping: dict[str, list[str]] = {
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
        result: dict[str, Any] = {
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
        resolved_ids: list[str] = []
        id_to_title: dict[str, str] = {}

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
            found_title_to_oid: dict[str, str] = {}
            found_title_to_type: dict[str, Any] = {}
            duplicate_titles: dict[str, int] = {}

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

                    self.logger.warning("Stopping pagination due to non-200 response. Status=%s Error=%s", status_code, reason)
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

                items: list[dict[str, Any]] = []
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

            for name in datamodel_names or []:
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
        all_datamodel_data: list[dict[str, Any]] = []

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

            source_os = self.source_client.operating_system
            if source_os == "windows":
                if api_dependencies:
                    self.logger.warning(
                        "Windows datamodel export does not support dependenciesIdsToInclude — dependencies (%s) will not be migrated for datamodel_id=%s.",
                        api_dependencies,
                        datamodel_id,
                    )
                response = self.source_client.get(f"/api/v1/elasticubes/{datamodel_id}/datamodel-exports/stream/schema")
            else:
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

        successfully_migrated_datamodels: list[dict[str, Any]] = []
        source_to_target_id: dict[str, str] = {}

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

                target_id: str | None = None
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
                    result["succeeded"].append({"title": title_str, "source_id": src_id_str, "target_id": target_id, "reason": None})
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
                    fallback_reason = f"Data model '{data_model.get('title')}' not found in target for overwrite. Retrying without overwrite option."
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

                    fb_target_id: str | None = None
                    fb_payload, _ = self._safe_json(fallback_response)
                    if isinstance(fb_payload, dict):
                        for k in ("oid", "id", "datamodelId"):
                            v = fb_payload.get(k)
                            if isinstance(v, str):
                                fb_target_id = v
                                break

                    if fallback_response is not None and fallback_response.status_code == 201:
                        self.logger.info("Successfully migrated datamodel without overwrite: %s", data_model.get("title"))
                        result["succeeded"].append({"title": title_str, "source_id": src_id_str, "target_id": fb_target_id, "reason": None})
                        successfully_migrated_datamodels.append(data_model)
                        result["meta"]["import_succeeded"] += 1
                        if src_id_str and fb_target_id:
                            source_to_target_id[src_id_str] = fb_target_id

                    elif fallback_response is not None and fallback_response.status_code == 400 and isinstance(fb_payload, dict) and fb_payload.get("title") == "ElasticubeAlreadyExists":
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
                    self.logger.error("Failed to migrate data model: %s. Error: %s", data_model.get("title"), error_message)
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
            source_user_ids: dict[str, str] = {}
            if source_user_resp is not None and source_user_resp.status_code == 200:
                payload, _ = self._safe_json(source_user_resp)
                if isinstance(payload, list):
                    source_user_ids = {user.get("email"): user.get("_id") for user in payload if isinstance(user, dict) and user.get("email")}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")

            self.logger.debug("Fetching userIds from target system")
            target_user_resp = self.target_client.get("/api/v1/users")
            target_user_ids: dict[str, str] = {}
            if target_user_resp is not None and target_user_resp.status_code == 200:
                payload, _ = self._safe_json(target_user_resp)
                if isinstance(payload, list):
                    target_user_ids = {user.get("email"): user.get("_id") for user in payload if isinstance(user, dict) and user.get("email")}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")

            user_mapping = {source_user_ids[email]: target_user_ids.get(email) for email in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_resp = self.source_client.get("/api/v1/groups")
            source_group_ids: dict[str, str] = {}
            if source_group_resp is not None and source_group_resp.status_code == 200:
                payload, _ = self._safe_json(source_group_resp)
                if isinstance(payload, list):
                    source_group_ids = {
                        group.get("name"): group.get("_id") for group in payload if isinstance(group, dict) and group.get("name") not in ["Everyone", "All users in system"] and group.get("_id")
                    }
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")

            self.logger.debug("Fetching groups from target system")
            target_group_resp = self.target_client.get("/api/v1/groups")
            target_group_ids: dict[str, str] = {}
            if target_group_resp is not None and target_group_resp.status_code == 200:
                payload, _ = self._safe_json(target_group_resp)
                if isinstance(payload, list):
                    target_group_ids = {
                        group.get("name"): group.get("_id") for group in payload if isinstance(group, dict) and group.get("name") not in ["Everyone", "All users in system"] and group.get("_id")
                    }
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")

            group_mapping = {source_group_ids[name]: target_group_ids.get(name) for name in source_group_ids}

            target_models_by_title: dict[str, dict[str, Any]] = {}
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

                    items: list[dict[str, Any]] = []
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

                    target_id: str | None = None
                    if src_id_str and src_id_str in source_to_target_id:
                        target_id = source_to_target_id[src_id_str]
                    elif title_str and title_str in target_models_by_title:
                        oid = target_models_by_title[title_str].get("oid")
                        if isinstance(oid, str):
                            target_id = oid

                    if dm_type == "extract":
                        datamodel_shares_response = self.source_client.get(f"/api/elasticubes/localhost/{title_str}/permissions")
                        shares_payload, _ = self._safe_json(datamodel_shares_response)
                        datamodel_shares = shares_payload.get("shares", []) if isinstance(shares_payload, dict) and isinstance(shares_payload.get("shares"), list) else []
                    elif dm_type == "live" and src_id_str:
                        datamodel_shares_response = self.source_client.get(f"/api/v1/elasticubes/live/{src_id_str}/permissions")
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
                        new_shares: list[dict[str, Any]] = []
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
                                response = self.target_client.put(f"/api/elasticubes/localhost/{title_str}/permissions", data=new_shares)

                            elif dm_type == "live":
                                if not target_id:
                                    self.logger.error(
                                        "Cannot update shares for live datamodel '%s' because target_id could not be resolved.",
                                        title_str,
                                    )
                                    result["meta"]["share_fail_count"] += 1
                                    continue

                                self.logger.info("Publishing datamodel '%s' to update shares.", title_str)
                                publish_response = self.target_client.post("/api/v2/builds", data={"datamodelId": target_id, "buildType": "publish"})

                                if publish_response is not None and publish_response.status_code == 201:
                                    self.logger.info("Datamodel '%s' published successfully. Now updating shares.", title_str)
                                    response = self.target_client.patch(f"/api/v1/elasticubes/live/{target_id}/permissions", data=new_shares)
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
            ok = failed_count == 0
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
        dependencies: list[str] | str | None = None,
        shares: bool = False,
        batch_size: int = 10,
        sleep_time: int = 5,
        action: str | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
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

        all_datamodel_ids: list[str] = []
        limit = 100
        skip = 0
        pages_fetched = 0
        total_items_seen = 0
        missing_oid_count = 0
        duplicate_oid_count = 0
        duplicate_oids_sample: list[str] = []
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

            items: list[dict[str, Any]] = []
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
        migration_summary: dict[str, Any] = {"succeeded": [], "skipped": [], "failed": []}
        batch_errors: list[dict[str, Any]] = []

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
                        migration_summary["failed"].append({"title": None, "source_id": dm_id, "reason": f"Salvage retry failed: {str(e2)}"})

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
