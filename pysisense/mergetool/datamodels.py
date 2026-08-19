from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..access_management import AccessManagement
from ..datamodel import DataModel

_DEPENDENCY_MAPPING: dict[str, list[str]] = {
    "dataSecurity": ["dataContext", "scopeConfiguration"],
    "formulas": ["formulaManagement"],
    "hierarchies": ["drillHierarchies"],
    "perspectives": ["perspectives"],
}


def _empty_summary(ok: bool = False, status: str = "failed") -> dict[str, Any]:
    return {
        "ok": ok,
        "status": status,
        "succeeded": [],
        "skipped": [],
        "failed": [],
        "source_count": 0,
        "succeeded_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
    }


def _resolve_api_dependencies(dependencies: list[str] | str | None) -> list[str]:
    if dependencies is None or dependencies == "all":
        dependencies = list(_DEPENDENCY_MAPPING.keys())
    elif isinstance(dependencies, str):
        dependencies = [dependencies]
    return list({dep for key in dependencies for dep in _DEPENDENCY_MAPPING.get(key, [])})


def _apply_connection_map(data_model: dict[str, Any], provider_connection_map: dict[str, str] | None) -> None:
    """Repoint each dataset's connection to a target connection, or strip credentials when unmapped."""
    for dataset in data_model.get("datasets", []):
        connection = dataset.get("connection")
        if not isinstance(connection, dict):
            continue
        provider = connection.get("provider")
        if provider_connection_map and provider in provider_connection_map:
            dataset["connection"] = {"oid": provider_connection_map[provider], "provider": provider}
        elif "parameters" in connection:
            connection["parameters"] = ""


def _extract_source_shares(dm_type: str | None, payload: Any) -> list[dict[str, Any]]:
    if dm_type == "extract":
        return payload.get("shares", []) if isinstance(payload, dict) else []
    return payload if isinstance(payload, list) else []


def _resolve_datamodel_share_entries(
    shares: list[dict[str, Any]],
    user_id_to_email: dict[str, str],
    email_to_target_id: dict[str, str],
    group_id_to_name: dict[str, str],
    group_name_to_target_id: dict[str, str],
) -> list[dict[str, Any]]:
    """Convert exported datamodel shares into target user/group ids."""
    resolved: list[dict[str, Any]] = []
    for share in shares:
        if not isinstance(share, dict):
            continue
        party_type = share.get("type")
        party_id = share.get("partyId")
        permission = share.get("permission", "a")

        target_party_id: str | None = None
        if party_type == "user":
            email = user_id_to_email.get(party_id)
            target_party_id = email_to_target_id.get(email) if email else None
        elif party_type == "group":
            name = group_id_to_name.get(party_id)
            target_party_id = group_name_to_target_id.get(name) if name else None

        if target_party_id:
            resolved.append({"partyId": target_party_id, "type": party_type, "permission": permission})
    return resolved


class DatamodelsMergeMixin:
    def migrate_datamodels(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        dependencies: list[str] | str | None = None,
        provider_connection_map: dict[str, str] | None = None,
        shares: bool = False,
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific data models from source to target.

        Exports each data model's schema from the source environment and
        imports it into the target via the schema import endpoint. Conflict
        detection is based on the data model's ``title``. Embedded connection
        credentials are repointed using ``provider_connection_map`` when a
        matching provider entry is supplied, and stripped otherwise — they
        must be re-entered (or reconnected) on the target after migration.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            Data model OIDs to migrate. Provide either this or
            ``datamodel_names``.
        datamodel_names : list[str] or None, default None
            Data model titles to migrate. Provide either this or
            ``datamodel_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a data model with the same ``title``
            already exists on the target.

            - ``"skip"`` — leave the existing data model unchanged.
            - ``"overwrite"`` — replace the existing data model's schema with
              the source version, matched by the target model's own OID.
            - ``"duplicate"`` — always create a new data model titled
              ``"<title> (Duplicate)"``, regardless of conflicts.
        dependencies : list[str] or str or None, default None
            Dependencies to include in the export. One or more of
            ``"dataSecurity"``, ``"formulas"``, ``"hierarchies"``,
            ``"perspectives"``. Defaults to all of them when ``None`` or
            ``"all"``. Not supported when the source is a Windows deployment.
        provider_connection_map : dict[str, str] or None, default None
            Maps a connection provider name (for example ``"Athena"``) to a
            target-environment connection OID. Datasets whose provider is not
            present in this map have their connection parameters cleared on
            import.
        shares : bool, default False
            Whether to also migrate each data model's shares after a
            successful import, remapping users and groups by email/name.
        concurrency : int, default 1
            Maximum number of data models to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Data models are
            independent of each other, so any value is safe. Values <= 1
            (the default) process data models one at a time.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys. When
            ``concurrency`` is greater than 1, this callback may be invoked
            from multiple worker threads concurrently.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``source_oid``,
              and ``target_id``
            - ``skipped`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``datamodel_ids`` and ``datamodel_names``
            are provided.

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting datamodel migration from source to target."})

        if datamodel_ids and datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not datamodel_ids and not datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting datamodel migration from source to target.")

        summary = _empty_summary()
        api_dependencies = _resolve_api_dependencies(dependencies)

        src_datamodel = DataModel(api_client=self.source_client)
        tgt_datamodel = DataModel(api_client=self.target_client)

        # Step 1: Fetch all source datamodels
        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetching datamodels from the source environment."})
        self.logger.debug("Fetching datamodels from source.")
        src_response = src_datamodel.get_all_datamodel()
        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch datamodels from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_datamodels", "message": "Failed to fetch datamodels from source.", "raw_error": raw_error})
            return summary

        all_source_datamodels: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        self.logger.debug("Found %s datamodel(s) on source.", len(all_source_datamodels))

        # Step 2: Filter to the requested ids or names
        if datamodel_ids:
            wanted: set[str] = set(datamodel_ids)
            datamodels_to_migrate = [d for d in all_source_datamodels if d.get("oid") in wanted]
            for missing_id in wanted - {d.get("oid") for d in datamodels_to_migrate}:
                self.logger.warning("Datamodel oid '%s' not found on source.", missing_id)
                summary["failed"].append({"title": None, "source_oid": missing_id, "reason": "Not found on source."})
        else:
            wanted_names: set[str] = set(datamodel_names)
            datamodels_to_migrate = [d for d in all_source_datamodels if d.get("title") in wanted_names]
            for missing_name in wanted_names - {d.get("title") for d in datamodels_to_migrate}:
                self.logger.warning("Datamodel '%s' not found on source.", missing_name)
                summary["failed"].append({"title": missing_name, "source_oid": None, "reason": "Not found on source."})

        summary["source_count"] = len(datamodels_to_migrate)

        if not datamodels_to_migrate:
            self.logger.info("No matching datamodels found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No datamodels to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetched source datamodels.", "count": len(datamodels_to_migrate)})

        # Step 3: Fetch target datamodels for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetching datamodels from the target environment."})
        tgt_response = tgt_datamodel.get_all_datamodel()
        target_datamodels: list[dict[str, Any]] = [] if isinstance(tgt_response, dict) and "error" in tgt_response else tgt_response
        target_oid_by_title: dict[str, str] = {d["title"]: d["oid"] for d in target_datamodels if d.get("title") and d.get("oid")}
        self.logger.debug("Found %s datamodel(s) on target.", len(target_datamodels))
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetched target datamodels.", "count": len(target_datamodels)})

        # Step 4: Fetch user/group mappings, only needed when migrating shares
        user_id_to_email: dict[str, str] = {}
        email_to_target_id: dict[str, str] = {}
        group_id_to_name: dict[str, str] = {}
        group_name_to_target_id: dict[str, str] = {}
        if shares:
            self._emit(emit, {"type": "progress", "step": "fetch_mappings", "message": "Fetching users and groups for share resolution."})
            src_access = AccessManagement(api_client=self.source_client)
            tgt_access = AccessManagement(api_client=self.target_client)

            src_users_result = src_access.get_users_expanded()
            src_users: list[dict[str, Any]] = [] if isinstance(src_users_result, dict) and "error" in src_users_result else src_users_result
            user_id_to_email = {u["_id"]: u["email"] for u in src_users if u.get("_id") and u.get("email")}

            tgt_users_result = tgt_access.get_users_expanded()
            tgt_users: list[dict[str, Any]] = [] if isinstance(tgt_users_result, dict) and "error" in tgt_users_result else tgt_users_result
            email_to_target_id = {u["email"]: u["_id"] for u in tgt_users if u.get("email") and u.get("_id")}

            src_groups_result = src_access.get_groups()
            src_groups: list[dict[str, Any]] = [] if isinstance(src_groups_result, dict) and "error" in src_groups_result else src_groups_result
            group_id_to_name = {g["_id"]: g["name"] for g in src_groups if g.get("_id") and g.get("name")}

            tgt_groups_result = tgt_access.get_groups()
            tgt_groups: list[dict[str, Any]] = [] if isinstance(tgt_groups_result, dict) and "error" in tgt_groups_result else tgt_groups_result
            group_name_to_target_id = {g["name"]: g["_id"] for g in tgt_groups if g.get("name") and g.get("_id")}

        # Step 5: Migrate each datamodel — independent of each other, so the
        # whole set can run concurrently when concurrency > 1.
        def _worker(datamodel: dict[str, Any]) -> None:
            self._migrate_one_datamodel(
                datamodel,
                action=action,
                api_dependencies=api_dependencies,
                provider_connection_map=provider_connection_map,
                target_oid_by_title=target_oid_by_title,
                summary=summary,
                shares=shares,
                user_id_to_email=user_id_to_email,
                email_to_target_id=email_to_target_id,
                group_id_to_name=group_id_to_name,
                group_name_to_target_id=group_name_to_target_id,
                emit=emit,
            )

        self._run_concurrently(datamodels_to_migrate, _worker, concurrency, "data models")

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Datamodel migration complete. source=%s succeeded=%s skipped=%s failed=%s",
            summary["source_count"],
            summary["succeeded_count"],
            summary["skipped_count"],
            summary["failed_count"],
        )
        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished datamodel migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def _migrate_one_datamodel(
        self,
        datamodel: dict[str, Any],
        *,
        action: str,
        api_dependencies: list[str],
        provider_connection_map: dict[str, str] | None,
        target_oid_by_title: dict[str, str],
        summary: dict[str, Any],
        shares: bool,
        user_id_to_email: dict[str, str],
        email_to_target_id: dict[str, str],
        group_id_to_name: dict[str, str],
        group_name_to_target_id: dict[str, str],
        emit: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        """Migrate a single datamodel, mutating ``summary`` in place.

        Safe to call concurrently — data models are independent of each other.
        """
        source_oid = datamodel.get("oid")
        title = datamodel.get("title")
        dm_type = datamodel.get("type")

        if not source_oid or not title:
            self.logger.warning("Skipping a datamodel — missing oid or title field.")
            summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "Missing oid or title field."})
            return

        existing_target_oid = target_oid_by_title.get(title)

        if existing_target_oid and action == "skip":
            self.logger.info("Skipping '%s' — already exists on target.", title)
            summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "Already exists on target."})
            self._emit(emit, {"type": "progress", "step": "migrate_datamodel", "message": f"Skipped '{title}' (already exists).", "action": "skip"})
            return

        self._emit(emit, {"type": "progress", "step": "migrate_datamodel", "message": f"Migrating '{title}'.", "source_oid": source_oid, "action": action})

        # Export schema from source
        source_os = self.source_client.operating_system
        if source_os == "windows":
            if api_dependencies:
                self.logger.warning(
                    "Windows datamodel export does not support dependenciesIdsToInclude — dependencies (%s) will not be migrated for '%s'.",
                    api_dependencies,
                    title,
                )
            export_response = self.source_client.get(f"/api/v1/elasticubes/{source_oid}/datamodel-exports/stream/schema")
        else:
            export_response = self.source_client.get(
                "/api/v2/datamodel-exports/schema",
                params={"datamodelId": source_oid, "type": "schema-latest", "dependenciesIdsToInclude": ",".join(api_dependencies)},
            )

        if export_response is None or export_response.status_code != 200:
            reason = f"Export failed: {self._extract_error_detail(export_response)}"
            self.logger.error("Failed to export datamodel '%s': %s", title, reason)
            summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
            self._emit(emit, {"type": "error", "step": "migrate_datamodel", "message": f"Export failed for '{title}'.", "reason": reason})
            return

        data_model_json, err = self._safe_json(export_response)
        if not isinstance(data_model_json, dict):
            reason = f"Export failed: {err or 'Export returned non-dict JSON'}"
            self.logger.error("Failed to export datamodel '%s': %s", title, reason)
            summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
            self._emit(emit, {"type": "error", "step": "migrate_datamodel", "message": f"Export failed for '{title}'.", "reason": reason})
            return

        _apply_connection_map(data_model_json, provider_connection_map)

        # Import schema into target
        query_string = ""
        if action == "overwrite" and existing_target_oid:
            query_string = f"?datamodelId={existing_target_oid}"
        elif action == "duplicate":
            query_string = f"?newTitle={title} (Duplicate)"

        import_response = self.target_client.post(f"/api/v2/datamodel-imports/schema{query_string}", data=data_model_json)

        if import_response is not None and import_response.status_code == 404 and action == "overwrite" and existing_target_oid:
            self.logger.warning("Overwrite target for '%s' not found (404). Retrying without overwrite.", title)
            import_response = self.target_client.post("/api/v2/datamodel-imports/schema", data=data_model_json)

        import_payload, _ = self._safe_json(import_response)
        target_id: str | None = None
        if isinstance(import_payload, dict):
            for key in ("oid", "id", "datamodelId"):
                value = import_payload.get(key)
                if isinstance(value, str):
                    target_id = value
                    break

        if import_response is not None and import_response.status_code == 201:
            self.logger.info("Successfully migrated datamodel '%s'.", title)
            summary["succeeded"].append({"title": title, "source_oid": source_oid, "target_id": target_id})
            self._emit(emit, {"type": "progress", "step": "migrate_datamodel", "message": f"Migrated '{title}'.", "action": action})

            if shares:
                self._migrate_datamodel_shares(
                    title=title,
                    dm_type=dm_type,
                    source_oid=source_oid,
                    target_id=target_id,
                    user_id_to_email=user_id_to_email,
                    email_to_target_id=email_to_target_id,
                    group_id_to_name=group_id_to_name,
                    group_name_to_target_id=group_name_to_target_id,
                    emit=emit,
                )
        elif import_response is not None and import_response.status_code == 400 and isinstance(import_payload, dict) and import_payload.get("title") == "ElasticubeAlreadyExists":
            reason = f"Datamodel '{title}' already exists on the target with a different ID. Use action='duplicate', or delete the existing model manually."
            self.logger.error(reason)
            summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
            self._emit(emit, {"type": "error", "step": "migrate_datamodel", "message": f"Import failed for '{title}'.", "reason": reason})
        else:
            reason = f"Import failed: {self._extract_error_detail(import_response)}"
            self.logger.error("Failed to import datamodel '%s': %s", title, reason)
            summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
            self._emit(emit, {"type": "error", "step": "migrate_datamodel", "message": f"Import failed for '{title}'.", "reason": reason})

    def _migrate_datamodel_shares(
        self,
        *,
        title: str,
        dm_type: str | None,
        source_oid: str,
        target_id: str | None,
        user_id_to_email: dict[str, str],
        email_to_target_id: dict[str, str],
        group_id_to_name: dict[str, str],
        group_name_to_target_id: dict[str, str],
        emit: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        """Best-effort share migration for a single, already-imported datamodel."""
        if dm_type == "extract":
            shares_response = self.source_client.get(f"/api/elasticubes/localhost/{title}/permissions")
        elif dm_type == "live":
            shares_response = self.source_client.get(f"/api/v1/elasticubes/live/{source_oid}/permissions")
        else:
            self.logger.warning("Unknown datamodel type '%s' for '%s' — skipping shares.", dm_type, title)
            return

        if shares_response is None or shares_response.status_code != 200:
            self.logger.error("Failed to fetch shares for datamodel '%s': %s", title, self._extract_error_detail(shares_response))
            self._emit(emit, {"type": "warning", "step": "migrate_shares", "message": f"Failed to fetch shares for '{title}'."})
            return

        payload, _ = self._safe_json(shares_response)
        source_shares = _extract_source_shares(dm_type, payload)

        new_shares = _resolve_datamodel_share_entries(source_shares, user_id_to_email, email_to_target_id, group_id_to_name, group_name_to_target_id)
        if not new_shares:
            self.logger.debug("No resolvable shares for datamodel '%s'.", title)
            return

        if dm_type == "extract":
            response = self.target_client.put(f"/api/elasticubes/localhost/{title}/permissions", data=new_shares)
        else:
            if not target_id:
                self.logger.error("Cannot migrate shares for live datamodel '%s' — target id could not be resolved.", title)
                self._emit(emit, {"type": "warning", "step": "migrate_shares", "message": f"Unresolved target id for '{title}'."})
                return

            self.logger.info("Publishing datamodel '%s' to update shares.", title)
            publish_response = self.target_client.post("/api/v2/builds", data={"datamodelId": target_id, "buildType": "publish"})
            if publish_response is None or publish_response.status_code != 201:
                self.logger.error("Failed to publish datamodel '%s' before updating shares: %s", title, self._extract_error_detail(publish_response))
                self._emit(emit, {"type": "warning", "step": "migrate_shares", "message": f"Failed to publish '{title}' before updating shares."})
                return

            response = self.target_client.patch(f"/api/v1/elasticubes/live/{target_id}/permissions", data=new_shares)

        if response is not None and response.status_code in (200, 201):
            self.logger.info("Migrated %s share(s) for datamodel '%s'.", len(new_shares), title)
            self._emit(emit, {"type": "progress", "step": "migrate_shares", "message": f"Migrated shares for '{title}'.", "count": len(new_shares)})
        else:
            self.logger.error("Failed to migrate shares for datamodel '%s': %s", title, self._extract_error_detail(response))
            self._emit(emit, {"type": "warning", "step": "migrate_shares", "message": f"Failed to migrate shares for '{title}'."})

    def migrate_all_datamodels(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        dependencies: list[str] | str | None = None,
        provider_connection_map: dict[str, str] | None = None,
        shares: bool = False,
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all data models from source to target.

        Fetches every data model from the source environment and delegates
        to ``migrate_datamodels``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every data model.
        dependencies : list[str] or str or None, default None
            Same as in ``migrate_datamodels``.
        provider_connection_map : dict[str, str] or None, default None
            Same as in ``migrate_datamodels``.
        shares : bool, default False
            Same as in ``migrate_datamodels``.
        concurrency : int, default 1
            Same as in ``migrate_datamodels``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_datamodels``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full datamodel migration from source to target."})
        self.logger.info("Starting full datamodel migration from source to target.")

        src_datamodel = DataModel(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetching all datamodels from source."})
        src_response = src_datamodel.get_all_datamodel()

        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch datamodels from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_datamodels", "message": "Failed to fetch datamodels from source.", "raw_error": raw_error})
            return _empty_summary()

        all_datamodels: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        datamodel_ids = [d["oid"] for d in all_datamodels if d.get("oid")]

        self.logger.info("Found %s datamodel(s) on source.", len(all_datamodels))
        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetched datamodels from source.", "count": len(all_datamodels)})

        if not datamodel_ids:
            self.logger.info("No datamodels found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No datamodels found on source.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        return self.migrate_datamodels(
            datamodel_ids=datamodel_ids,
            action=action,
            dependencies=dependencies,
            provider_connection_map=provider_connection_map,
            shares=shares,
            concurrency=concurrency,
            emit=emit,
        )
