from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..datamodel import DataModel
from ..metadata import Metadata

_STRIP_KEYS = frozenset({"_id", "created", "lastUpdated", "oid"})


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


def _build_filter_payload(saved_filter: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in saved_filter.items() if k not in _STRIP_KEYS}


class FiltersMergeMixin:
    def migrate_saved_filters(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved filter dimensions for specific data models from source to target.

        Fetches each data model's dimensions from the source, keeps only the
        ones that carry a saved filter definition (a truthy ``filter`` key),
        and recreates them on the target via a raw metadata query. Conflict
        detection is based on a filter's ``title``. Requires the target data
        model to already exist — run
        ``migrate_datamodels``/``migrate_all_datamodels`` first.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            Data model OIDs to migrate. Provide either this or
            ``datamodel_names``.
        datamodel_names : list[str] or None, default None
            Data model titles to migrate. Provide either this or
            ``datamodel_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy for filters whose ``title`` already exists on
            the target datasource.

            - ``"skip"`` — leave the existing filter unchanged (default).
            - ``"overwrite"`` or ``"duplicate"`` — always create, regardless
              of conflicts. The Sisense metadata API exposes no update or
              delete endpoint for saved filters, so ``"overwrite"`` cannot
              replace the existing filter in place and behaves identically to
              ``"duplicate"``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``datamodel`` and
              ``filter`` (the filter's title)
            - ``skipped`` : list[dict] — each has ``datamodel``, ``filter``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``datamodel``, ``filter``,
              and ``reason``
            - ``source_count`` : int — total saved filters found across the
              resolved data models
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``datamodel_ids`` and ``datamodel_names``
            are provided.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting saved filters migration from source to target."})

        if datamodel_ids and datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not datamodel_ids and not datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting saved filters migration from source to target.")

        summary = _empty_summary()

        src_datamodel = DataModel(api_client=self.source_client)
        tgt_datamodel = DataModel(api_client=self.target_client)
        src_metadata = Metadata(api_client=self.source_client)
        tgt_metadata = Metadata(api_client=self.target_client)

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
                summary["failed"].append({"datamodel": None, "filter": None, "reason": f"Datamodel oid '{missing_id}' not found on source."})
        else:
            wanted_names: set[str] = set(datamodel_names)
            datamodels_to_migrate = [d for d in all_source_datamodels if d.get("title") in wanted_names]
            for missing_name in wanted_names - {d.get("title") for d in datamodels_to_migrate}:
                self.logger.warning("Datamodel '%s' not found on source.", missing_name)
                summary["failed"].append({"datamodel": missing_name, "filter": None, "reason": "Not found on source."})

        if not datamodels_to_migrate:
            self.logger.info("No matching datamodels found on source.")
            summary["failed_count"] = len(summary["failed"])
            summary["ok"] = summary["failed_count"] == 0
            summary["status"] = "noop" if summary["ok"] else "failed"
            self._emit(emit, {"type": "completed", "step": "done", "message": "No datamodels to migrate.", "status": summary["status"]})
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetched source datamodels.", "count": len(datamodels_to_migrate)})

        # Step 3: Fetch target datamodels — filters can only be written onto a datasource that already exists
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetching datamodels from the target environment."})
        tgt_response = tgt_datamodel.get_all_datamodel()
        target_datamodels: list[dict[str, Any]] = [] if isinstance(tgt_response, dict) and "error" in tgt_response else tgt_response
        target_titles: set[str] = {d["title"] for d in target_datamodels if d.get("title")}
        self.logger.debug("Found %s datamodel(s) on target.", len(target_datamodels))
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetched target datamodels.", "count": len(target_datamodels)})

        # Step 4: Migrate saved filters datamodel by datamodel
        for datamodel in datamodels_to_migrate:
            title = datamodel.get("title")
            source_oid = datamodel.get("oid")

            if not title:
                self.logger.warning("Skipping a datamodel — missing title field.")
                summary["skipped"].append({"datamodel": None, "filter": None, "reason": "Missing title field."})
                continue

            if title not in target_titles:
                self.logger.info("Skipping '%s' — data model not found on target.", title)
                summary["skipped"].append({"datamodel": title, "filter": None, "reason": "Data model not found on target. Migrate the data model first."})
                self._emit(emit, {"type": "progress", "step": "migrate_saved_filters", "message": f"Skipped '{title}' (not found on target)."})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_saved_filters", "message": f"Migrating saved filters for '{title}'.", "source_oid": source_oid})

            src_dimensions_result = src_metadata.get_datasource_dimensions(datasource=title)
            if isinstance(src_dimensions_result, dict) and "error" in src_dimensions_result:
                reason = f"Failed to fetch saved filters for '{title}' from source: {src_dimensions_result['error']}"
                self.logger.error(reason)
                summary["failed"].append({"datamodel": title, "filter": None, "reason": reason})
                self._emit(emit, {"type": "error", "step": "migrate_saved_filters", "message": reason})
                continue

            source_dimensions: list[dict[str, Any]] = src_dimensions_result if isinstance(src_dimensions_result, list) else []
            source_filters = [d for d in source_dimensions if isinstance(d, dict) and d.get("filter")]

            if not source_filters:
                self.logger.info("No saved filters found for '%s' on source.", title)
                self._emit(emit, {"type": "progress", "step": "migrate_saved_filters", "message": f"No saved filters to migrate for '{title}'."})
                continue

            existing_titles: set[str] = set()
            if action == "skip":
                tgt_dimensions_result = tgt_metadata.get_datasource_dimensions(datasource=title)
                tgt_dimensions: list[dict[str, Any]] = [] if isinstance(tgt_dimensions_result, dict) and "error" in tgt_dimensions_result else tgt_dimensions_result
                existing_titles = {d.get("title") for d in tgt_dimensions if isinstance(d, dict) and d.get("filter") and d.get("title")}

            for saved_filter in source_filters:
                filter_title = saved_filter.get("title")
                summary["source_count"] += 1

                if action == "skip" and filter_title in existing_titles:
                    self.logger.info("Skipping filter '%s' on '%s' — already exists on target.", filter_title, title)
                    summary["skipped"].append({"datamodel": title, "filter": filter_title, "reason": "Already exists on target."})
                    self._emit(emit, {"type": "progress", "step": "migrate_saved_filters", "message": f"Skipped filter '{filter_title}' on '{title}' (already exists).", "action": "skip"})
                    continue

                create_result = tgt_metadata.post_metadata_query(_build_filter_payload(saved_filter))

                if isinstance(create_result, dict) and "error" in create_result:
                    reason = f"Failed to create saved filter: {create_result['error']}"
                    self.logger.error("Failed to migrate filter '%s' on '%s': %s", filter_title, title, create_result["error"])
                    summary["failed"].append({"datamodel": title, "filter": filter_title, "reason": reason})
                    self._emit(emit, {"type": "error", "step": "migrate_saved_filters", "message": f"Failed to migrate filter '{filter_title}' on '{title}'.", "reason": reason})
                    continue

                self.logger.info("Migrated saved filter '%s' for '%s'.", filter_title, title)
                summary["succeeded"].append({"datamodel": title, "filter": filter_title})
                self._emit(emit, {"type": "progress", "step": "migrate_saved_filters", "message": f"Migrated filter '{filter_title}' for '{title}'."})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if (ok and summary["source_count"] > 0) else ("noop" if (ok and summary["source_count"] == 0) else "failed")

        self.logger.info(
            "Saved filters migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished saved filters migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_saved_filters(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved filter dimensions for all data models from source to target.

        Fetches every data model from the source environment and delegates
        to ``migrate_saved_filters``. Data models not yet present on the
        target are skipped — run ``migrate_all_datamodels`` first.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Same as in ``migrate_saved_filters``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_saved_filters``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full saved filters migration from source to target."})
        self.logger.info("Starting full saved filters migration from source to target.")

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

        return self.migrate_saved_filters(datamodel_ids=datamodel_ids, action=action, emit=emit)
