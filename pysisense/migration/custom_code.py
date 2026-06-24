from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..custom_code import CustomCode

_PAYLOAD_FIELDS_TO_STRIP = frozenset({"oid", "_id", "_rev", "createdAt", "updatedAt", "createdBy", "updatedBy"})


def _extract_notebooks(response: Any) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return response
    if isinstance(response, dict) and "error" not in response:
        for key in ("data", "items", "results"):
            val = response.get(key)
            if isinstance(val, list):
                return val
    return []


class CustomCodeMigrationMixin:
    def migrate_notebooks(
        self,
        notebook_ids: list[str] | None = None,
        notebook_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific notebooks from source to target.

        Exports each notebook from the source environment and creates or
        updates it on the target. Conflict detection is based on ``displayName``.

        Parameters
        ----------
        notebook_ids : list[str] or None, default None
            Notebook IDs to migrate. Provide either this or ``notebook_names``.
        notebook_names : list[str] or None, default None
            Notebook display names to migrate. Provide either this or
            ``notebook_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a notebook with the same ``displayName``
            already exists on the target.

            - ``"skip"`` — leave the existing notebook unchanged.
            - ``"overwrite"`` — delete the existing notebook then recreate from
              source.
            - ``"duplicate"`` — always create, regardless of conflicts.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``name`` and ``source_id``
            - ``skipped`` : list[dict] — each has ``name``, ``source_id``, and
              ``reason``
            - ``failed`` : list[dict] — each has ``name``, ``source_id``, and
              ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``notebook_ids`` and ``notebook_names`` are
            provided.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting notebook migration from source to target."})

        if notebook_ids and notebook_names:
            msg = "Provide either 'notebook_ids' or 'notebook_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not notebook_ids and not notebook_names:
            msg = "Provide either 'notebook_ids' or 'notebook_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting notebook migration from source to target.")

        summary: dict[str, Any] = {
            "ok": False,
            "status": "failed",
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "source_count": 0,
            "succeeded_count": 0,
            "skipped_count": 0,
            "failed_count": 0,
        }

        src_cc = CustomCode(api_client=self.source_client)
        tgt_cc = CustomCode(api_client=self.target_client)

        # Step 1: Fetch all source notebooks
        self._emit(emit, {"type": "progress", "step": "fetch_source_notebooks", "message": "Fetching notebooks from the source environment."})
        self.logger.debug("Fetching notebooks from source.")
        src_response = src_cc.get_notebooks()
        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch notebooks from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_notebooks", "message": "Failed to fetch notebooks from source.", "raw_error": raw_error})
            return summary

        all_source_notebooks = _extract_notebooks(src_response)
        self.logger.debug("Found %s notebooks on source.", len(all_source_notebooks))

        # Step 2: Filter to the requested IDs or names
        if notebook_ids:
            wanted: set[str] = set(notebook_ids)
            notebooks_to_migrate = [nb for nb in all_source_notebooks if nb.get("id") in wanted]
            for missing_id in wanted - {nb.get("id") for nb in notebooks_to_migrate}:
                self.logger.warning("Notebook id '%s' not found on source.", missing_id)
                summary["failed"].append({"name": None, "source_id": missing_id, "reason": "Not found on source."})
        else:
            wanted_names: set[str] = set(notebook_names)
            notebooks_to_migrate = [nb for nb in all_source_notebooks if nb.get("displayName") in wanted_names]
            for missing_name in wanted_names - {nb.get("displayName") for nb in notebooks_to_migrate}:
                self.logger.warning("Notebook '%s' not found on source.", missing_name)
                summary["failed"].append({"name": missing_name, "source_id": None, "reason": "Not found on source."})

        summary["source_count"] = len(notebooks_to_migrate)

        if not notebooks_to_migrate:
            self.logger.info("No matching notebooks found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No notebooks to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_notebooks", "message": "Fetched source notebooks.", "count": len(notebooks_to_migrate)})

        # Step 3: Fetch target notebooks for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_notebooks", "message": "Fetching notebooks from the target environment."})
        tgt_response = tgt_cc.get_notebooks()
        target_notebooks = _extract_notebooks(tgt_response) if not (isinstance(tgt_response, dict) and "error" in tgt_response) else []
        target_by_name: dict[str, dict[str, Any]] = {}
        for nb in target_notebooks:
            name = nb.get("displayName")
            if name:
                target_by_name[name] = nb
        self.logger.debug("Found %s notebooks on target.", len(target_notebooks))
        self._emit(emit, {"type": "progress", "step": "fetch_target_notebooks", "message": "Fetched target notebooks.", "count": len(target_notebooks)})

        # Step 4: Migrate each notebook
        for notebook in notebooks_to_migrate:
            notebook_id = notebook.get("id")
            notebook_name = notebook.get("displayName", notebook_id or "Unknown")

            if not notebook_id:
                self.logger.warning("Skipping notebook '%s' — missing id field.", notebook_name)
                summary["skipped"].append({"name": notebook_name, "source_id": None, "reason": "Missing id field."})
                continue

            existing = target_by_name.get(notebook_name)

            if existing and action == "skip":
                self.logger.info("Skipping '%s' — already exists on target.", notebook_name)
                summary["skipped"].append({"name": notebook_name, "source_id": notebook_id, "reason": "Already exists on target."})
                self._emit(emit, {"type": "progress", "step": "migrate_notebook", "message": f"Skipped '{notebook_name}' (already exists).", "action": "skip"})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_notebook", "message": f"Migrating '{notebook_name}'.", "source_id": notebook_id, "action": action})

            # Export from source
            self.logger.debug("Exporting notebook '%s' (id=%s) from source.", notebook_name, notebook_id)
            export_response = src_cc.export_notebook(notebook_id)
            if isinstance(export_response, dict) and "error" in export_response:
                reason = export_response["error"]
                self.logger.error("Failed to export notebook '%s': %s", notebook_name, reason)
                summary["failed"].append({"name": notebook_name, "source_id": notebook_id, "reason": f"Export failed: {reason}"})
                self._emit(emit, {"type": "error", "step": "migrate_notebook", "message": f"Export failed for '{notebook_name}'.", "reason": reason})
                continue

            payload: dict[str, Any] = {k: v for k, v in export_response.items() if k not in _PAYLOAD_FIELDS_TO_STRIP}
            if "id" not in payload:
                payload["id"] = notebook_id
            if "displayName" not in payload:
                payload["displayName"] = notebook_name

            # Overwrite: delete existing on target before recreating
            if existing and action == "overwrite":
                existing_id = existing.get("id")
                if existing_id:
                    self.logger.info("Deleting existing notebook '%s' (id=%s) on target.", notebook_name, existing_id)
                    del_response = tgt_cc.delete_notebook(existing_id)
                    if isinstance(del_response, dict) and "error" in del_response:
                        self.logger.warning("Could not delete existing notebook '%s': %s — proceeding with create.", notebook_name, del_response["error"])

            # Create on target
            self.logger.info("Creating notebook '%s' on target.", notebook_name)
            create_response = tgt_cc.create_notebook(payload)
            if isinstance(create_response, dict) and "error" in create_response:
                reason = create_response["error"]
                self.logger.error("Failed to create notebook '%s': %s", notebook_name, reason)
                summary["failed"].append({"name": notebook_name, "source_id": notebook_id, "reason": f"Create failed: {reason}"})
                self._emit(emit, {"type": "error", "step": "migrate_notebook", "message": f"Create failed for '{notebook_name}'.", "reason": reason})
                continue

            self.logger.info("Successfully migrated notebook '%s'.", notebook_name)
            summary["succeeded"].append({"name": notebook_name, "source_id": notebook_id})
            self._emit(emit, {"type": "progress", "step": "migrate_notebook", "message": f"Migrated '{notebook_name}'.", "action": action})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Notebook migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished notebook migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_notebooks(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all notebooks from source to target.

        Fetches every notebook from the source environment and delegates to
        ``migrate_notebooks``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every notebook.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_notebooks``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full notebook migration from source to target."})
        self.logger.info("Starting full notebook migration from source to target.")

        src_cc = CustomCode(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_notebooks", "message": "Fetching all notebooks from source."})
        src_response = src_cc.get_notebooks()

        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch notebooks from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_notebooks", "message": "Failed to fetch notebooks from source.", "raw_error": raw_error})
            return {
                "ok": False,
                "status": "failed",
                "succeeded": [],
                "skipped": [],
                "failed": [],
                "source_count": 0,
                "succeeded_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
            }

        all_notebooks = _extract_notebooks(src_response)
        notebook_ids = [nb["id"] for nb in all_notebooks if nb.get("id")]

        self.logger.info("Found %s notebooks on source.", len(all_notebooks))
        self._emit(emit, {"type": "progress", "step": "fetch_source_notebooks", "message": "Fetched notebooks from source.", "count": len(all_notebooks)})

        if not notebook_ids:
            self.logger.info("No notebooks found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No notebooks found on source.", "status": "noop"})
            return {
                "ok": True,
                "status": "noop",
                "succeeded": [],
                "skipped": [],
                "failed": [],
                "source_count": 0,
                "succeeded_count": 0,
                "skipped_count": 0,
                "failed_count": 0,
            }

        return self.migrate_notebooks(notebook_ids=notebook_ids, action=action, emit=emit)
