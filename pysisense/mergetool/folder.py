from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..folder import Folder


def _build_oid_to_folder(folders: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {f["oid"]: f for f in folders if f.get("oid")}


def _build_path_map(oid_to_folder: dict[str, dict[str, Any]]) -> dict[str, str]:
    """Return a map of folder_oid -> full slash-separated path from tree root."""
    cache: dict[str, str] = {}

    def _path(oid: str) -> str:
        if oid in cache:
            return cache[oid]
        f = oid_to_folder.get(oid)
        if not f:
            cache[oid] = ""
            return ""
        parent_id = f.get("parentId") or ""
        result = _path(parent_id) + "/" + f["name"] if parent_id and parent_id in oid_to_folder else f["name"]
        cache[oid] = result
        return result

    for oid in oid_to_folder:
        _path(oid)
    return cache


def _get_subtree_oids(root_oids: set[str], oid_to_folder: dict[str, dict[str, Any]]) -> set[str]:
    """Return root_oids plus all recursive descendant oids."""
    children_map: dict[str, list[str]] = {}
    for oid, f in oid_to_folder.items():
        pid = f.get("parentId") or ""
        if pid:
            children_map.setdefault(pid, []).append(oid)
    found: set[str] = set()
    queue = list(root_oids)
    while queue:
        oid = queue.pop()
        found.add(oid)
        queue.extend(children_map.get(oid, []))
    return found


class FolderMergeMixin:
    def migrate_folders(
        self,
        folder_ids: list[str] | None = None,
        folder_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific folders and their full subtrees from source to target.

        Resolves the requested root folders by ID or display name, expands
        each to its complete subtree, then recreates the hierarchy on the
        target in depth-first order. Conflict detection is path-based (full
        ``parent/child`` path), so identically-named folders in different
        branches are treated independently.

        Folders whose parent is not part of the migration (e.g. when migrating
        a sub-folder in isolation) are created at the root level on the target.

        Parameters
        ----------
        folder_ids : list[str] or None, default None
            Folder OIDs to migrate. Provide either this or ``folder_names``.
        folder_names : list[str] or None, default None
            Folder display names to migrate. Provide either this or
            ``folder_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a folder at the same path already exists on
            the target.

            - ``"skip"`` — leave the existing folder unchanged and map its OID
              so child folders can still be placed under it correctly.
            - ``"overwrite"`` — delete the existing folder then recreate from
              source. **Warning:** deleting a folder on Sisense also removes
              all dashboards inside it.
            - ``"duplicate"`` — always create, regardless of conflicts.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``name``, ``path``, and
              ``source_oid``
            - ``skipped`` : list[dict] — each has ``name``, ``path``,
              ``source_oid``, and ``reason``
            - ``failed`` : list[dict] — each has ``name``, ``path``,
              ``source_oid``, and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``folder_ids`` and ``folder_names`` are
            provided.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting folder migration from source to target."})

        if folder_ids and folder_names:
            msg = "Provide either 'folder_ids' or 'folder_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not folder_ids and not folder_names:
            msg = "Provide either 'folder_ids' or 'folder_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting folder migration from source to target.")

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

        src_folder = Folder(api_client=self.source_client)
        tgt_folder = Folder(api_client=self.target_client)

        # Step 1: Fetch all source folders (flat, needed for tree context)
        self._emit(emit, {"type": "progress", "step": "fetch_source_folders", "message": "Fetching folders from the source environment."})
        self.logger.debug("Fetching folders from source.")
        src_response = src_folder.get_folders("flat")
        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch folders from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_folders", "message": "Failed to fetch folders from source.", "raw_error": raw_error})
            return summary

        all_source_folders: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        self.logger.debug("Found %s folders on source.", len(all_source_folders))

        oid_to_folder = _build_oid_to_folder(all_source_folders)
        path_map = _build_path_map(oid_to_folder)

        # Step 2: Resolve requested root folders
        if folder_ids:
            wanted: set[str] = set(folder_ids)
            root_folders = [f for f in all_source_folders if f.get("oid") in wanted]
            for missing_id in wanted - {f.get("oid") for f in root_folders}:
                self.logger.warning("Folder oid '%s' not found on source.", missing_id)
                summary["failed"].append({"name": None, "path": None, "source_oid": missing_id, "reason": "Not found on source."})
        else:
            wanted_names: set[str] = set(folder_names)
            root_folders = [f for f in all_source_folders if f.get("name") in wanted_names]
            for missing_name in wanted_names - {f.get("name") for f in root_folders}:
                self.logger.warning("Folder '%s' not found on source.", missing_name)
                summary["failed"].append({"name": missing_name, "path": None, "source_oid": None, "reason": "Not found on source."})

        # Step 3: Expand each root to its full subtree
        root_oids = {f["oid"] for f in root_folders if f.get("oid")}
        subtree_oids = _get_subtree_oids(root_oids, oid_to_folder)
        folders_to_migrate = [f for f in all_source_folders if f.get("oid") in subtree_oids]
        folders_to_migrate.sort(key=lambda f: path_map.get(f.get("oid", ""), "").count("/"))

        summary["source_count"] = len(folders_to_migrate)

        if not folders_to_migrate:
            self.logger.info("No matching folders found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No folders to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_folders", "message": "Resolved source folders.", "count": len(folders_to_migrate)})

        # Step 4: Fetch target folders for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_folders", "message": "Fetching folders from the target environment."})
        tgt_response = tgt_folder.get_folders("flat")
        target_folders: list[dict[str, Any]] = tgt_response if isinstance(tgt_response, list) else []
        tgt_oid_to_folder = _build_oid_to_folder(target_folders)
        tgt_path_map = _build_path_map(tgt_oid_to_folder)
        target_path_to_folder: dict[str, dict[str, Any]] = {path: tgt_oid_to_folder[oid] for oid, path in tgt_path_map.items() if path}
        target_oid_to_path: dict[str, str] = {oid: path for oid, path in tgt_path_map.items() if path}
        self.logger.debug("Found %s folders on target.", len(target_folders))
        self._emit(emit, {"type": "progress", "step": "fetch_target_folders", "message": "Fetched target folders.", "count": len(target_folders)})

        # Step 5: Migrate each folder in depth order (parents before children)
        oid_map: dict[str, str] = {}  # source_oid -> target_oid

        for folder in folders_to_migrate:
            source_oid = folder.get("oid")
            folder_name = folder.get("name", source_oid or "Unknown")
            source_parent_id = folder.get("parentId") or ""

            if not source_oid:
                self.logger.warning("Skipping folder '%s' — missing oid field.", folder_name)
                summary["skipped"].append({"name": folder_name, "path": None, "source_oid": None, "reason": "Missing oid field."})
                continue

            # Determine target parent and expected path
            if source_parent_id and source_parent_id in oid_map:
                target_parent_oid: str | None = oid_map[source_parent_id]
                parent_path = target_oid_to_path.get(target_parent_oid, "")
                expected_path = parent_path + "/" + folder_name if parent_path else folder_name
            else:
                target_parent_oid = None
                expected_path = folder_name

            folder_path = path_map.get(source_oid, folder_name)
            existing = target_path_to_folder.get(expected_path)

            if existing and action == "skip":
                existing_oid = existing.get("oid", "")
                oid_map[source_oid] = existing_oid
                target_oid_to_path[existing_oid] = expected_path
                self.logger.info("Skipping '%s' — already exists on target.", folder_name)
                summary["skipped"].append({"name": folder_name, "path": folder_path, "source_oid": source_oid, "reason": "Already exists on target."})
                self._emit(emit, {"type": "progress", "step": "migrate_folder", "message": f"Skipped '{folder_name}' (already exists).", "action": "skip"})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_folder", "message": f"Migrating '{folder_name}'.", "source_oid": source_oid, "action": action})

            if existing and action == "overwrite":
                existing_oid = existing.get("oid")
                if existing_oid:
                    self.logger.info("Deleting existing folder '%s' (oid=%s) on target.", folder_name, existing_oid)
                    del_response = tgt_folder.delete_folder(existing_oid)
                    if isinstance(del_response, dict) and "error" in del_response:
                        self.logger.warning("Could not delete existing folder '%s': %s — proceeding with create.", folder_name, del_response["error"])
                    else:
                        target_path_to_folder.pop(expected_path, None)

            create_response = tgt_folder.create_folder(folder_name, parent_id=target_parent_oid)
            if isinstance(create_response, dict) and "error" in create_response:
                reason = create_response["error"]
                self.logger.error("Failed to create folder '%s': %s", folder_name, reason)
                summary["failed"].append({"name": folder_name, "path": folder_path, "source_oid": source_oid, "reason": f"Create failed: {reason}"})
                self._emit(emit, {"type": "error", "step": "migrate_folder", "message": f"Create failed for '{folder_name}'.", "reason": reason})
                continue

            new_oid = create_response.get("oid", "")
            oid_map[source_oid] = new_oid
            target_oid_to_path[new_oid] = expected_path
            target_path_to_folder[expected_path] = create_response

            self.logger.info("Successfully migrated folder '%s'.", folder_name)
            summary["succeeded"].append({"name": folder_name, "path": folder_path, "source_oid": source_oid})
            self._emit(emit, {"type": "progress", "step": "migrate_folder", "message": f"Migrated '{folder_name}'.", "action": action})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Folder migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished folder migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_folders(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all folders from source to target, preserving full hierarchy.

        Fetches every folder from the source environment and delegates to
        ``migrate_folders``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every folder.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_folders``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full folder migration from source to target."})
        self.logger.info("Starting full folder migration from source to target.")

        src_folder = Folder(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_folders", "message": "Fetching all folders from source."})
        src_response = src_folder.get_folders("flat")

        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch folders from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_folders", "message": "Failed to fetch folders from source.", "raw_error": raw_error})
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

        all_folders: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        folder_ids = [f["oid"] for f in all_folders if f.get("oid")]

        self.logger.info("Found %s folders on source.", len(all_folders))
        self._emit(emit, {"type": "progress", "step": "fetch_source_folders", "message": "Fetched folders from source.", "count": len(all_folders)})

        if not folder_ids:
            self.logger.info("No folders found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No folders found on source.", "status": "noop"})
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

        return self.migrate_folders(folder_ids=folder_ids, action=action, emit=emit)
