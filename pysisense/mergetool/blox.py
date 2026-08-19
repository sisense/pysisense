from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Literal

from ..blox import Blox

_PAYLOAD_FIELDS_TO_STRIP = frozenset({"_id", "oid", "createdAt", "updatedAt", "createdBy", "updatedBy"})


def _transform_blox_action(source_action: dict[str, Any]) -> dict[str, Any]:
    """Convert a Blox action fetched from source into a target-ready save payload."""
    payload = {k: v for k, v in source_action.items() if k not in _PAYLOAD_FIELDS_TO_STRIP}
    payload.setdefault("snippet", {"type": source_action.get("type", "Unknown"), "title": source_action.get("title", "title")})
    payload.setdefault("step", "2")
    return payload


def _migrate_one_blox_action(
    tgt_blox: Blox,
    source_action: dict[str, Any],
    action: str,
    target_by_type: dict[str, dict[str, Any]],
    summary: dict[str, Any],
    logger: Any,
    progress: Callable[[dict[str, Any]], None],
) -> None:
    """Migrate a single Blox action, mutating ``summary`` in place.

    Safe to call concurrently — Blox actions are independent of each other.
    """
    action_type = source_action.get("type")

    if not action_type:
        logger.warning("Skipping a Blox action — missing type field.")
        summary["skipped"].append({"type": None, "reason": "Missing type field."})
        return

    existing = target_by_type.get(action_type)

    if existing and action == "skip":
        logger.info("Skipping '%s' — already exists on target.", action_type)
        summary["skipped"].append({"type": action_type, "reason": "Already exists on target."})
        progress({"type": "progress", "step": "migrate_action", "message": f"Skipped '{action_type}' (already exists).", "action": "skip"})
        return

    progress({"type": "progress", "step": "migrate_action", "message": f"Migrating '{action_type}'.", "action": action})

    if existing and action == "overwrite":
        logger.info("Deleting existing Blox action '%s' on target.", action_type)
        del_response = tgt_blox.delete_blox_action(action_type)
        if isinstance(del_response, dict) and "error" in del_response:
            logger.warning("Could not delete existing Blox action '%s': %s — proceeding with create.", action_type, del_response["error"])

    logger.debug("Transforming Blox action '%s' from source format.", action_type)
    payload = _transform_blox_action(source_action)

    logger.info("Saving Blox action '%s' on target.", action_type)
    save_response = tgt_blox.save_blox_action(payload)
    if isinstance(save_response, dict) and "error" in save_response:
        reason = save_response["error"]
        logger.error("Failed to save Blox action '%s': %s", action_type, reason)
        summary["failed"].append({"type": action_type, "reason": f"Save failed: {reason}"})
        progress({"type": "error", "step": "migrate_action", "message": f"Save failed for '{action_type}'.", "reason": reason})
        return

    logger.info("Successfully migrated Blox action '%s'.", action_type)
    summary["succeeded"].append({"type": action_type})
    progress({"type": "progress", "step": "migrate_action", "message": f"Migrated '{action_type}'.", "action": action})


class BloxMergeMixin:
    def migrate_blox_actions(
        self,
        action_types: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific Blox actions from source to target.

        Fetches the requested actions from the source environment, transforms
        each into a save-ready payload, and creates or overwrites it on the
        target. Conflict detection is based on the action's ``type`` field.
        Saving and deleting Blox actions is Linux-only, so the target
        environment must be a Linux deployment.

        Parameters
        ----------
        action_types : list[str] or None, default None
            The ``type`` identifiers of the Blox actions to migrate. If
            ``None``, every Blox action on the source is migrated.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when an action with the same ``type`` already
            exists on the target.

            - ``"skip"`` — leave the existing action unchanged.
            - ``"overwrite"`` — delete the existing action then recreate from
              source.
            - ``"duplicate"`` — always create, regardless of conflicts.
        concurrency : int, default 1
            Maximum number of Blox actions to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Blox actions are
            independent of each other, so any value is safe. Values <= 1
            (the default) process actions one at a time.
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
            - ``succeeded`` : list[dict] — each has ``type``
            - ``skipped`` : list[dict] — each has ``type`` and ``reason``
            - ``failed`` : list[dict] — each has ``type`` and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting Blox action migration from source to target."})

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

        if self.target_client.operating_system == "windows":
            msg = "migrate_blox_actions is not supported when the target is a Windows deployment."
            self.logger.error(msg)
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            return summary

        src_blox = Blox(api_client=self.source_client)
        tgt_blox = Blox(api_client=self.target_client)

        # Step 1: Fetch all source Blox actions
        self._emit(emit, {"type": "progress", "step": "fetch_source_actions", "message": "Fetching Blox actions from the source environment."})
        self.logger.debug("Fetching Blox actions from source.")
        src_response = src_blox.get_blox_actions()
        if src_response and isinstance(src_response[0], dict) and "error" in src_response[0]:
            raw_error = src_response[0]["error"]
            self.logger.error("Failed to fetch Blox actions from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_actions", "message": "Failed to fetch Blox actions from source.", "raw_error": raw_error})
            return summary

        all_source_actions: list[dict[str, Any]] = src_response
        self.logger.debug("Found %s Blox action(s) on source.", len(all_source_actions))

        # Step 2: Filter to the requested types
        if action_types:
            wanted: set[str] = set(action_types)
            actions_to_migrate = [a for a in all_source_actions if a.get("type") in wanted]
            for missing_type in wanted - {a.get("type") for a in actions_to_migrate}:
                self.logger.warning("Blox action type '%s' not found on source.", missing_type)
                summary["failed"].append({"type": missing_type, "reason": "Not found on source."})
        else:
            actions_to_migrate = all_source_actions

        summary["source_count"] = len(actions_to_migrate)

        if not actions_to_migrate:
            self.logger.info("No matching Blox actions found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No Blox actions to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_actions", "message": "Fetched source Blox actions.", "count": len(actions_to_migrate)})

        # Step 3: Fetch target Blox actions for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_actions", "message": "Fetching Blox actions from the target environment."})
        tgt_response = tgt_blox.get_blox_actions()
        target_actions: list[dict[str, Any]] = [] if tgt_response and isinstance(tgt_response[0], dict) and "error" in tgt_response[0] else tgt_response
        target_by_type: dict[str, dict[str, Any]] = {a["type"]: a for a in target_actions if a.get("type")}
        self.logger.debug("Found %s Blox action(s) on target.", len(target_actions))
        self._emit(emit, {"type": "progress", "step": "fetch_target_actions", "message": "Fetched target Blox actions.", "count": len(target_actions)})

        # Step 4: Migrate each Blox action — independent of each other, so the
        # whole set can run concurrently when concurrency > 1.
        progress = functools.partial(self._emit, emit)

        def _worker(source_action: dict[str, Any]) -> None:
            _migrate_one_blox_action(tgt_blox, source_action, action, target_by_type, summary, self.logger, progress)

        self._run_concurrently(actions_to_migrate, _worker, concurrency, "Blox actions")

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Blox action migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished Blox action migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_blox_actions(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all Blox actions from source to target.

        Delegates to ``migrate_blox_actions`` with no type filter, so every
        Blox action found on the source is migrated.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every Blox action.
        concurrency : int, default 1
            Same as in ``migrate_blox_actions``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_blox_actions``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full Blox action migration from source to target."})
        self.logger.info("Starting full Blox action migration from source to target.")

        return self.migrate_blox_actions(action_types=None, action=action, concurrency=concurrency, emit=emit)
