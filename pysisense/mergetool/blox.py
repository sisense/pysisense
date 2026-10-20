from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/blox.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class BloxMergeMixin:
    @deprecated("use Migration.migrate_blox_actions")
    def migrate_blox_actions(
        self,
        action_types: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific Blox actions from source to target.

        Deprecated: use ``Migration.migrate_blox_actions``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

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
        return self._as_migration().migrate_blox_actions(action_types=action_types, action=action, concurrency=concurrency, emit=emit)

    @deprecated("use Migration.migrate_all_blox_actions")
    def migrate_all_blox_actions(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all Blox actions from source to target.

        Deprecated: use ``Migration.migrate_all_blox_actions``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

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
        return self._as_migration().migrate_all_blox_actions(action=action, concurrency=concurrency, emit=emit)
