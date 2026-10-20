from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/folder.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class FolderMergeMixin:
    @deprecated("use Migration.migrate_folders")
    def migrate_folders(
        self,
        folder_ids: list[str] | None = None,
        folder_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific folders and their full subtrees from source to target.

        Deprecated: use ``Migration.migrate_folders``, which runs the same code with
        the same parameters and result. MergeTool is removed in the next major
        release.

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
        concurrency : int, default 1
            Maximum number of folders to create/delete concurrently within
            each hierarchy depth level, run via a background thread pool
            (``asyncio.to_thread``) since the underlying HTTP client is
            synchronous. Folders at the same depth are independent — their
            parent was already migrated in an earlier, fully-completed depth
            level — so they're safe to run in parallel; different depths are
            still processed strictly in order, since a child's target parent
            OID must exist before it can be created. Values <= 1 (the
            default) process folders one at a time.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys. When
            ``concurrency`` is greater than 1, this callback may be invoked
            from multiple worker threads concurrently for folders in the same
            depth level.

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

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing for
        the affected depth level (a nested event loop cannot be started) and
        logs a warning.
        """
        return self._as_migration().migrate_folders(folder_ids=folder_ids, folder_names=folder_names, action=action, concurrency=concurrency, emit=emit)

    @deprecated("use Migration.migrate_all_folders")
    def migrate_all_folders(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all folders from source to target, preserving full hierarchy.

        Deprecated: use ``Migration.migrate_all_folders``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

        Fetches every folder from the source environment and delegates to
        ``migrate_folders``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every folder.
        concurrency : int, default 1
            Same as in ``migrate_folders`` — maximum number of folders to
            create/delete concurrently within each hierarchy depth level.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_folders``.
        """
        return self._as_migration().migrate_all_folders(action=action, concurrency=concurrency, emit=emit)
