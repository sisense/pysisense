from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/custom_code.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class CustomCodeMergeMixin:
    @deprecated("use Migration.migrate_notebooks")
    def migrate_notebooks(
        self,
        notebook_ids: list[str] | None = None,
        notebook_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific notebooks from source to target.

        Deprecated: use ``Migration.migrate_notebooks``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

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
        concurrency : int, default 1
            Maximum number of notebooks to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Notebooks are independent
            of each other, so any value is safe. Values <= 1 (the default)
            process notebooks one at a time.
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

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        return self._as_migration().migrate_notebooks(notebook_ids=notebook_ids, notebook_names=notebook_names, action=action, concurrency=concurrency, emit=emit)

    @deprecated("use Migration.migrate_all_notebooks")
    def migrate_all_notebooks(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all notebooks from source to target.

        Deprecated: use ``Migration.migrate_all_notebooks``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every notebook from the source environment and delegates to
        ``migrate_notebooks``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every notebook.
        concurrency : int, default 1
            Same as in ``migrate_notebooks``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_notebooks``.
        """
        return self._as_migration().migrate_all_notebooks(action=action, concurrency=concurrency, emit=emit)
