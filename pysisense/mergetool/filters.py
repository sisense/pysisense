from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/filters.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class FiltersMergeMixin:
    @deprecated("use Migration.migrate_saved_filters")
    def migrate_saved_filters(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved filter dimensions for specific data models from source to target.

        Deprecated: use ``Migration.migrate_saved_filters``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches each data model's dimensions from the source, keeps only the
        ones that carry a saved filter definition (a truthy ``filter`` key),
        and recreates them on the target via a raw metadata query. Conflict
        detection is based on a filter's ``title``. Requires the target data
        model to already exist — run
        ``migrate_datamodels_v2``/``migrate_all_datamodels_v2`` first.

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
        return self._as_migration().migrate_saved_filters(datamodel_ids=datamodel_ids, datamodel_names=datamodel_names, action=action, emit=emit)

    @deprecated("use Migration.migrate_all_saved_filters")
    def migrate_all_saved_filters(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved filter dimensions for all data models from source to target.

        Deprecated: use ``Migration.migrate_all_saved_filters``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every data model from the source environment and delegates
        to ``migrate_saved_filters``. Data models not yet present on the
        target are skipped — run ``migrate_all_datamodels_v2`` first.

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
        return self._as_migration().migrate_all_saved_filters(action=action, emit=emit)
