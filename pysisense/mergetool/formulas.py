from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/formulas.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class FormulasMergeMixin:
    @deprecated("use Migration.migrate_saved_formulas")
    def migrate_saved_formulas(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved formula measures for specific data models from source to target.

        Deprecated: use ``Migration.migrate_saved_formulas``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches each data model's saved formula measures from the source and
        creates them on the target via the metadata measure endpoint.
        Conflict detection is based on a formula's ``title``. Requires the
        target data model to already exist — run
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
            Conflict strategy for formulas whose ``title`` already exists on
            the target datasource.

            - ``"skip"`` — leave the existing formula unchanged (default).
            - ``"overwrite"`` or ``"duplicate"`` — always create, regardless
              of conflicts. The Sisense metadata API exposes no update or
              delete endpoint for saved formulas, so ``"overwrite"`` cannot
              replace the existing formula in place and behaves identically
              to ``"duplicate"``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``datamodel`` and
              ``formula`` (the formula's title)
            - ``skipped`` : list[dict] — each has ``datamodel``, ``formula``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``datamodel``, ``formula``,
              and ``reason``
            - ``source_count`` : int — total saved formulas found across the
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
        return self._as_migration().migrate_saved_formulas(datamodel_ids=datamodel_ids, datamodel_names=datamodel_names, action=action, emit=emit)

    @deprecated("use Migration.migrate_all_saved_formulas")
    def migrate_all_saved_formulas(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate saved formula measures for all data models from source to target.

        Deprecated: use ``Migration.migrate_all_saved_formulas``, which runs the
        same code with the same parameters and result. MergeTool is removed in the
        next major release.

        Fetches every data model from the source environment and delegates
        to ``migrate_saved_formulas``. Data models not yet present on the
        target are skipped — run ``migrate_all_datamodels_v2`` first.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Same as in ``migrate_saved_formulas``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_saved_formulas``.
        """
        return self._as_migration().migrate_all_saved_formulas(action=action, emit=emit)
