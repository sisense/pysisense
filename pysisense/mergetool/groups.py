from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/groups_v2.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class GroupsMergeMixin:
    @deprecated("use Migration.migrate_groups_v2")
    def migrate_groups(
        self,
        group_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific groups from source to target.

        Deprecated: use ``Migration.migrate_groups_v2``, which runs the same code
        with the same parameters and result. MergeTool is removed in the next major
        release.

        Fetches the requested groups from the source environment and creates
        (or replaces) them on the target via the bulk group endpoint. Conflict
        detection is based on the group's ``name`` field.

        Parameters
        ----------
        group_names : list[str] or None, default None
            Group names to migrate. If ``None``, every group on the source is
            migrated.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a group with the same ``name`` already
            exists on the target.

            - ``"skip"`` — leave the existing group unchanged.
            - ``"overwrite"`` — delete the existing group then recreate from
              source. **Warning:** this can disrupt user/group associations
              still referencing the deleted group on the target.
            - ``"duplicate"`` — always create, regardless of conflicts.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``name``
            - ``skipped`` : list[dict] — each has ``name`` and ``reason``
            - ``failed`` : list[dict] — each has ``name`` and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
            - ``error`` : str — only when the run could not start
            - ``meta`` : dict — ``existing_on_target_count`` (selected
              groups that already exist on the target) and ``warnings``
              (list[str]: unreadable target groups, failed deletes on
              overwrite, existing groups sent with ``"duplicate"``)
        """
        return self._as_migration().migrate_groups_v2(group_names=group_names, action=action, emit=emit)

    @deprecated("use Migration.migrate_all_groups_v2")
    def migrate_all_groups(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all eligible groups from source to target.

        Deprecated: use ``Migration.migrate_all_groups_v2``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every group from the source environment, excludes the
        built-in ``Admins``, ``All users in system``, and ``Everyone`` groups,
        and — when the source environment exposes tenant information —
        restricts migration to groups belonging to the system tenant, then
        delegates to ``migrate_groups``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every group.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_groups``. ``meta`` also carries
            ``source_total_count`` (every group on the source),
            ``eligible_count``, ``excluded_count`` (built-in groups and
            groups without a name) and ``skipped_multi_tenant_count``.

        Notes
        -----
        If the source environment does not expose tenant information (single
        tenant on-premises deployments), tenant-based filtering is skipped and
        every non-excluded group is treated as eligible.
        """
        return self._as_migration().migrate_all_groups_v2(action=action, emit=emit)
