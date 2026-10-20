from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from typing_extensions import deprecated


# Deprecated: each method runs the matching Migration method (pysisense/migration/dashboards_v2.py)
# through a Migration built on the same clients, with the same signature. A test fails if a
# signature here drifts from its Migration counterpart.
class DashboardMergeMixin:
    @deprecated("use Migration.migrate_dashboards_v2")
    def migrate_dashboards(
        self,
        dashboard_ids: list[str] | None = None,
        dashboard_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
        republish: bool = False,
        migrate_share: bool = True,
        change_ownership: bool = True,
    ) -> dict[str, Any]:
        """Migrate specific dashboards from source to target.

        Deprecated: use ``Migration.migrate_dashboards_v2``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Exports each dashboard from the source environment, repoints its
        embedded datasource references to the target's local Elasticube, and
        imports it into the target via the bulk import endpoint, which
        matches dashboards by ``oid`` and applies ``action`` natively.
        After a successful import, the dashboard's owner and shares are
        remapped from source to target users/groups (matched by email/name;
        see ``change_ownership`` and ``migrate_share``),
        and the dashboard is moved into the target folder whose path matches
        its source parent folder's path — if that folder has already been
        migrated with ``migrate_folders``. Conflict detection for the
        ``action`` parameter itself is based on the dashboard's ``oid``.

        Parameters
        ----------
        dashboard_ids : list[str] or None, default None
            Dashboard OIDs to migrate. Provide either this or
            ``dashboard_names``.
        dashboard_names : list[str] or None, default None
            Dashboard titles to migrate. Provide either this or
            ``dashboard_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a dashboard with the same ``oid`` already
            exists on the target.

            - ``"skip"`` — leave the existing dashboard unchanged.
            - ``"overwrite"`` — replace the existing dashboard with the
              source version.
            - ``"duplicate"`` — always create, regardless of conflicts.
        concurrency : int, default 1
            Maximum number of dashboards to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Dashboards are independent
            of each other, so any value is safe. Values <= 1 (the default)
            process dashboards one at a time.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys. When
            ``concurrency`` is greater than 1, this callback may be invoked
            from multiple worker threads concurrently.
        republish : bool, default False
            Republish each imported dashboard to the users it is shared with
            (``republish=true`` on the bulk import request).
        migrate_share : bool, default True
            Recreate the dashboard's shares on the target, matching users by
            email and groups by name. ``False`` leaves the imported dashboard
            unshared.
        change_ownership : bool, default True
            Give the target dashboard the same owner as on the source,
            matched by email. ``False`` leaves the token user as owner.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``oid``, and
              ``source_oid``
            - ``skipped`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
            - ``error`` : str — only when the run could not start
            - ``meta`` : dict — counters for each step:
              ``export_requested``/``export_succeeded``/``export_failed``,
              ``import_succeeded``/``import_failed``,
              ``owner_change_success_count``/``owner_change_fail_count``,
              ``share_success_count``/``share_fail_count``,
              ``folder_move_success_count``/``folder_move_fail_count``/
              ``folder_not_found_count``, and ``warnings`` (list[str]) for
              owner, share and folder problems that did not fail the
              dashboard

        Raises
        ------
        ValueError
            If both or neither of ``dashboard_ids`` and ``dashboard_names``
            are provided.

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        return self._as_migration().migrate_dashboards_v2(
            dashboard_ids=dashboard_ids,
            dashboard_names=dashboard_names,
            action=action,
            concurrency=concurrency,
            emit=emit,
            republish=republish,
            migrate_share=migrate_share,
            change_ownership=change_ownership,
        )

    @deprecated("use Migration.migrate_all_dashboards_v2")
    def migrate_all_dashboards(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
        republish: bool = False,
        migrate_share: bool = True,
        change_ownership: bool = True,
    ) -> dict[str, Any]:
        """Migrate all dashboards from source to target.

        Deprecated: use ``Migration.migrate_all_dashboards_v2``, which runs the same
        code with the same parameters and result. MergeTool is removed in the next
        major release.

        Fetches every dashboard from the source environment and delegates to
        ``migrate_dashboards``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every dashboard.
        concurrency : int, default 1
            Same as in ``migrate_dashboards``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.
        republish : bool, default False
            Same as in ``migrate_dashboards``.
        migrate_share : bool, default True
            Same as in ``migrate_dashboards``.
        change_ownership : bool, default True
            Same as in ``migrate_dashboards``.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_dashboards``.
        """
        return self._as_migration().migrate_all_dashboards_v2(action=action, concurrency=concurrency, emit=emit, republish=republish, migrate_share=migrate_share, change_ownership=change_ownership)
