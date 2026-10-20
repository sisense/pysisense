"""Tests for Migration's *_v2 methods and the deprecations around them.

The *_v2 methods hold the code that used to live in MergeTool; their behaviour
is covered by the test_mergetool_*.py files, which now run through the
deprecated MergeTool aliases. This file covers the layout (which method is
deprecated towards which) and the options the *_v2 methods gained.
"""

import inspect
import warnings

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool
from pysisense.migration import Migration

# Migration methods replaced by a *_v2 method.
_REPLACED = [
    "migrate_groups",
    "migrate_all_groups",
    "migrate_users",
    "migrate_all_users",
    "migrate_dashboards",
    "migrate_all_dashboards",
    "migrate_datamodels",
    "migrate_all_datamodels",
]

# Methods that already lived on Migration under the same name as on MergeTool.
_SAME_NAME = [
    "migrate_notebooks",
    "migrate_all_notebooks",
    "migrate_folders",
    "migrate_all_folders",
    "migrate_blox_actions",
    "migrate_all_blox_actions",
    "migrate_datasecurity",
    "migrate_all_datasecurity",
    "migrate_saved_formulas",
    "migrate_all_saved_formulas",
    "migrate_saved_filters",
    "migrate_all_saved_filters",
]

# Arguments a single-item method needs to get past input validation.
_REQUIRED_SELECTION = {
    "migrate_groups": {"group_name_list": ["x"]},
    "migrate_users": {"user_name_list": ["x"]},
    "migrate_dashboards": {"dashboard_ids": ["x"]},
    "migrate_datamodels": {"datamodel_ids": ["x"]},
}


def _clients():
    return FakeApiClient(logger=FakeLogger()), FakeApiClient(logger=FakeLogger())


def _public_methods(cls):
    return sorted(name for name, _ in inspect.getmembers(cls, inspect.isfunction) if not name.startswith("_"))


# ---------------------------------------------------------------------------
# Migration: old methods deprecated towards *_v2
# ---------------------------------------------------------------------------


class TestMigrationDeprecations:
    @pytest.mark.parametrize("name", _REPLACED)
    def test_old_method_points_at_its_v2_replacement(self, name):
        assert getattr(Migration, name).__deprecated__ == f"use {name}_v2"

    @pytest.mark.parametrize("name", _REPLACED)
    def test_v2_method_is_not_deprecated(self, name):
        assert getattr(getattr(Migration, f"{name}_v2"), "__deprecated__", None) is None

    @pytest.mark.parametrize("name", _SAME_NAME + ["migrate_dashboard_shares"])
    def test_other_migration_methods_are_not_deprecated(self, name):
        assert getattr(getattr(Migration, name), "__deprecated__", None) is None

    # An old _all_ method calls its single-item method, which warns again from
    # inside pysisense; Python's default filters hide that one from scripts.
    @pytest.mark.filterwarnings("ignore:use migrate_:DeprecationWarning")
    @pytest.mark.parametrize("name", _REPLACED)
    def test_calling_an_old_method_warns(self, name):
        src, tgt = _clients()
        migration = Migration(source_client=src, target_client=tgt)
        with pytest.warns(DeprecationWarning, match=f"use {name}_v2"):
            getattr(migration, name)(**_REQUIRED_SELECTION.get(name, {}))

    def test_v2_methods_raise_no_deprecation_warning(self):
        src, tgt = _clients()
        migration = Migration(source_client=src, target_client=tgt)
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            for name in _REPLACED:
                if not name.startswith("migrate_all_"):
                    continue
                getattr(migration, f"{name}_v2")()


# ---------------------------------------------------------------------------
# MergeTool: every method deprecated towards Migration
# ---------------------------------------------------------------------------


class TestMergeToolDeprecations:
    def test_every_public_mergetool_method_is_deprecated(self):
        live = [name for name in _public_methods(MergeTool) if getattr(getattr(MergeTool, name), "__deprecated__", None) is None]
        assert live == []

    @pytest.mark.parametrize("name", _REPLACED)
    def test_old_name_maps_to_the_v2_method(self, name):
        method = getattr(MergeTool, name)
        assert method.__deprecated__ == f"use Migration.{name}_v2"
        assert method.__name__ == name
        assert inspect.unwrap(method) is getattr(Migration, f"{name}_v2")
        assert inspect.signature(method) == inspect.signature(getattr(Migration, f"{name}_v2"))

    @pytest.mark.parametrize("name", _SAME_NAME)
    def test_same_name_methods_map_to_the_same_migration_method(self, name):
        method = getattr(MergeTool, name)
        assert method.__deprecated__ == f"use Migration.{name}"
        assert inspect.unwrap(method) is getattr(Migration, name)

    @pytest.mark.parametrize("name", [f"{n}_v2" for n in _REPLACED])
    def test_inherited_v2_names_are_deprecated_too(self, name):
        assert getattr(MergeTool, name).__deprecated__ == f"use Migration.{name}"

    @pytest.mark.filterwarnings("ignore:use Migration\\.:DeprecationWarning")
    def test_calling_a_mergetool_method_warns_once_on_the_callers_line(self):
        src, tgt = _clients()
        merge = MergeTool(source_client=src, target_client=tgt)
        with pytest.warns(DeprecationWarning, match="use Migration.migrate_all_groups_v2") as record:
            merge.migrate_all_groups()
        assert record[0].filename == __file__


# ---------------------------------------------------------------------------
# New options on the *_v2 methods
# ---------------------------------------------------------------------------


class CapturingFakeApiClient(FakeApiClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = []

    def post(self, url, data=None, **kwargs):
        self.calls.append(("POST", url, data))
        return super().post(url, data=data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        self.calls.append(("PATCH", url, data))
        return super().patch(url, data=data, **kwargs)


_EXPORTED_DASHBOARD = {
    "oid": "dash1",
    "title": "Sales",
    "owner": "src_user_1",
    "shares": [{"type": "user", "shareId": "src_user_1", "rule": "edit"}],
}


def _dashboard_migration():
    src = FakeApiClient(
        get_responses={
            "/api/v1/dashboards/admin": FakeResponse(200, [{"oid": "dash1", "title": "Sales", "owner": "src_user_1"}]),
            "/api/v1/users": FakeResponse(200, [{"_id": "src_user_1", "email": "alice@example.com"}]),
            "/api/v1/groups": FakeResponse(200, []),
            "/api/v1/folders": FakeResponse(200, []),
            "/api/v1/dashboards/export?dashboardIds=dash1&adminAccess=true": FakeResponse(200, [_EXPORTED_DASHBOARD]),
        },
        logger=FakeLogger(),
    )
    tgt = CapturingFakeApiClient(
        get_responses={
            "/api/v1/dashboards/admin": FakeResponse(200, []),
            "/api/v1/users": FakeResponse(200, [{"_id": "tgt_user_1", "email": "alice@example.com"}]),
            "/api/v1/groups": FakeResponse(200, []),
            "/api/v1/folders": FakeResponse(200, []),
            "/api/shares/dashboard/dash1": FakeResponse(200, {"sharesTo": []}),
        },
        post_responses={
            "/api/v1/dashboards/import/bulk": FakeResponse(200, {"succeded": [{"oid": "dash1", "title": "Sales"}], "failed": []}),
            "/api/v1/dashboards/dash1/change_owner": FakeResponse(200, {"success": True}),
            "/api/shares/dashboard/dash1": FakeResponse(200, {"success": True}),
        },
        logger=FakeLogger(),
    )
    return Migration(source_client=src, target_client=tgt), tgt


class TestDashboardsV2Options:
    def test_by_default_owner_and_shares_move_and_nothing_is_republished(self):
        migration, tgt = _dashboard_migration()
        result = migration.migrate_dashboards_v2(dashboard_ids=["dash1"])
        assert result["succeeded_count"] == 1
        import_url = next(c[1] for c in tgt.calls if c[1].startswith("/api/v1/dashboards/import/bulk"))
        assert "republish" not in import_url
        assert any("change_owner" in c[1] for c in tgt.calls)
        assert any(c[1].startswith("/api/shares/dashboard/dash1") for c in tgt.calls)

    def test_republish_adds_the_query_parameter(self):
        migration, tgt = _dashboard_migration()
        migration.migrate_dashboards_v2(dashboard_ids=["dash1"], republish=True)
        import_url = next(c[1] for c in tgt.calls if c[1].startswith("/api/v1/dashboards/import/bulk"))
        assert import_url.endswith("&republish=true")

    def test_migrate_share_false_leaves_shares_alone(self):
        migration, tgt = _dashboard_migration()
        result = migration.migrate_dashboards_v2(dashboard_ids=["dash1"], migrate_share=False)
        assert result["succeeded_count"] == 1
        assert not any(c[1].startswith("/api/shares/dashboard/dash1") for c in tgt.calls)
        assert any("change_owner" in c[1] for c in tgt.calls)

    def test_change_ownership_false_leaves_the_owner_alone(self):
        migration, tgt = _dashboard_migration()
        result = migration.migrate_dashboards_v2(dashboard_ids=["dash1"], change_ownership=False)
        assert result["succeeded_count"] == 1
        assert not any("change_owner" in c[1] for c in tgt.calls)

    def test_migrate_all_passes_the_options_through(self):
        migration, tgt = _dashboard_migration()
        migration.migrate_all_dashboards_v2(republish=True, migrate_share=False, change_ownership=False)
        import_url = next(c[1] for c in tgt.calls if c[1].startswith("/api/v1/dashboards/import/bulk"))
        assert import_url.endswith("&republish=true")
        assert not any("change_owner" in c[1] or c[1].startswith("/api/shares/") for c in tgt.calls)


def _ecm(datamodels):
    return FakeResponse(200, {"data": {"elasticubesMetadata": datamodels}})


class TestDatamodelsV2NewTitle:
    def test_new_title_names_the_duplicate(self):
        model = {"oid": "dm1", "title": "Sample ECommerce", "type": "extract"}
        src = FakeApiClient(
            get_responses={"/api/v2/datamodel-exports/schema": FakeResponse(200, {"oid": "dm1", "title": "Sample ECommerce", "datasets": []})},
            post_responses={"/api/v2/ecm/": _ecm([model])},
            logger=FakeLogger(),
        )
        tgt = CapturingFakeApiClient(
            post_responses={"/api/v2/ecm/": _ecm([model]), "/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm9"})},
            logger=FakeLogger(),
        )
        result = Migration(source_client=src, target_client=tgt).migrate_datamodels_v2(datamodel_ids=["dm1"], action="duplicate", new_title="Sample ECommerce Copy")
        assert result["succeeded"][0]["target_id"] == "dm9"
        import_url = next(c[1] for c in tgt.calls if c[1].startswith("/api/v2/datamodel-imports/schema"))
        assert import_url == "/api/v2/datamodel-imports/schema?newTitle=Sample ECommerce Copy"


class TestRunThatCannotStart:
    @pytest.mark.parametrize("name", ["migrate_all_groups_v2", "migrate_all_users_v2", "migrate_all_dashboards_v2", "migrate_all_datamodels_v2"])
    def test_source_failure_carries_an_error_sentence(self, name):
        src = FakeApiClient(
            get_responses={
                "/api/v1/groups": FakeResponse(500, {"error": "boom"}),
                "/api/v1/users": FakeResponse(500, {"error": "boom"}),
                "/api/v1/dashboards/admin": FakeResponse(500, {"error": "boom"}),
            },
            post_responses={"/api/v2/ecm/": FakeResponse(500, {"error": "boom"})},
            logger=FakeLogger(),
        )
        result = getattr(Migration(source_client=src, target_client=FakeApiClient(logger=FakeLogger())), name)()
        assert result["ok"] is False
        assert isinstance(result.get("error"), str) and result["error"]
