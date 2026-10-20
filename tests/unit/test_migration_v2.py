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


# ---------------------------------------------------------------------------
# meta counters on the *_v2 results
# ---------------------------------------------------------------------------

_SYSTEM_TENANTS = [{"_id": "t-system", "name": "system"}, {"_id": "t-acme", "name": "Acme"}]


class TestGroupsV2Meta:
    def _migration(self, tgt_groups, tenants=None):
        src = FakeApiClient(
            get_responses={
                "/api/v1/groups": FakeResponse(
                    200,
                    [
                        {"_id": "g1", "name": "Sales", "tenantId": "t-system"},
                        {"_id": "g2", "name": "Finance", "tenantId": "t-system"},
                        {"_id": "g3", "name": "Admins", "tenantId": "t-system"},
                        {"_id": "g4", "name": "Acme Team", "tenantId": "t-acme"},
                    ],
                ),
                "/api/v1/tenants": tenants if tenants is not None else FakeResponse(200, _SYSTEM_TENANTS),
            },
            logger=FakeLogger(),
        )
        tgt = FakeApiClient(
            get_responses={"/api/v1/groups": FakeResponse(200, tgt_groups)},
            post_responses={"/api/v1/groups/bulk": FakeResponse(201, [{"name": "Finance"}])},
            logger=FakeLogger(),
        )
        return Migration(source_client=src, target_client=tgt)

    def test_migrate_all_groups_reports_why_groups_were_left_out(self):
        result = self._migration([{"_id": "t1", "name": "Sales"}]).migrate_all_groups_v2()
        meta = result["meta"]
        assert meta["source_total_count"] == 4
        assert meta["eligible_count"] == 2
        assert meta["excluded_count"] == 1
        assert meta["skipped_multi_tenant_count"] == 1
        assert meta["existing_on_target_count"] == 1
        assert meta["warnings"] == []

    def test_unreadable_tenants_are_reported(self):
        result = self._migration([], tenants=FakeResponse(500, {"error": "boom"})).migrate_all_groups_v2()
        assert any("tenants" in w for w in result["meta"]["warnings"])

    def test_duplicate_reports_groups_that_already_exist(self):
        result = self._migration([{"_id": "t1", "name": "Sales"}]).migrate_groups_v2(group_names=["Sales"], action="duplicate")
        assert "1 groups already exist in the target environment." in result["meta"]["warnings"]


class TestUsersV2Meta:
    def test_counts_missing_roles_groups_and_skipped_users(self):
        users = [
            {"_id": "u1", "email": "ann@example.com", "tenantId": "t-system", "role": {"_id": "r_consumer", "name": "consumer"}, "groups": [{"name": "Ghost Group"}]},
            {"_id": "u2", "email": "bob@example.com", "tenantId": "t-system", "role": {"_id": "r_custom", "name": "custom_x"}, "groups": []},
            {"_id": "u3", "email": "root@example.com", "tenantId": "t-system", "role": {"_id": "r_super", "name": "super"}, "groups": []},
            {"_id": "u4", "email": "acme@example.com", "tenantId": "t-acme", "role": {"_id": "r_consumer", "name": "consumer"}, "groups": []},
        ]
        src = FakeApiClient(
            get_responses={
                "/api/v1/users": FakeResponse(200, users),
                "/api/roles": FakeResponse(200, [{"_id": "r_consumer", "name": "consumer"}, {"_id": "r_custom", "name": "custom_x"}, {"_id": "r_super", "name": "super"}]),
                "/api/v1/tenants": FakeResponse(200, _SYSTEM_TENANTS),
            },
            logger=FakeLogger(),
        )
        tgt = FakeApiClient(
            get_responses={"/api/v1/users": FakeResponse(200, []), "/api/roles": FakeResponse(200, [{"_id": "t_consumer", "name": "consumer"}]), "/api/v1/groups": FakeResponse(200, [])},
            post_responses={"/api/v1/users/bulk": FakeResponse(201, [{"email": "ann@example.com"}])},
            logger=FakeLogger(),
        )
        meta = Migration(source_client=src, target_client=tgt).migrate_all_users_v2()["meta"]
        assert meta["source_total_count"] == 4
        assert meta["eligible_count"] == 2
        assert meta["skipped_super_count"] == 1
        assert meta["skipped_multi_tenant_count"] == 1
        assert meta["missing_role_mappings_count"] == 1
        assert meta["unmapped_role_names"] == ["custom_x", "super"]  # every source role without a target match
        assert meta["missing_group_mappings_count"] == 1
        assert any("Ghost Group" in w for w in meta["warnings"])


class TestDashboardsV2Meta:
    def test_counts_each_step(self):
        migration, _ = _dashboard_migration()
        meta = migration.migrate_dashboards_v2(dashboard_ids=["dash1"])["meta"]
        assert (meta["export_requested"], meta["export_succeeded"], meta["export_failed"]) == (1, 1, 0)
        assert (meta["import_succeeded"], meta["import_failed"]) == (1, 0)
        assert meta["owner_change_success_count"] == 1
        assert meta["share_success_count"] == 1

    def test_an_owner_missing_on_the_target_is_a_warning_not_a_failure(self):
        migration, _ = _dashboard_migration()
        migration.target_client._get["/api/v1/users"] = FakeResponse(200, [])
        result = migration.migrate_dashboards_v2(dashboard_ids=["dash1"], migrate_share=False)
        assert result["succeeded_count"] == 1
        assert result["meta"]["owner_change_fail_count"] == 1
        assert any("Owner of dashboard 'Sales'" in w for w in result["meta"]["warnings"])


class TestDatamodelsV2Meta:
    def _migration(self, shares_response):
        model = {"oid": "dm1", "title": "Sample ECommerce", "type": "extract"}
        src = FakeApiClient(
            get_responses={
                "/api/v2/datamodel-exports/schema": FakeResponse(200, {"oid": "dm1", "title": "Sample ECommerce", "datasets": []}),
                "/api/elasticubes/localhost/Sample ECommerce/permissions": shares_response,
                "/api/v1/users": FakeResponse(200, []),
                "/api/v1/groups": FakeResponse(200, []),
            },
            post_responses={"/api/v2/ecm/": _ecm([model])},
            logger=FakeLogger(),
        )
        tgt = FakeApiClient(
            get_responses={"/api/v1/users": FakeResponse(200, []), "/api/v1/groups": FakeResponse(200, [])},
            post_responses={"/api/v2/ecm/": _ecm([]), "/api/v2/datamodel-imports/schema": FakeResponse(201, {"oid": "dm1"})},
            logger=FakeLogger(),
        )
        return Migration(source_client=src, target_client=tgt)

    def test_counts_each_step_and_records_the_request(self):
        meta = self._migration(FakeResponse(200, [])).migrate_datamodels_v2(datamodel_ids=["dm1"], dependencies="formulas", shares=True)["meta"]
        assert meta["dependencies"] == ["formulas"]
        assert meta["api_dependencies"] == ["formulaManagement"]
        assert meta["shares_requested"] is True
        assert (meta["export_requested"], meta["export_succeeded"], meta["import_succeeded"]) == (1, 1, 1)

    def test_a_share_read_failure_is_counted_and_warned(self):
        result = self._migration(FakeResponse(500, {"error": "boom"})).migrate_datamodels_v2(datamodel_ids=["dm1"], shares=True)
        assert result["succeeded_count"] == 1
        assert result["meta"]["share_fail_count"] == 1
        assert result["meta"]["warnings"]


class TestMetaIsAlwaysPresent:
    @pytest.mark.parametrize("name", ["migrate_all_groups_v2", "migrate_all_users_v2", "migrate_all_dashboards_v2", "migrate_all_datamodels_v2"])
    def test_on_failure_and_on_noop(self, name):
        failing = FakeApiClient(
            get_responses={"/api/v1/groups": FakeResponse(500, {}), "/api/v1/users": FakeResponse(500, {}), "/api/v1/dashboards/admin": FakeResponse(500, {})},
            post_responses={"/api/v2/ecm/": FakeResponse(500, {})},
            logger=FakeLogger(),
        )
        empty = FakeApiClient(
            get_responses={"/api/v1/groups": FakeResponse(200, []), "/api/v1/users": FakeResponse(200, []), "/api/v1/dashboards/admin": FakeResponse(200, [])},
            post_responses={"/api/v2/ecm/": _ecm([])},
            logger=FakeLogger(),
        )
        for src in (failing, empty):
            result = getattr(Migration(source_client=src, target_client=FakeApiClient(logger=FakeLogger())), name)()
            assert isinstance(result["meta"], dict)
            assert isinstance(result["meta"]["warnings"], list)
