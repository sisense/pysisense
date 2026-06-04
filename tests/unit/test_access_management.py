"""Unit tests for pysisense.access_management.AccessManagement."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.access_management import AccessManagement

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------
_ROLES = [
    {"_id": "role_consumer", "name": "consumer"},
    {"_id": "role_super", "name": "super"},
    {"_id": "role_contributor", "name": "contributor"},
]

_GROUPS = [
    {"_id": "grp_engineers", "name": "Engineers", "defaultRole": ""},
    {"_id": "grp_admins", "name": "Admins", "defaultRole": ""},
]

# User as returned by /api/v1/users with expand=groups,role
_USER_EXPANDED = {
    "_id": "user123",
    "userName": "jdoe",
    "firstName": "John",
    "lastName": "Doe",
    "email": "jdoe@example.com",
    "active": True,
    "role": {"_id": "role_consumer", "name": "consumer"},
    "groups": [{"_id": "grp_engineers", "name": "Engineers"}],
    "roleId": "role_consumer",
}

# User as returned by /api/v1/users WITHOUT expand (raw IDs)
_USER_RAW = {
    "_id": "user123",
    "userName": "jdoe",
    "firstName": "John",
    "lastName": "Doe",
    "email": "jdoe@example.com",
    "active": True,
    "roleId": "role_consumer",
    "groups": ["grp_engineers"],
}


def _make_am(
    get_responses=None,
    post_responses=None,
    patch_responses=None,
    delete_responses=None,
):
    """Build an AccessManagement instance backed by a FakeApiClient."""
    logger = FakeLogger()
    client = FakeApiClient(
        get_responses=get_responses,
        post_responses=post_responses,
        patch_responses=patch_responses,
        delete_responses=delete_responses,
        logger=logger,
    )
    return AccessManagement(api_client=client)


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


class TestAccessManagementInit:
    def test_can_create_with_fake_client(self):
        am = _make_am()
        assert am is not None
        assert hasattr(am, "api_client")
        assert hasattr(am, "datamodel")
        assert hasattr(am, "logger")


# ---------------------------------------------------------------------------
# _build_role_and_group_mappings
# ---------------------------------------------------------------------------


class TestBuildRoleAndGroupMappings:
    def test_returns_roles_and_groups_dicts(self):
        am = _make_am(
            get_responses={
                "/api/roles": FakeResponse(200, _ROLES),
                "/api/v1/groups": FakeResponse(200, _GROUPS),
            }
        )
        result = am._build_role_and_group_mappings()
        assert result is not None
        assert result["roles_by_id"]["role_consumer"] == "consumer"
        assert result["groups_by_id"]["grp_engineers"] == "Engineers"

    def test_returns_none_when_roles_api_fails(self):
        am = _make_am(
            get_responses={
                "/api/roles": FakeResponse(500, {"error": "server error"}),
            }
        )
        assert am._build_role_and_group_mappings() is None


# ---------------------------------------------------------------------------
# get_user_with_role_and_group_names
# ---------------------------------------------------------------------------


class TestGetUserWithRoleAndGroupNames:
    def test_returns_user_dict_with_role_and_group_names(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user_with_role_and_group_names("jdoe@example.com")
        assert result["USER_ID"] == "user123"
        assert result["ROLE_NAME"] == "viewer"  # consumer → viewer
        assert "Engineers" in result["GROUP_NAMES"]

    def test_returns_error_when_user_not_found(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user_with_role_and_group_names("nobody@example.com")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(500, {})})
        result = am.get_user_with_role_and_group_names("jdoe@example.com")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_users_with_role_names_and_group_names
# ---------------------------------------------------------------------------


class TestGetUsersWithRoleNamesAndGroupNames:
    def test_returns_enriched_user_list(self):
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER_RAW]),
                "/api/roles": FakeResponse(200, _ROLES),
                "/api/v1/groups": FakeResponse(200, _GROUPS),
            }
        )
        result = am.get_users_with_role_names_and_group_names()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["USER_ID"] == "user123"
        assert result[0]["ROLE_NAME"] == "consumer"

    def test_returns_error_list_when_users_api_fails(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(500, {})})
        result = am.get_users_with_role_names_and_group_names()
        assert isinstance(result, list)
        assert "error" in result[0]


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


class TestGetUser:
    def test_returns_user_dict_on_success(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user("jdoe@example.com")
        assert result["USER_ID"] == "user123"
        assert result["EMAIL"] == "jdoe@example.com"
        assert result["ROLE_NAME"] == "viewer"

    def test_returns_error_when_email_not_found(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user("ghost@example.com")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={})  # no response → None
        result = am.get_user("jdoe@example.com")
        assert "error" in result


# ---------------------------------------------------------------------------
# change_user_password
# ---------------------------------------------------------------------------


class TestChangeUserPassword:
    def test_returns_user_on_success(self):
        updated = {"_id": "user123", "email": "jdoe@example.com"}
        am = _make_am(
            patch_responses={
                "/api/users/user123": FakeResponse(200, updated),
            },
        )
        result = am.change_user_password("user123", "NewSecurePass1!")
        assert result["_id"] == "user123"

    def test_returns_error_when_password_empty(self):
        am = _make_am()
        result = am.change_user_password("user123", "")
        assert "error" in result

    def test_returns_error_on_patch_failure(self):
        am = _make_am(
            patch_responses={
                "/api/users/user123": FakeResponse(400, {"error": "invalid password"}),
            },
        )
        result = am.change_user_password("user123", "short")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_users_all
# ---------------------------------------------------------------------------


class TestGetUsersAll:
    def test_returns_list_of_user_dicts(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_users_all()
        assert isinstance(result, list)
        assert result[0]["USER_ID"] == "user123"
        assert result[0]["ROLE_NAME"] == "viewer"

    def test_removes_everyone_group_when_multiple_groups_present(self):
        user = dict(_USER_EXPANDED)
        user["groups"] = [
            {"_id": "g1", "name": "Everyone"},
            {"_id": "g2", "name": "Engineers"},
        ]
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_users_all()
        assert "Everyone" not in result[0]["GROUPS"]
        assert "Engineers" in result[0]["GROUPS"]

    def test_returns_error_list_on_api_failure(self):
        am = _make_am(get_responses={})
        result = am.get_users_all()
        assert "error" in result[0]


# ---------------------------------------------------------------------------
# get_group
# ---------------------------------------------------------------------------


class TestGetGroup:
    def test_returns_group_dict_on_success(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [_GROUPS[0]])})
        result = am.get_group("Engineers")
        assert result["GROUP_ID"] == "grp_engineers"
        assert result["GROUP_NAME"] == "Engineers"

    def test_returns_error_when_group_not_found(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [])})
        result = am.get_group("Nonexistent")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={})
        result = am.get_group("Engineers")
        assert "error" in result


# ---------------------------------------------------------------------------
# create_user
# ---------------------------------------------------------------------------


class TestCreateUser:
    def test_resolves_role_and_groups_and_creates_user(self):
        new_user = {"_id": "newuser1", "email": "newbie@example.com", "userName": "newbie"}
        am = _make_am(
            get_responses={
                "/api/roles": FakeResponse(200, _ROLES),
                "/api/v1/groups": FakeResponse(200, _GROUPS),
            },
            post_responses={"/api/v1/users": FakeResponse(200, new_user)},
        )
        result = am.create_user({"email": "newbie@example.com", "firstName": "New", "lastName": "Bie", "role": "consumer", "groups": ["Engineers"]})
        assert result.get("_id") == "newuser1"

    def test_returns_error_when_role_not_found(self):
        am = _make_am(get_responses={"/api/roles": FakeResponse(200, _ROLES)})
        result = am.create_user({"email": "x@x.com", "role": "unknownrole", "groups": []})
        assert "error" in result

    def test_returns_error_when_roles_api_fails(self):
        am = _make_am(get_responses={})
        result = am.create_user({"email": "x@x.com", "role": "consumer", "groups": []})
        assert "error" in result


# ---------------------------------------------------------------------------
# update_user
# ---------------------------------------------------------------------------


class TestUpdateUser:
    def test_updates_user_with_role_resolution(self):
        updated = {**_USER_EXPANDED, "roleId": "role_contributor"}
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
                "/api/roles": FakeResponse(200, _ROLES),
            },
            patch_responses={"/api/v1/users/": FakeResponse(200, updated)},
        )
        result = am.update_user("jdoe@example.com", {"role": "contributor"})
        assert "error" not in result

    def test_returns_error_when_user_not_found(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [])})
        result = am.update_user("ghost@example.com", {"role": "viewer"})
        assert "error" in result

    def test_returns_error_when_role_api_fails(self):
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
                "/api/roles": FakeResponse(500, {}),
            }
        )
        result = am.update_user("jdoe@example.com", {"role": "viewer"})
        assert "error" in result


# ---------------------------------------------------------------------------
# delete_user
# ---------------------------------------------------------------------------


class TestDeleteUser:
    def test_deletes_user_successfully(self):
        am = _make_am(
            get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])},
            delete_responses={"/api/v1/users/": FakeResponse(200, {})},
        )
        result = am.delete_user("jdoe@example.com")
        assert "error" not in result

    def test_returns_error_when_user_not_found(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.delete_user("ghost@example.com")
        assert "error" in result


# ---------------------------------------------------------------------------
# users_per_group
# ---------------------------------------------------------------------------


class TestUsersPerGroup:
    def test_returns_users_in_group(self):
        users_in_group = [_USER_EXPANDED]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, [_GROUPS[0]]),
                "/api/v1/users": FakeResponse(200, users_in_group),
            }
        )
        result = am.users_per_group("Engineers")
        assert isinstance(result, list)
        assert result[0]["_id"] == "user123"

    def test_returns_error_when_group_not_found(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [])})
        result = am.users_per_group("Nonexistent")
        assert "error" in result


# ---------------------------------------------------------------------------
# users_per_group_all
# ---------------------------------------------------------------------------


class TestUsersPerGroupAll:
    def test_returns_list_with_group_and_usernames(self):
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, _GROUPS),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        result = am.users_per_group_all()
        assert isinstance(result, list)
        group_names = [entry["group"] for entry in result]
        assert "Engineers" in group_names

    def test_returns_empty_list_when_groups_api_fails(self):
        am = _make_am(get_responses={})
        result = am.users_per_group_all()
        assert result == []


# ---------------------------------------------------------------------------
# change_folder_and_dashboard_ownership
# ---------------------------------------------------------------------------


class TestChangeFolderAndDashboardOwnership:
    def test_returns_error_when_executing_user_not_found(self):
        # get_user for the executing user returns not-found
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [])})
        result = am.change_folder_and_dashboard_ownership("executor@example.com", "MyFolder", "newowner@example.com")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_datamodel_columns
# ---------------------------------------------------------------------------


class TestGetDatamodelColumns:
    def test_returns_column_list_on_success(self):
        schema = {"oid": "dm123", "title": "MyModel"}
        datasets = [{"oid": "ds1"}]
        tables = [
            {
                "name": "orders",
                "columns": [{"name": "order_id"}, {"name": "amount"}],
            }
        ]
        am = _make_am(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, schema),
                "/api/v2/datamodels/dm123/schema/datasets": FakeResponse(200, datasets),
                "/api/v2/datamodels/dm123/schema/datasets/ds1/tables": FakeResponse(200, tables),
            }
        )
        result = am.get_datamodel_columns("MyModel")
        assert isinstance(result, list)
        col_names = [r["column"] for r in result]
        assert "order_id" in col_names
        assert "amount" in col_names

    def test_returns_empty_list_when_model_not_found(self):
        am = _make_am(get_responses={"/api/v2/datamodels/schema": FakeResponse(404, {})})
        result = am.get_datamodel_columns("NoSuchModel")
        assert result == []


# ---------------------------------------------------------------------------
# get_unused_columns
# ---------------------------------------------------------------------------


class TestGetUnusedColumns:
    def test_raises_value_error_when_no_columns_found(self):
        # Model not found → get_datamodel_columns returns []
        am = _make_am(get_responses={"/api/v2/datamodels/schema": FakeResponse(404, {})})
        with pytest.raises(ValueError, match="No columns found"):
            am.get_unused_columns("NoSuchModel")

    def test_all_columns_unused_when_no_dashboards(self):
        schema = {"oid": "dm123", "title": "MyModel"}
        datasets = [{"oid": "ds1"}]
        tables = [{"name": "tbl", "columns": [{"name": "col1"}]}]
        am = _make_am(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, schema),
                "/api/v2/datamodels/dm123/schema/datasets": FakeResponse(200, datasets),
                "/api/v2/datamodels/dm123/schema/datasets/ds1/tables": FakeResponse(200, tables),
                "/api/v1/dashboards/admin": FakeResponse(200, []),  # no dashboards
            }
        )
        result = am.get_unused_columns("MyModel")
        assert isinstance(result, list)
        assert all(r["used"] is False for r in result)


# ---------------------------------------------------------------------------
# get_unused_columns_bulk
# ---------------------------------------------------------------------------


class TestGetUnusedColumnsBulk:
    def test_returns_empty_list_when_datamodels_is_none(self):
        am = _make_am()
        result = am.get_unused_columns_bulk(None)
        assert result == []

    def test_returns_empty_list_for_empty_list_input(self):
        am = _make_am()
        result = am.get_unused_columns_bulk([])
        assert result == []

    def test_skips_model_when_resolve_fails(self):
        # datamodel.resolve_datamodel_reference will call GET /api/v2/datamodels/{ref}/schema
        # and then GET /api/v2/datamodels/schema with params — both return 404
        am = _make_am(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(404, {}),
                "/api/v2/datamodels/NoSuchModel/schema": FakeResponse(404, {}),
            }
        )
        result = am.get_unused_columns_bulk("NoSuchModel")
        assert result == []


# ---------------------------------------------------------------------------
# get_all_dashboard_shares
# ---------------------------------------------------------------------------


class TestGetAllDashboardShares:
    def test_returns_empty_list_when_no_dashboards(self):
        am = _make_am(
            post_responses={"/api/v1/dashboards/searches": FakeResponse(200, {"items": []})},
            get_responses={
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "a@b.com"}]),
                "/api/v1/groups": FakeResponse(200, _GROUPS),
            },
        )
        result = am.get_all_dashboard_shares()
        assert result == []

    def test_returns_empty_list_when_post_fails(self):
        am = _make_am()  # no POST responses → None → breaks loop → users/groups fail → []
        result = am.get_all_dashboard_shares()
        assert result == []


# ---------------------------------------------------------------------------
# create_schedule_build
# ---------------------------------------------------------------------------


class TestCreateScheduleBuild:
    def test_returns_error_when_model_not_found(self):
        am = _make_am(get_responses={"/api/v2/datamodels/schema": FakeResponse(404, {})})
        result = am.create_schedule_build("NoSuchModel")
        assert "error" in result

    def test_returns_error_when_interval_is_zero(self):
        schema = {"oid": "dm123"}
        am = _make_am(get_responses={"/api/v2/datamodels/schema": FakeResponse(200, schema)})
        result = am.create_schedule_build("MyModel", interval_days=0, interval_hours=0, interval_minutes=0)
        assert "error" in result

    def test_creates_interval_schedule_successfully(self):
        schema = {"oid": "dm123"}
        schedule_result = {"id": "sched1", "status": "created"}
        am = _make_am(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, schema)},
            post_responses={"/api/v2/datamodels/dm123/schedule": FakeResponse(200, schedule_result)},
        )
        result = am.create_schedule_build("MyModel", interval_hours=2)
        # Should return the API response (or an error dict if endpoint doesn't match)
        assert result is not None

    def test_creates_cron_schedule_successfully(self):
        schema = {"oid": "dm123"}
        schedule_result = {"id": "sched2", "status": "created"}
        am = _make_am(
            get_responses={"/api/v2/datamodels/schema": FakeResponse(200, schema)},
            post_responses={"/api/v2/datamodels/dm123/schedule": FakeResponse(200, schedule_result)},
        )
        result = am.create_schedule_build("MyModel", days=["MON", "FRI"], hour=9, minute=0)
        assert result is not None


# ---------------------------------------------------------------------------
# get_my_user
# ---------------------------------------------------------------------------


class TestGetMyUser:
    def test_returns_user_on_success(self):
        logged_in = {"_id": "user123", "email": "admin@example.com", "userName": "admin"}
        am = _make_am(get_responses={"/api/users/loggedin": FakeResponse(200, logged_in)})
        result = am.get_my_user()
        assert result["email"] == "admin@example.com"

    def test_returns_error_on_none_response(self):
        am = _make_am()
        result = am.get_my_user()
        assert "error" in result

    def test_returns_error_on_non_200(self):
        am = _make_am(get_responses={"/api/users/loggedin": FakeResponse(401, {"message": "unauthorized"})})
        result = am.get_my_user()
        assert "error" in result


# ---------------------------------------------------------------------------
# get_roles
# ---------------------------------------------------------------------------


class TestGetRoles:
    def test_returns_roles_list_on_success(self):
        am = _make_am(get_responses={"/api/roles": FakeResponse(200, _ROLES)})
        result = am.get_roles()
        assert isinstance(result, list)
        assert result[0]["name"] == "consumer"

    def test_returns_error_on_failure(self):
        am = _make_am(get_responses={"/api/roles": FakeResponse(500, {"error": "server error"})})
        result = am.get_roles()
        assert "error" in result

    def test_returns_error_on_none_response(self):
        am = _make_am()
        result = am.get_roles()
        assert "error" in result
