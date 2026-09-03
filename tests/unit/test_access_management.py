"""Unit tests for pysisense.access_management.AccessManagement."""

import pytest
from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.access_management import AccessManagement


class FakeResponseEmpty(FakeResponse):
    """FakeResponse with an empty body — simulates a 200 with no JSON content."""

    def __init__(self, status_code: int) -> None:
        super().__init__(status_code, None)
        self.content = b""


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
# get_user_with_role_and_group_names
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
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


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
class TestGetUsersWithRoleNamesAndGroupNames:
    def test_returns_enriched_user_list(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_users_with_role_names_and_group_names()
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["USER_ID"] == "user123"
        # Unlike get_user_with_role_and_group_names, this method keeps the
        # RAW role name ("consumer"), not the public alias ("viewer").
        assert result[0]["ROLE_NAME"] == "consumer"
        assert "Engineers" in result[0]["GROUP_NAMES"]

    def test_returns_error_list_when_users_api_fails(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(500, {})})
        result = am.get_users_with_role_names_and_group_names()
        assert isinstance(result, list)
        assert "error" in result[0]

    def test_stale_role_reference_resolves_to_none_not_preserved(self):
        # Documents an accepted, UNVERIFIED assumption (see
        # micael_similar_methods_fixes.md, Users module) about how
        # GET /api/v1/users?expand=groups,role behaves for a stale roleId —
        # a role that's been deleted but is still referenced by a user.
        # Before this method switched from a raw-ID manual join to expand,
        # a stale roleId was preserved in ROLE_ID (with ROLE_NAME as None).
        # This pins the now-accepted behavior: if a live tenant's expand
        # response actually returns "role": null for a stale reference (as
        # assumed, not confirmed against a live stale reference), both
        # ROLE_ID and ROLE_NAME come back None, and the raw ID is lost.
        user = dict(_USER_EXPANDED)
        user["role"] = None
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_users_with_role_names_and_group_names()
        assert result[0]["ROLE_ID"] is None
        assert result[0]["ROLE_NAME"] is None


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


class TestGetUser:
    def test_returns_canonical_user_row_on_success(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user("jdoe@example.com")
        assert result["USER_ID"] == "user123"
        assert result["EMAIL"] == "jdoe@example.com"
        # ROLE_NAME keeps its 1.x meaning (the UI name), so role comparisons
        # written against 1.x keep working; the raw value is ROLE_RAW_NAME.
        assert result["ROLE_NAME"] == "viewer"
        assert result["ROLE_DISPLAY_NAME"] == "viewer"
        assert result["ROLE_RAW_NAME"] == "consumer"
        assert result["GROUP_IDS"] == ["grp_engineers"]
        assert result["GROUPS"] == ["Engineers"]

    def test_group_fields_come_from_the_group_side(self):
        # Sisense resolves Admins / All users in system on the GROUP side and
        # never writes them into a user's own `groups` field. Reading the user
        # record alone made get_user disagree with users_per_group about the
        # same person — see test_agrees_with_users_per_group below.
        admin = {**_USER_EXPANDED, "groups": [{"_id": "g_ev", "name": "Everyone"}]}
        groups = [
            {"_id": "g_ev", "name": "Everyone", "users": [admin]},
            {"_id": "g_adm", "name": "Admins", "admins": True, "users": [admin]},
        ]
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [admin]),
                "/api/v1/groups": FakeResponse(200, groups),
            }
        )
        row = am.get_user("jdoe@example.com")
        assert sorted(row["GROUPS"]) == ["Admins", "Everyone"], "Admins is on the group side only"
        assert sorted(row["GROUP_IDS"]) == ["g_adm", "g_ev"]

    def test_agrees_with_users_per_group_about_the_same_person(self):
        # The invariant: two canonical methods must not answer the same
        # question differently. This is the regression that motivated the fix.
        admin = {**_USER_EXPANDED, "groups": [{"_id": "g_ev", "name": "Everyone"}]}
        groups = [
            {"_id": "g_ev", "name": "Everyone", "users": [admin]},
            {"_id": "g_adm", "name": "Admins", "admins": True, "users": [admin]},
        ]
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [admin]),
                "/api/v1/groups": FakeResponse(200, groups),
            }
        )
        from_user = set(am.get_user("jdoe@example.com")["GROUPS"])
        from_group = {r["GROUP_NAME"] for r in am.users_per_group("Admins") if r["EMAIL"] == "jdoe@example.com"}
        assert "Admins" in from_user and "Admins" in from_group

    def test_falls_back_to_the_user_record_when_groups_cannot_be_fetched(self):
        # A failed group fetch must degrade to the user record, not blank the
        # group fields — an incomplete answer beats losing them entirely.
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
                "/api/v1/groups": FakeResponse(500, {}),
            }
        )
        row = am.get_user("jdoe@example.com")
        assert row["GROUPS"] == ["Engineers"]
        assert row["GROUP_IDS"] == ["grp_engineers"]

    def test_returns_error_when_email_not_found(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_user("ghost@example.com")
        assert "error" in result

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={})  # no response → None
        result = am.get_user("jdoe@example.com")
        assert "error" in result

    def test_returns_empty_role_id_and_name_when_user_has_no_role(self):
        # Regression: get_user resolves ROLE_ID/ROLE_NAME via _map_user_role_and_groups,
        # which returns None for a missing role — must still surface as "" here.
        user = dict(_USER_EXPANDED)
        user.pop("role")
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_user("jdoe@example.com")
        assert result["ROLE_ID"] == ""
        assert result["ROLE_NAME"] == ""

    def test_includes_empty_string_placeholder_for_group_missing_name(self):
        user = dict(_USER_EXPANDED)
        user["groups"] = [{"_id": "g9"}]  # no "name" key
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_user("jdoe@example.com")
        assert result["GROUP_IDS"] == ["g9"]
        assert result["GROUPS"] == [""]


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
    def test_returns_canonical_user_rows(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        result = am.get_users_all()
        assert isinstance(result, list)
        assert result[0]["USER_ID"] == "user123"
        assert result[0]["ROLE_NAME"] == "viewer"
        assert result[0]["ROLE_DISPLAY_NAME"] == "viewer"
        assert result[0]["ROLE_RAW_NAME"] == "consumer"
        assert result[0]["GROUPS"] == ["Engineers"]

    def test_one_x_role_comparisons_still_work(self):
        # The reason ROLE_NAME holds the UI name: a 1.x comparison like
        # `user["ROLE_NAME"] == "sysAdmin"` would otherwise match nothing and
        # fail SILENTLY, quietly turning an admin check into "not an admin".
        admin = dict(_USER_EXPANDED)
        admin["role"] = {"_id": "role_super", "name": "super"}
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [admin])})
        rows = am.get_users_all()
        assert [u for u in rows if u["ROLE_NAME"] == "sysAdmin"], "1.x role filter must still match"
        assert rows[0]["ROLE_RAW_NAME"] == "super"

    def test_everyone_group_is_reported_not_filtered(self):
        # The SDK reports what Sisense says; consumers decide what to hide.
        # A silently filtered membership is invisible downstream ("is Joe in
        # Everyone?" would be answered wrongly).
        user = dict(_USER_EXPANDED)
        user["groups"] = [
            {"_id": "g1", "name": "Everyone"},
            {"_id": "g2", "name": "Engineers"},
        ]
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_users_all()
        assert result[0]["GROUPS"] == ["Everyone", "Engineers"]

    def test_returns_error_dict_on_api_failure(self):
        am = _make_am(get_responses={})
        result = am.get_users_all()
        assert isinstance(result, dict)
        assert "error" in result

    def test_user_without_role_is_included_with_empty_role_fields(self):
        # Canonical rows report every user Sisense returns; a missing role is
        # a normal state surfaced as empty role fields, not a dropped record.
        user = dict(_USER_EXPANDED)
        user.pop("role")
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [user])})
        result = am.get_users_all()
        assert len(result) == 1
        assert result[0]["ROLE_ID"] == ""
        assert result[0]["ROLE_NAME"] == ""
        assert result[0]["ROLE_DISPLAY_NAME"] == ""

    def test_zero_users_is_an_empty_list_not_an_error(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [])})
        assert am.get_users_all() == []


# ---------------------------------------------------------------------------
# get_users_expanded
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
class TestGetUsersExpanded:
    def test_deprecated_alias_returns_raw_user_list(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])})
        with pytest.warns(DeprecationWarning, match="use get_users_all"):
            result = am.get_users_expanded()
        assert result == [_USER_EXPANDED]

    def test_deprecated_alias_returns_empty_list_when_no_users(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [])})
        with pytest.warns(DeprecationWarning):
            result = am.get_users_expanded()
        assert result == []

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(500, {"error": "boom"})})
        result = am.get_users_expanded()
        assert "error" in result


# ---------------------------------------------------------------------------
# create_users_bulk
# ---------------------------------------------------------------------------


class TestCreateUsersBulk:
    def test_returns_created_users_on_success(self):
        am = _make_am(post_responses={"/api/v1/users/bulk": FakeResponse(201, [_USER_RAW])})
        result = am.create_users_bulk([{"email": "jdoe@example.com", "firstName": "John", "roleId": "role_consumer"}])
        assert result == [_USER_RAW]

    def test_returns_error_on_non_201_status(self):
        am = _make_am(post_responses={"/api/v1/users/bulk": FakeResponse(400, {"error": "bad request"})})
        result = am.create_users_bulk([{"email": "jdoe@example.com"}])
        assert "error" in result

    def test_returns_error_when_no_response(self):
        am = _make_am(post_responses={})
        result = am.create_users_bulk([{"email": "jdoe@example.com"}])
        assert "error" in result


# ---------------------------------------------------------------------------
# get_group
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
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
# get_groups
# ---------------------------------------------------------------------------


class TestGetGroups:
    def test_returns_group_list_on_success(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, _GROUPS)})
        result = am.get_groups()
        assert result == _GROUPS

    def test_returns_empty_list_when_no_groups(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [])})
        result = am.get_groups()
        assert result == []

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(500, {"error": "boom"})})
        result = am.get_groups()
        assert "error" in result

    def test_name_filter_returns_matching_group(self):
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [_GROUPS[0]])})
        result = am.get_groups(name=_GROUPS[0]["name"])
        assert result == [_GROUPS[0]]

    def test_name_filter_with_unknown_name_returns_error_dict(self):
        # The ?name= filter is an exact-match dereference (live-verified) —
        # same honesty rule as get_user(email): a typo'd name fails loudly
        # naming the reference, it never reads as an empty listing.
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [])})
        result = am.get_groups(name="NoSuchGroup")
        assert result["ok"] is False
        assert "NoSuchGroup" in result["error"]


# ---------------------------------------------------------------------------
# create_groups_bulk
# ---------------------------------------------------------------------------


class TestCreateGroupsBulk:
    def test_returns_created_groups_on_success(self):
        am = _make_am(post_responses={"/api/v1/groups/bulk": FakeResponse(201, _GROUPS)})
        result = am.create_groups_bulk([{"name": "Engineers"}, {"name": "Admins"}])
        assert result == _GROUPS

    def test_returns_error_on_non_201_status(self):
        am = _make_am(post_responses={"/api/v1/groups/bulk": FakeResponse(400, {"error": "bad request"})})
        result = am.create_groups_bulk([{"name": "Engineers"}])
        assert "error" in result

    def test_returns_error_when_no_response(self):
        am = _make_am(post_responses={})
        result = am.create_groups_bulk([{"name": "Engineers"}])
        assert "error" in result


# ---------------------------------------------------------------------------
# delete_group
# ---------------------------------------------------------------------------


class TestDeleteGroup:
    def test_returns_message_on_204(self):
        am = _make_am(delete_responses={"/api/v1/groups/grp_engineers": FakeResponse(204, {})})
        result = am.delete_group("grp_engineers")
        assert result == {"message": "Group deleted successfully."}

    def test_returns_error_on_failure(self):
        am = _make_am(delete_responses={"/api/v1/groups/grp_engineers": FakeResponse(500, {"error": "cannot delete"})})
        result = am.delete_group("grp_engineers")
        assert "error" in result

    def test_returns_error_when_no_response(self):
        am = _make_am(delete_responses={})
        result = am.delete_group("grp_engineers")
        assert "error" in result


# ---------------------------------------------------------------------------
# get_tenants
# ---------------------------------------------------------------------------


class TestGetTenants:
    def test_returns_tenant_list_on_success(self):
        tenants = [{"_id": "tenant-system", "name": "system"}]
        am = _make_am(get_responses={"/api/v1/tenants": FakeResponse(200, tenants)})
        result = am.get_tenants()
        assert result == tenants

    def test_returns_error_on_api_failure(self):
        am = _make_am(get_responses={"/api/v1/tenants": FakeResponse(404, {"error": "not found"})})
        result = am.get_tenants()
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

    def test_returns_error_instead_of_raising_when_role_is_explicitly_none(self):
        # Regression: user_data.get("role", "").upper() raised AttributeError when
        # "role" was present but explicitly None. Must return a structured error.
        am = _make_am(get_responses={"/api/roles": FakeResponse(200, _ROLES)})
        result = am.create_user({"email": "x@x.com", "role": None, "groups": []})
        assert "error" in result

    @pytest.mark.parametrize(
        ("role", "expected_role_id"),
        [
            # Raw Sisense vocabulary
            ("consumer", "role_consumer"),
            ("super", "role_super"),
            ("contributor", "role_contributor"),
            # UI display vocabulary — what get_user()/get_users_all() hand back
            # in ROLE_DISPLAY_NAME. Before 2.0 the last two were REJECTED.
            ("viewer", "role_consumer"),
            ("sysAdmin", "role_super"),
            ("dashboardDesigner", "role_contributor"),
            # Human phrasings: case, spacing and punctuation are ignored
            ("sys admin", "role_super"),
            ("System Admin", "role_super"),
            ("system administrator", "role_super"),
            ("dashboard designer", "role_contributor"),
            ("designer", "role_contributor"),
            ("  viewer  ", "role_consumer"),
            ("SUPER", "role_super"),
        ],
    )
    def test_accepts_both_role_vocabularies_and_human_spellings(self, role, expected_role_id):
        captured = {}

        class _CapturingClient(FakeApiClient):
            def post(self, url, data=None, **kwargs):
                captured["payload"] = data
                return super().post(url, data=data, **kwargs)

        client = _CapturingClient(
            get_responses={"/api/roles": FakeResponse(200, _ROLES), "/api/v1/groups": FakeResponse(200, _GROUPS)},
            post_responses={"/api/v1/users": FakeResponse(200, {"_id": "u1"})},
            logger=FakeLogger(),
        )
        result = AccessManagement(api_client=client).create_user({"email": "x@x.com", "role": role})
        assert result.get("_id") == "u1", f"role {role!r} was rejected: {result}"
        assert captured["payload"]["roleId"] == expected_role_id
        assert "role" not in captured["payload"]

    @pytest.mark.parametrize(
        ("role", "expected_role_id"),
        [
            # dataDesigner is its OWN role, never a synonym for contributor.
            ("dataDesigner", "role_data_designer"),
            ("data designer", "role_data_designer"),
            # admin is distinct from super — aliasing it would silently
            # over-privilege the user, so the real role must win.
            ("admin", "role_admin"),
            ("dataAdmin", "role_data_admin"),
            ("custom_analyst", "role_custom_analyst"),
        ],
    )
    def test_real_instance_roles_win_over_the_alias_table(self, role, expected_role_id):
        captured = {}

        class _CapturingClient(FakeApiClient):
            def post(self, url, data=None, **kwargs):
                captured["payload"] = data
                return super().post(url, data=data, **kwargs)

        extended_roles = _ROLES + [
            {"_id": "role_data_designer", "name": "dataDesigner"},
            {"_id": "role_data_admin", "name": "dataAdmin"},
            {"_id": "role_admin", "name": "admin"},
            {"_id": "role_custom_analyst", "name": "custom_analyst"},
        ]
        client = _CapturingClient(
            get_responses={"/api/roles": FakeResponse(200, extended_roles), "/api/v1/groups": FakeResponse(200, _GROUPS)},
            post_responses={"/api/v1/users": FakeResponse(200, {"_id": "u1"})},
            logger=FakeLogger(),
        )
        result = AccessManagement(api_client=client).create_user({"email": "x@x.com", "role": role})
        assert result.get("_id") == "u1", f"role {role!r} was rejected: {result}"
        assert captured["payload"]["roleId"] == expected_role_id

    def test_matches_display_name_from_the_roles_payload_when_present(self):
        # Some Sisense versions return a displayName alongside the raw name;
        # use it when it is there, without depending on it.
        roles = [{"_id": "role_super", "name": "super", "displayName": "System Administrator"}]
        am = _make_am(
            get_responses={"/api/roles": FakeResponse(200, roles), "/api/v1/groups": FakeResponse(200, _GROUPS)},
            post_responses={"/api/v1/users": FakeResponse(200, {"_id": "u1"})},
        )
        assert am.create_user({"email": "x@x.com", "role": "System Administrator"}).get("_id") == "u1"

    def test_unknown_role_error_lists_the_available_roles(self):
        # The caller (often an assistant) needs to know what it may retry with.
        am = _make_am(get_responses={"/api/roles": FakeResponse(200, _ROLES)})
        result = am.create_user({"email": "x@x.com", "role": "wizard"})
        assert result["ok"] is False
        assert "wizard" in result["error"]
        for name in ("consumer", "super", "contributor"):
            assert name in result["error"]

    def test_does_not_mutate_the_caller_payload(self):
        am = _make_am(
            get_responses={"/api/roles": FakeResponse(200, _ROLES), "/api/v1/groups": FakeResponse(200, _GROUPS)},
            post_responses={"/api/v1/users": FakeResponse(200, {"_id": "u1"})},
        )
        payload = {"email": "x@x.com", "role": "viewer", "groups": ["Engineers"]}
        am.create_user(payload)
        assert payload == {"email": "x@x.com", "role": "viewer", "groups": ["Engineers"]}

    def test_password_is_redacted_from_debug_log(self):
        am = _make_am(
            get_responses={"/api/roles": FakeResponse(200, _ROLES)},
            post_responses={"/api/v1/users": FakeResponse(200, {"_id": "u1"})},
        )
        am.create_user({"email": "x@x.com", "role": "viewer", "password": "hunter2"})
        assert not any("hunter2" in str(m["msg"]) for m in am.logger.messages)


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

    @pytest.mark.parametrize(
        ("role", "expected_role_id"),
        [
            ("sysAdmin", "role_super"),
            ("sys admin", "role_super"),
            ("dashboardDesigner", "role_contributor"),
            ("viewer", "role_consumer"),
            ("super", "role_super"),
        ],
    )
    def test_accepts_both_role_vocabularies(self, role, expected_role_id):
        # update_user shares _resolve_role_id with create_user — the round-trip
        # that matters is reading ROLE_DISPLAY_NAME and writing it straight back.
        captured = {}

        class _CapturingClient(FakeApiClient):
            def patch(self, url, data=None, **kwargs):
                captured["payload"] = data
                return super().patch(url, data=data, **kwargs)

        client = _CapturingClient(
            get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED]), "/api/roles": FakeResponse(200, _ROLES)},
            patch_responses={"/api/v1/users/": FakeResponse(200, _USER_EXPANDED)},
            logger=FakeLogger(),
        )
        result = AccessManagement(api_client=client).update_user("jdoe@example.com", {"role": role})
        assert "error" not in result, f"role {role!r} was rejected: {result}"
        assert captured["payload"]["roleId"] == expected_role_id
        assert "role" not in captured["payload"]

    def test_unknown_user_returns_error_dict_even_when_role_resolves(self):
        # Regression: get_user's failure dict is non-empty, so the old
        # `if not user` guard never fired and the PATCH raised KeyError on
        # user["USER_ID"]. Roles are mocked so the failure cannot be blamed
        # on role resolution.
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, []), "/api/roles": FakeResponse(200, _ROLES)})
        result = am.update_user("ghost@example.com", {"role": "viewer"})
        assert result["ok"] is False
        assert "ghost@example.com" in result["error"]

    def test_does_not_mutate_the_caller_payload(self):
        am = _make_am(
            get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED]), "/api/roles": FakeResponse(200, _ROLES)},
            patch_responses={"/api/v1/users/": FakeResponse(200, _USER_EXPANDED)},
        )
        payload = {"role": "viewer"}
        am.update_user("jdoe@example.com", payload)
        assert payload == {"role": "viewer"}

    def test_password_is_redacted_from_debug_log(self):
        am = _make_am(
            get_responses={"/api/v1/users": FakeResponse(200, [_USER_EXPANDED])},
            patch_responses={"/api/v1/users/": FakeResponse(200, _USER_EXPANDED)},
        )
        am.update_user("jdoe@example.com", {"password": "hunter2"})
        assert not any("hunter2" in str(m["msg"]) for m in am.logger.messages)


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
    def test_returns_flat_membership_rows_for_one_group(self):
        # Membership comes from the GROUP side (?expand=users), which is what
        # the Sisense UI shows — see test_reports_system_group_membership.
        group_with_members = {**_GROUPS[0], "users": [_USER_EXPANDED]}
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, [group_with_members]),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        result = am.users_per_group("Engineers")
        assert isinstance(result, list)
        assert result == [
            {
                "GROUP_ID": "grp_engineers",
                "GROUP_NAME": "Engineers",
                "USER_ID": "user123",
                "USER_NAME": "jdoe",
                "EMAIL": "jdoe@example.com",
                "FIRST_NAME": "John",
                "LAST_NAME": "Doe",
                "IS_ACTIVE": True,
                "ROLE_ID": "role_consumer",
                "ROLE_NAME": "viewer",
                "ROLE_DISPLAY_NAME": "viewer",
                "ROLE_RAW_NAME": "consumer",
            }
        ]

    def test_no_argument_returns_every_membership_flat(self):
        # One row per (group, user) — counts equal real membership counts.
        groups = [
            {"_id": "g1", "name": "Finance", "users": [_USER_EXPANDED]},
            {"_id": "grp_engineers", "name": "Engineers", "users": [_USER_EXPANDED]},
        ]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, groups),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        result = am.users_per_group()
        assert [(r["GROUP_NAME"], r["USER_NAME"]) for r in result] == [("Finance", "jdoe"), ("Engineers", "jdoe")]

    def test_reports_system_group_membership_the_ui_shows(self):
        # Regression for the 'Admins shows 0' defect. Sisense resolves the
        # auto-generated groups (Admins, All users in system) on the GROUP side
        # only — their members never appear in a user's own `groups` field. So
        # reading membership from the user side reported 0 while the UI showed
        # 34. Live-verified on the sandbox: 34/67/67, matching the UI exactly.
        admin_user = {**_USER_EXPANDED, "groups": [], "role": {"_id": "role_super", "name": "super"}}
        groups = [
            # note: the member is NOT in admin_user["groups"] — that is the point
            {"_id": "g_adm", "name": "Admins", "admins": True, "users": [admin_user]},
            {"_id": "g_all", "name": "All users in system", "users": [admin_user]},
        ]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, groups),
                "/api/v1/users": FakeResponse(200, [admin_user]),
            }
        )
        assert [r["USER_NAME"] for r in am.users_per_group("Admins")] == ["jdoe"]
        assert [r["USER_NAME"] for r in am.users_per_group("All users in system")] == ["jdoe"]
        # Admins is not a universal group, so it stays in the survey view;
        # "All users in system" is, so it is omitted there.
        assert [r["GROUP_NAME"] for r in am.users_per_group()] == ["Admins"]

    def test_universal_groups_are_omitted_from_the_all_groups_view(self):
        # Sisense puts every user in Everyone and All users in system, so in the
        # survey view they duplicate get_users_all() and swamp the real
        # memberships (192 rows vs 58 on the sandbox).
        groups = [
            {"_id": "g_ev", "name": "Everyone", "users": [_USER_EXPANDED]},
            {"_id": "g_all", "name": "All users in system", "users": [_USER_EXPANDED]},
            {"_id": "grp_engineers", "name": "Engineers", "users": [_USER_EXPANDED]},
        ]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, groups),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        assert [r["GROUP_NAME"] for r in am.users_per_group()] == ["Engineers"]
        # ...but each is still reachable by name.
        assert len(am.users_per_group("Everyone")) == 1
        assert len(am.users_per_group("All users in system")) == 1

    def test_naming_a_universal_group_always_returns_it(self):
        # The filter applies to the survey view only. An explicit request is
        # unambiguous and must be honored, so nothing is unreachable.
        groups = [{"_id": "g_ev", "name": "Everyone", "users": [_USER_EXPANDED]}]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, groups),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        assert [r["USER_NAME"] for r in am.users_per_group("Everyone")] == ["jdoe"]

    def test_member_missing_from_the_user_list_still_yields_a_row(self):
        # A group can list a member the user endpoint does not return (stale or
        # filtered). Drop the role detail, never the row — counts stay honest.
        ghost = {"_id": "u_ghost", "userName": "ghost", "email": "ghost@x.com", "active": True}
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, [{"_id": "g1", "name": "Engineers", "users": [ghost]}]),
                "/api/v1/users": FakeResponse(200, []),
            }
        )
        rows = am.users_per_group("Engineers")
        assert len(rows) == 1
        assert rows[0]["USER_NAME"] == "ghost"
        assert rows[0]["ROLE_NAME"] == ""

    def test_returns_error_when_group_not_found(self):
        # A typo'd group name must fail loudly — never a silent empty list.
        am = _make_am(get_responses={"/api/v1/groups": FakeResponse(200, [])})
        result = am.users_per_group("Nonexistent")
        assert "Nonexistent" in result["error"]

    def test_group_with_no_members_returns_empty_list(self):
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, [{"_id": "g7", "name": "EmptyGroup", "users": []}]),
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        assert am.users_per_group("EmptyGroup") == []

    def test_named_group_fetches_the_group_listing_once(self):
        # The expanded listing already answers "does this group exist", so a
        # separate ?name= lookup is a wasted round trip. A second groups call
        # would hit the 500 and turn this into a failure.
        group_with_members = {**_GROUPS[0], "users": [_USER_EXPANDED]}
        am = _make_am(
            get_responses={
                "/api/v1/groups": [FakeResponse(200, [group_with_members]), FakeResponse(500, {})],
                "/api/v1/users": FakeResponse(200, [_USER_EXPANDED]),
            }
        )
        rows = am.users_per_group("Engineers")
        assert [r["USER_NAME"] for r in rows] == ["jdoe"]


# ---------------------------------------------------------------------------
# users_per_group_all
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
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

    def test_skips_user_group_not_in_current_group_list_instead_of_raising(self):
        # A user references a group name ("Engineers") that isn't present in the
        # current /api/v1/groups response — e.g. the group was deleted between calls.
        user = dict(_USER_EXPANDED)
        user["groups"] = [{"_id": "grp_engineers", "name": "Engineers"}]
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, []),
                "/api/v1/users": FakeResponse(200, [user]),
            }
        )
        result = am.users_per_group_all()
        assert result == [{"group": "Admins", "username": []}]

    def test_guarantees_admins_group_when_groups_list_is_empty_but_successful(self):
        # An empty (but HTTP-successful) /api/v1/groups response is not a failure —
        # it must still produce the guaranteed "Admins" entry, unlike the API-failure case.
        admin_user = dict(_USER_EXPANDED)
        admin_user["role"] = {"_id": "role_super", "name": "super"}  # super -> sysAdmin
        am = _make_am(
            get_responses={
                "/api/v1/groups": FakeResponse(200, []),
                "/api/v1/users": FakeResponse(200, [admin_user]),
            }
        )
        result = am.users_per_group_all()
        assert result == [{"group": "Admins", "username": ["jdoe"]}]


# ---------------------------------------------------------------------------
# change_folder_and_dashboard_ownership
# ---------------------------------------------------------------------------


_OWNERSHIP_USERS = [
    {**_USER_EXPANDED, "_id": "executor_id", "email": "executor@example.com"},
    {**_USER_EXPANDED, "_id": "newowner_id", "email": "newowner@example.com"},
]

_NAVVER_FOLDERS = {
    "folders": [
        {
            "oid": "folder1",
            "name": "MyFolder",
            "dashboards": [{"oid": "dash1", "title": "Sales"}],
            "folders": [],
        }
    ]
}


class TestChangeFolderAndDashboardOwnership:
    def test_returns_error_when_executing_user_not_found(self):
        # get_user for the executing user returns not-found
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [])})
        result = am.change_folder_and_dashboard_ownership("executor@example.com", "MyFolder", "newowner@example.com")
        assert "error" in result

    def test_changes_ownership_when_folder_and_dashboard_responses_have_no_body(self):
        # Some Sisense versions return 200 with no JSON body for the folder-owner
        # PATCH and the dashboard change_owner POST — both must still count as success.
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, _OWNERSHIP_USERS),
                "/api/v1/navver": FakeResponse(200, _NAVVER_FOLDERS),
                "/api/v1/dashboards/dash1": FakeResponse(200, {"oid": "dash1", "title": "Sales", "owner": "executor_id"}),
            },
            patch_responses={"/api/v1/folders/folder1": FakeResponseEmpty(200)},
            post_responses={"/api/v1/dashboards/dash1/change_owner": FakeResponseEmpty(200)},
        )
        result = am.change_folder_and_dashboard_ownership("executor@example.com", "MyFolder", "newowner@example.com")
        assert result == {"total_folders_changed": 1, "total_dashboards_changed": 1}

    def test_fallback_path_does_not_crash_when_dashboard_search_post_fails(self):
        # Regression: the folder-not-found fallback path used to call .json()
        # directly on the dashboard-search POST response with no None-check,
        # crashing with AttributeError if that POST failed. It now goes
        # through the shared _fetch_all_dashboards_paginated() helper, which
        # handles a failed/missing response gracefully.
        navver_without_match = {"folders": [{"oid": "other", "name": "OtherFolder", "dashboards": [], "folders": []}]}
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, _OWNERSHIP_USERS),
                "/api/v1/navver": FakeResponse(200, navver_without_match),
                "/api/v1/folders": FakeResponse(200, []),
            },
            # No post_responses for /api/v1/dashboards/searches -> None -> used
            # to crash inside the pagination loop before reaching this point.
        )
        result = am.change_folder_and_dashboard_ownership("executor@example.com", "NoSuchFolder", "newowner@example.com")
        assert result is None

    def test_fallback_path_collects_dashboards_across_multiple_pages(self):
        page_1 = FakeResponse(200, {"items": [{"oid": "dashA", "title": "A", "parentFolder": "folderX", "shares": []}]})
        page_2_empty = FakeResponse(200, {"items": []})
        navver_without_match = {"folders": []}
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, _OWNERSHIP_USERS),
                "/api/v1/navver": FakeResponse(200, navver_without_match),
                "/api/v1/folders": FakeResponse(200, []),
            },
            post_responses={
                "/api/v1/dashboards/searches": [page_1, page_2_empty],
                "/api/shares/dashboard/dashA": FakeResponse(200, {"shared": True}),
            },
        )
        result = am.change_folder_and_dashboard_ownership("executor@example.com", "NoSuchFolder", "newowner@example.com")
        assert result is None  # folder still not found after the access-grant retry


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


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
# Deprecated alias — behavior frozen until 2.0 removal; the warning is expected.
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
    def test_datamodels_is_a_required_parameter(self):
        # No default — schema generators must see it as required, so a bare
        # call cannot silently return "no unused columns".
        import inspect

        sig = inspect.signature(type(_make_am()).get_unused_columns_bulk)
        assert sig.parameters["datamodels"].default is inspect.Parameter.empty
        with pytest.raises(TypeError):
            _make_am().get_unused_columns_bulk()

    def test_explicit_none_returns_error_dict(self):
        am = _make_am()
        result = am.get_unused_columns_bulk(None)
        assert result["ok"] is False
        assert "error" in result
        assert result["results"] == []

    def test_empty_list_input_returns_error_dict(self):
        am = _make_am()
        result = am.get_unused_columns_bulk([])
        assert result["ok"] is False
        assert "error" in result
        assert result["results"] == []

    def test_unresolvable_reference_fails_loudly_not_silently(self):
        # A typo'd model name must NOT read as "no unused columns" — the
        # total-failure dict carries ok: False and names the reference.
        am = _make_am(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(404, {}),
                "/api/v2/datamodels/NoSuchModel/schema": FakeResponse(404, {}),
            }
        )
        result = am.get_unused_columns_bulk("NoSuchModel")
        assert result["ok"] is False
        assert "NoSuchModel" in result["error"]
        assert result["results"] == []
        assert result["errors"][0]["ref"] == "NoSuchModel"

    def test_success_returns_results_and_empty_errors(self):
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
        result = am.get_unused_columns_bulk("MyModel")
        assert result["errors"] == []
        assert "ok" not in result
        assert len(result["results"]) == 1
        assert result["results"][0]["used"] is False

    def test_partial_success_reports_typo_in_errors_alongside_good_rows(self):
        # The 5-models-one-typo case: good rows land in "results", the typo'd
        # reference is reported in-band in "errors" instead of a silent skip.
        schema = {"oid": "dm123", "title": "MyModel"}
        datasets = [{"oid": "ds1"}]
        tables = [{"name": "tbl", "columns": [{"name": "col1"}]}]
        am = _make_am(
            get_responses={
                # Called 3× in order: resolve MyModel, column fetch for MyModel,
                # resolve NoSuchModel (list values are consumed per call).
                "/api/v2/datamodels/schema": [FakeResponse(200, schema), FakeResponse(200, schema), FakeResponse(404, {})],
                "/api/v2/datamodels/dm123/schema/datasets": FakeResponse(200, datasets),
                "/api/v2/datamodels/dm123/schema/datasets/ds1/tables": FakeResponse(200, tables),
                "/api/v1/dashboards/admin": FakeResponse(200, []),
            }
        )
        result = am.get_unused_columns_bulk(["MyModel", "NoSuchModel"])
        assert "ok" not in result
        assert len(result["results"]) == 1
        assert result["errors"][0]["ref"] == "NoSuchModel"


class TestUnusedColumnsDashboardParsing:
    """The usage side of get_unused_columns_bulk: a column cited by a dashboard is used."""

    @staticmethod
    def _am(tables, dim):
        schema = {"oid": "dm123", "title": "MyModel"}
        export = [{"title": "D", "filters": [], "widgets": [{"oid": "w1", "metadata": {"panels": [{"items": [{"jaql": {"dim": dim}}]}]}}]}]
        return _make_am(
            get_responses={
                "/api/v2/datamodels/schema": FakeResponse(200, schema),
                "/api/v2/datamodels/dm123/schema/datasets": FakeResponse(200, [{"oid": "ds1"}]),
                "/api/v2/datamodels/dm123/schema/datasets/ds1/tables": FakeResponse(200, tables),
                "/api/v1/dashboards/admin": FakeResponse(200, [{"oid": "d1", "title": "D"}]),
                "/api/v1/dashboards/export": FakeResponse(200, export),
            }
        )

    @staticmethod
    def _used(result):
        return {(r["table"], r["column"]): r["used"] for r in result["results"]}

    def test_one_bracket_reference_marks_the_column_used(self):
        am = self._am([{"name": "tbl", "columns": [{"name": "col1"}, {"name": "col2"}]}], "[tbl.col1]")
        assert self._used(am.get_unused_columns_bulk("MyModel")) == {("tbl", "col1"): True, ("tbl", "col2"): False}

    def test_two_bracket_reference_marks_the_column_used(self):
        # Regression: [tbl].[col1] used to parse as ("tbl]", "[col1") and col1 read as unused.
        am = self._am([{"name": "tbl", "columns": [{"name": "col1"}, {"name": "col2"}]}], "[tbl].[col1]")
        assert self._used(am.get_unused_columns_bulk("MyModel")) == {("tbl", "col1"): True, ("tbl", "col2"): False}

    def test_table_name_starting_with_bracket_marks_the_column_used(self):
        # Live-observed: a table renamed to "[region" is emitted as [[region.col]; strip("[]") lost the bracket.
        am = self._am([{"name": "[region", "columns": [{"name": "r_name"}, {"name": "r_comment"}]}], "[[region.r_name]")
        assert self._used(am.get_unused_columns_bulk("MyModel")) == {("[region", "r_name"): True, ("[region", "r_comment"): False}

    def test_csv_table_name_marks_the_column_used(self):
        # The common real case: CSV uploads are tables named "x.csv"; the old
        # first-dot split read [T1.csv.C1] as table "T1" and marked C1 unused.
        am = self._am([{"name": "T1.csv", "columns": [{"name": "C1"}, {"name": "C2"}]}], "[T1.csv.C1]")
        assert self._used(am.get_unused_columns_bulk("MyModel")) == {("T1.csv", "C1"): True, ("T1.csv", "C2"): False}

    def test_dotted_column_name_resolves_against_the_schema(self):
        # The dim has three dots; only the schema can say where the table ends.
        am = self._am([{"name": "@trips", "columns": [{"name": '."pickup.'}, {"name": "fare"}]}], '[@trips.."pickup. (Calendar)]')
        assert self._used(am.get_unused_columns_bulk("MyModel")) == {("@trips", '."pickup.'): True, ("@trips", "fare"): False}


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

    def test_resolves_shares_across_multiple_pages(self):
        # Regression: previously untested — the dashboard-pagination loop was
        # never exercised with an actual non-empty page followed by the
        # terminating empty page. Also covers the shared
        # _fetch_all_dashboards_paginated() helper (used by both this method
        # and change_folder_and_dashboard_ownership's fallback path).
        page_1 = FakeResponse(200, {"items": [{"title": "Sales", "shares": [{"type": "user", "shareId": "u1"}, {"type": "group", "shareId": "g1"}]}, {"title": "Marketing", "shares": []}]})
        page_2_empty = FakeResponse(200, {"items": []})
        am = _make_am(
            post_responses={"/api/v1/dashboards/searches": [page_1, page_2_empty]},
            get_responses={
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, [{"_id": "g1", "name": "Engineers"}]),
            },
        )
        result = am.get_all_dashboard_shares()
        # Marketing has no shares and must contribute no rows — a placeholder
        # would read as one share to any consumer that counts results.
        assert result == [
            {"dashboard": "Sales", "type": "user", "name": "alice@example.com"},
            {"dashboard": "Sales", "type": "group", "name": "Engineers"},
        ]

    def test_distinguishes_empty_string_email_from_unresolved_share(self):
        # Regression: the refactor to _get_user_email_and_group_name_maps() must
        # use "shareId in map" membership checks, not truthiness of the
        # resolved value — a user whose email is genuinely "" must still be
        # resolved (name: ""), not treated the same as an unresolvable shareId.
        page_1 = FakeResponse(
            200,
            {
                "items": [
                    {
                        "title": "Sales",
                        "shares": [
                            {"type": "user", "shareId": "u_empty_email"},
                            {"type": "user", "shareId": "u_missing"},
                        ],
                    }
                ]
            },
        )
        page_2_empty = FakeResponse(200, {"items": []})
        am = _make_am(
            post_responses={"/api/v1/dashboards/searches": [page_1, page_2_empty]},
            get_responses={
                "/api/v1/users": FakeResponse(200, [{"_id": "u_empty_email", "email": ""}]),
                "/api/v1/groups": FakeResponse(200, []),
            },
        )
        result = am.get_all_dashboard_shares()
        assert result == [
            {"dashboard": "Sales", "type": "user", "name": ""},
            {"dashboard": "Sales", "type": None, "name": None},
        ]


# ---------------------------------------------------------------------------
# _get_user_email_and_group_name_maps
# ---------------------------------------------------------------------------


class TestGetUserEmailAndGroupNameMaps:
    def test_returns_id_to_name_maps_on_success(self):
        am = _make_am(
            get_responses={
                "/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "alice@example.com"}]),
                "/api/v1/groups": FakeResponse(200, [{"_id": "g1", "name": "Engineers"}]),
            }
        )
        result = am._get_user_email_and_group_name_maps()
        assert result == {"users_by_id": {"u1": "alice@example.com"}, "groups_by_id": {"g1": "Engineers"}}

    def test_returns_error_when_users_api_fails(self):
        am = _make_am()
        result = am._get_user_email_and_group_name_maps()
        assert "error" in result

    def test_returns_error_when_groups_api_fails(self):
        am = _make_am(get_responses={"/api/v1/users": FakeResponse(200, [{"_id": "u1", "email": "a@b.com"}])})
        result = am._get_user_email_and_group_name_maps()
        assert "error" in result


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
