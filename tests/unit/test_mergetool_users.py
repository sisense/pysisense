"""Unit tests for pysisense.mergetool.users.UsersMergeMixin."""

from helpers import FakeApiClient, FakeLogger, FakeResponse

from pysisense.mergetool import MergeTool

_SOURCE_USER_JOHN = {
    "_id": "src_user_1",
    "email": "john@example.com",
    "userName": "john",
    "firstName": "John",
    "lastName": "Doe",
    "active": True,
    "tenantId": "tenant-system",
    "role": {"_id": "role_consumer", "name": "consumer"},
    "groups": [{"_id": "src_grp_1", "name": "Engineers"}],
}

_SOURCE_USER_SUPER = {
    "_id": "src_user_super",
    "email": "admin@example.com",
    "userName": "admin",
    "firstName": "Admin",
    "lastName": "",
    "active": True,
    "tenantId": "tenant-system",
    "role": {"_id": "role_super", "name": "super"},
    "groups": [],
}

_SOURCE_USER_OTHER_TENANT = {
    "_id": "src_user_2",
    "email": "acme@example.com",
    "userName": "acmeuser",
    "firstName": "Acme",
    "lastName": "User",
    "active": True,
    "tenantId": "tenant-acme",
    "role": {"_id": "role_consumer", "name": "consumer"},
    "groups": [],
}

_SOURCE_ROLES = [
    {"_id": "role_consumer", "name": "consumer"},
    {"_id": "role_contributor", "name": "contributor"},
    {"_id": "role_super", "name": "super"},
    {"_id": "role_custom_analyst", "name": "custom_analyst"},
    {"_id": "role_tenantadmin", "name": "tenantAdmin"},
]

_TARGET_ROLES_BASIC = [
    {"_id": "trole_consumer", "name": "consumer"},
    {"_id": "trole_contributor", "name": "contributor"},
    {"_id": "trole_super", "name": "super"},
]

_TARGET_ROLES_MULTITENANT = [
    {"_id": "trole_consumer", "name": "consumer"},
    {"_id": "trole_contributor", "name": "contributor"},
    {"_id": "trole_tenantadmin", "name": "tenantAdmin"},
]

_TARGET_ROLES_WINDOWS = [
    {"_id": "trole_consumer", "name": "consumer"},
    {"_id": "trole_admin", "name": "admin"},
]

_TARGET_GROUPS = [{"_id": "tgt_grp_1", "name": "Engineers"}]

_TENANTS = [
    {"_id": "tenant-system", "name": "system"},
    {"_id": "tenant-acme", "name": "Acme Corp"},
]

_EXISTING_TARGET_USER_JOHN = {
    "_id": "tgt_user_1",
    "email": "john@example.com",
    "userName": "john",
    "firstName": "John",
    "lastName": "Doe",
    "active": True,
    "role": {"_id": "trole_consumer", "name": "consumer"},
    "groups": [],
}


def _make_merge(
    src_get=None,
    src_post=None,
    src_delete=None,
    tgt_get=None,
    tgt_post=None,
    tgt_delete=None,
    target_operating_system="linux",
):
    """Build a MergeTool instance backed by separate FakeApiClient source/target."""
    src = FakeApiClient(get_responses=src_get, post_responses=src_post, delete_responses=src_delete, logger=FakeLogger())
    tgt = FakeApiClient(
        get_responses=tgt_get,
        post_responses=tgt_post,
        delete_responses=tgt_delete,
        logger=FakeLogger(),
        operating_system=target_operating_system,
    )
    return MergeTool(source_client=src, target_client=tgt)


def _basic_source(users_response, extra=None):
    """Source GET fixtures with the standard role list, so role mapping succeeds."""
    src_get = {
        "/api/v1/users": users_response,
        "/api/roles": FakeResponse(200, _SOURCE_ROLES),
    }
    if extra:
        src_get.update(extra)
    return src_get


def _basic_target(tgt_get_extra=None, tgt_post=None):
    tgt_get = {
        "/api/v1/users": FakeResponse(200, []),
        "/api/roles": FakeResponse(200, _TARGET_ROLES_BASIC),
        "/api/v1/groups": FakeResponse(200, _TARGET_GROUPS),
    }
    if tgt_get_extra:
        tgt_get.update(tgt_get_extra)
    return tgt_get, (tgt_post or {"/api/v1/users/bulk": FakeResponse(201, [{"email": "john@example.com"}])})


# ---------------------------------------------------------------------------
# migrate_users — fetch failures
# ---------------------------------------------------------------------------


class TestMigrateUsersFetchFailures:
    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/users": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_source_fetch_none_response_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/users": None})
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["ok"] is False

    def test_target_roles_fetch_failure_returns_failed_summary(self):
        tgt_get = {
            "/api/v1/users": FakeResponse(200, []),
            "/api/roles": FakeResponse(500, {"error": "boom"}),
            "/api/v1/groups": FakeResponse(200, []),
        }
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])), tgt_get=tgt_get)
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["ok"] is False
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# migrate_users — email filtering
# ---------------------------------------------------------------------------


class TestMigrateUsersEmailFiltering:
    def test_missing_email_is_reported_as_failed(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["john@example.com", "ghost@example.com"])
        failed_emails = {f["email"] for f in result["failed"]}
        assert "ghost@example.com" in failed_emails
        assert result["source_count"] == 1

    def test_no_matching_users_is_noop(self):
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])))
        result = merge.migrate_users(user_emails=["ghost@example.com"])
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_none_emails_migrates_everything_on_source(self):
        tgt_get, tgt_post = _basic_target(tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "john@example.com"}, {"email": "admin@example.com"}])})
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN, _SOURCE_USER_SUPER])),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_users(user_emails=None)
        assert result["source_count"] == 2
        assert result["succeeded_count"] == 2


# ---------------------------------------------------------------------------
# migrate_users — role resolution
# ---------------------------------------------------------------------------


class TestMigrateUsersRoleResolution:
    def test_unmapped_role_is_reported_as_failed(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])),
            tgt_get={**tgt_get, "/api/roles": FakeResponse(200, [])},
            tgt_post=tgt_post,
        )
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["failed"][0]["email"] == "john@example.com"
        assert "No matching target role" in result["failed"][0]["reason"]

    def test_multitenant_target_maps_super_to_tenantadmin(self):
        tgt_get, tgt_post = _basic_target(
            tgt_get_extra={"/api/roles": FakeResponse(200, _TARGET_ROLES_MULTITENANT)},
            tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "admin@example.com"}])},
        )
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_SUPER])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["admin@example.com"])
        assert result["succeeded"] == [{"email": "admin@example.com"}]

    def test_windows_target_maps_tenantadmin_back_to_admin(self):
        tenantadmin_user = {**_SOURCE_USER_JOHN, "email": "tadmin@example.com", "role": {"_id": "role_tenantadmin", "name": "tenantAdmin"}}
        tgt_get, tgt_post = _basic_target(
            tgt_get_extra={"/api/roles": FakeResponse(200, _TARGET_ROLES_WINDOWS)},
            tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "tadmin@example.com"}])},
        )
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [tenantadmin_user])),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
            target_operating_system="windows",
        )
        result = merge.migrate_users(user_emails=["tadmin@example.com"])
        assert result["succeeded"] == [{"email": "tadmin@example.com"}]

    def test_ignore_custom_roles_strips_prefix_for_matching(self):
        analyst_user = {**_SOURCE_USER_JOHN, "email": "analyst@example.com", "role": {"_id": "role_custom_analyst", "name": "custom_analyst"}}
        tgt_get, tgt_post = _basic_target(
            tgt_get_extra={"/api/roles": FakeResponse(200, [{"_id": "trole_analyst", "name": "analyst"}])},
            tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "analyst@example.com"}])},
        )
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [analyst_user])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["analyst@example.com"], ignore_custom_roles=True)
        assert result["succeeded"] == [{"email": "analyst@example.com"}]

    def test_ignore_custom_roles_matches_prefixed_target_role(self):
        analyst_user = {**_SOURCE_USER_JOHN, "email": "analyst@example.com", "role": {"_id": "role_custom_analyst", "name": "custom_analyst"}}
        tgt_get, tgt_post = _basic_target(
            tgt_get_extra={"/api/roles": FakeResponse(200, [{"_id": "trole_analyst", "name": "custom_analyst"}])},
            tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "analyst@example.com"}])},
        )
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [analyst_user])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["analyst@example.com"], ignore_custom_roles=True)
        assert result["succeeded"] == [{"email": "analyst@example.com"}]


# ---------------------------------------------------------------------------
# migrate_users — conflict handling
# ---------------------------------------------------------------------------


class TestMigrateUsersConflictHandling:
    def test_skip_leaves_existing_user_unchanged(self):
        tgt_get, tgt_post = _basic_target(tgt_get_extra={"/api/v1/users": FakeResponse(200, [_EXISTING_TARGET_USER_JOHN])})
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["john@example.com"], action="skip")
        assert result["skipped"] == [{"email": "john@example.com", "reason": "Already exists on target."}]
        assert result["ok"] is True
        assert result["status"] == "success"

    def test_overwrite_deletes_existing_then_recreates(self):
        tgt_get, tgt_post = _basic_target(tgt_get_extra={"/api/v1/users": FakeResponse(200, [_EXISTING_TARGET_USER_JOHN])})
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
            tgt_delete={f"/api/v1/users/{_EXISTING_TARGET_USER_JOHN['_id']}": FakeResponse(204, {})},
        )
        result = merge.migrate_users(user_emails=["john@example.com"], action="overwrite")
        assert result["succeeded"] == [{"email": "john@example.com"}]

    def test_overwrite_delete_failure_still_proceeds_with_create(self):
        tgt_get, tgt_post = _basic_target(tgt_get_extra={"/api/v1/users": FakeResponse(200, [_EXISTING_TARGET_USER_JOHN])})
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
            tgt_delete={f"/api/v1/users/{_EXISTING_TARGET_USER_JOHN['_id']}": FakeResponse(500, {"error": "cannot delete"})},
        )
        result = merge.migrate_users(user_emails=["john@example.com"], action="overwrite")
        assert result["succeeded"] == [{"email": "john@example.com"}]

    def test_duplicate_creates_regardless_of_conflict(self):
        tgt_get, tgt_post = _basic_target(tgt_get_extra={"/api/v1/users": FakeResponse(200, [_EXISTING_TARGET_USER_JOHN])})
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["john@example.com"], action="duplicate")
        assert result["succeeded"] == [{"email": "john@example.com"}]
        assert result["skipped"] == []


# ---------------------------------------------------------------------------
# migrate_users — bulk create response handling
# ---------------------------------------------------------------------------


class TestMigrateUsersBulkCreate:
    def test_bulk_create_success_marks_succeeded(self):
        tgt_get, tgt_post = _basic_target()
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])), tgt_get=tgt_get, tgt_post=tgt_post)
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["ok"] is True
        assert result["succeeded"] == [{"email": "john@example.com"}]

    def test_bulk_create_failure_marks_all_failed(self):
        tgt_get, _ = _basic_target()
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])),
            tgt_get=tgt_get,
            tgt_post={"/api/v1/users/bulk": FakeResponse(400, {"error": "bad request"})},
        )
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["ok"] is False
        assert result["failed"][0]["email"] == "john@example.com"

    def test_target_user_fetch_failure_treated_as_no_conflicts(self):
        merge = _make_merge(
            src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_JOHN])),
            tgt_get={
                "/api/v1/users": FakeResponse(500, {"error": "boom"}),
                "/api/roles": FakeResponse(200, _TARGET_ROLES_BASIC),
                "/api/v1/groups": FakeResponse(200, _TARGET_GROUPS),
            },
            tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "john@example.com"}])},
        )
        result = merge.migrate_users(user_emails=["john@example.com"])
        assert result["succeeded"] == [{"email": "john@example.com"}]


# ---------------------------------------------------------------------------
# migrate_all_users — filtering
# ---------------------------------------------------------------------------


class TestMigrateAllUsers:
    def test_excludes_super_role_and_other_tenants(self):
        tgt_get, tgt_post = _basic_target(tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "john@example.com"}])})
        merge = _make_merge(
            src_get=_basic_source(
                FakeResponse(200, [_SOURCE_USER_JOHN, _SOURCE_USER_SUPER, _SOURCE_USER_OTHER_TENANT]),
                extra={"/api/v1/tenants": FakeResponse(200, _TENANTS)},
            ),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_all_users()
        assert result["source_count"] == 1
        assert result["succeeded"] == [{"email": "john@example.com"}]

    def test_no_tenants_endpoint_skips_tenant_filtering(self):
        tgt_get, tgt_post = _basic_target(tgt_post={"/api/v1/users/bulk": FakeResponse(201, [{"email": "john@example.com"}, {"email": "acme@example.com"}])})
        merge = _make_merge(
            src_get=_basic_source(
                FakeResponse(200, [_SOURCE_USER_JOHN, _SOURCE_USER_OTHER_TENANT]),
                extra={"/api/v1/tenants": FakeResponse(404, {"error": "not found"})},
            ),
            tgt_get=tgt_get,
            tgt_post=tgt_post,
        )
        result = merge.migrate_all_users()
        assert result["source_count"] == 2

    def test_no_source_users_is_noop(self):
        merge = _make_merge(src_get={"/api/v1/users": FakeResponse(200, [])})
        result = merge.migrate_all_users()
        assert result["ok"] is True
        assert result["status"] == "noop"

    def test_source_fetch_failure_returns_failed_summary(self):
        merge = _make_merge(src_get={"/api/v1/users": FakeResponse(500, {"error": "boom"})})
        result = merge.migrate_all_users()
        assert result["ok"] is False
        assert result["status"] == "failed"

    def test_all_excluded_is_noop(self):
        merge = _make_merge(src_get=_basic_source(FakeResponse(200, [_SOURCE_USER_SUPER]), extra={"/api/v1/tenants": FakeResponse(200, _TENANTS)}))
        result = merge.migrate_all_users()
        assert result["ok"] is True
        assert result["status"] == "noop"
