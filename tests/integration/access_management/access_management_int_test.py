import os

import pytest

from pysisense.access_management import AccessManagement
from pysisense.sisenseclient import SisenseClient

CONFIG_PATH = os.environ.get("PYSISENSE_CONFIG_PATH", "config.yaml")


def _make_client() -> SisenseClient:
    if not os.path.exists(CONFIG_PATH):
        pytest.skip(f"Config file '{CONFIG_PATH}' not found; skipping integration test.")
    return SisenseClient(config_file=CONFIG_PATH, debug=False)


@pytest.mark.integration
def test_get_users_all_returns_list() -> None:
    """get_users_all should return a non-empty list of user dicts."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_users_all()

    assert isinstance(result, list), f"Expected list, got: {result}"
    assert len(result) > 0, "Expected at least one user."

    first = result[0]
    # Canonical user row: uppercase keys, both role vocabularies, and GROUPS
    # (names, kept from 1.x) alongside the new GROUP_IDS.
    assert "EMAIL" in first
    assert "ROLE_NAME" in first
    assert "ROLE_DISPLAY_NAME" in first
    assert "ROLE_RAW_NAME" in first
    assert "GROUP_IDS" in first
    assert "GROUPS" in first


@pytest.mark.integration
def test_get_user_returns_user_dict() -> None:
    """get_user should return the correct user dict for a known email."""
    client = _make_client()
    am = AccessManagement(api_client=client)

    all_users = am.get_users_all()
    if not isinstance(all_users, list) or not all_users:
        pytest.skip("No users available.")

    email = next((u.get("EMAIL", "") for u in all_users if u.get("EMAIL")), "")
    if not email:
        pytest.skip("No users with an email address found.")

    result = am.get_user(email)

    assert isinstance(result, dict)
    assert "error" not in result
    assert result.get("EMAIL") == email


@pytest.mark.integration
def test_get_users_with_role_names_and_group_names_returns_enriched() -> None:
    """get_users_with_role_names_and_group_names should return enriched user dicts."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_users_with_role_names_and_group_names()

    assert isinstance(result, list)
    assert len(result) > 0

    first = result[0]
    assert "ROLE_NAME" in first
    assert "GROUP_NAMES" in first


@pytest.mark.integration
def test_users_per_group_all_returns_dict() -> None:
    """users_per_group_all should return a dict mapping group names to user lists."""
    am = AccessManagement(api_client=_make_client())
    result = am.users_per_group_all()

    assert isinstance(result, dict | list)


@pytest.mark.integration
def test_get_all_dashboard_shares_returns_list() -> None:
    """get_all_dashboard_shares should return a list of share records."""
    am = AccessManagement(api_client=_make_client())
    result = am.get_all_dashboard_shares()

    assert isinstance(result, list)


@pytest.mark.integration
def test_role_resolution_accepts_both_vocabularies_and_keeps_roles_distinct() -> None:
    """Role names resolve against the live instance's own /api/roles list.

    Read-only (only GETs /api/roles). Live-verified findings this encodes
    (2026-08 sandbox, L2025.x): /api/roles carries a displayName alongside the
    raw name ("super" / "Sys. Admin"), and the instance defines roles beyond
    the three with UI aliases — admin, tenantAdmin, dataDesigner, dataAdmin.

    The two assertions that matter are the negative ones: 'admin' must NOT
    resolve to 'super' (that would silently over-privilege a created user) and
    'data designer' must NOT resolve to 'contributor' (dataDesigner is its own
    role, not a synonym for Designer). Both are guaranteed by matching the
    instance's real roles before consulting the alias table.
    """
    am = AccessManagement(api_client=_make_client())

    roles_response = am.api_client.get("/api/roles")
    if roles_response is None or not roles_response.ok:
        pytest.skip("Could not fetch /api/roles.")
    name_by_id = {r["_id"]: r.get("name") for r in roles_response.json() if r.get("_id")}
    available = set(name_by_id.values())

    expectations = {
        "super": "super",
        "sysAdmin": "super",
        "sys admin": "super",
        "System Administrator": "super",
        "viewer": "consumer",
        "consumer": "consumer",
        "dashboardDesigner": "contributor",
        "designer": "contributor",
        "contributor": "contributor",
        # Roles that must resolve to themselves, never to a similar-sounding alias
        "admin": "admin",
        "dataDesigner": "dataDesigner",
        "data designer": "dataDesigner",
        "dataAdmin": "dataAdmin",
    }

    for given, expected_raw_name in expectations.items():
        if expected_raw_name not in available:
            continue  # instance does not define this role; nothing to assert
        resolved = am._resolve_role_id(given)
        assert not isinstance(resolved, dict), f"role {given!r} failed to resolve: {resolved}"
        assert name_by_id[resolved] == expected_raw_name, f"role {given!r} resolved to {name_by_id[resolved]!r}, expected {expected_raw_name!r}"

    unknown = am._resolve_role_id("definitely-not-a-role-xyz")
    assert isinstance(unknown, dict) and unknown["ok"] is False
    assert "Available roles" in unknown["error"], "the error must tell the caller what it can retry with"


@pytest.mark.integration
def test_users_per_group_matches_sisense_own_membership() -> None:
    """Membership must match what Sisense itself reports per group.

    Regression for the "Admins shows 0" defect: users_per_group used to derive
    membership from each user's own `groups` field, but Sisense resolves its
    auto-generated groups (Admins, All users in system) on the GROUP side only.
    Their members never appear user-side, so the SDK reported 0 while the
    Sisense UI showed the real count.

    This compares every group against `GET /api/v1/groups?expand=users`, which
    is the source the UI reads, rather than hardcoding tenant-specific numbers.
    """
    am = AccessManagement(api_client=_make_client())

    response = am.api_client.get("/api/v1/groups", params={"expand": "users"})
    if response is None or not response.ok:
        pytest.skip("Could not fetch expanded groups.")
    expected = {g["name"]: len(g.get("users") or []) for g in response.json() if g.get("name")}
    if not expected:
        pytest.skip("No groups on this instance.")

    universal = {"Everyone", "All users in system"}
    rows = am.users_per_group()
    assert isinstance(rows, list), f"expected rows, got: {rows}"

    actual: dict[str, int] = {}
    for row in rows:
        actual[row["GROUP_NAME"]] = actual.get(row["GROUP_NAME"], 0) + 1

    for name, count in expected.items():
        if name in universal:
            continue  # omitted from the all-groups view; checked by name below
        assert actual.get(name, 0) == count, f"group '{name}': SDK reported {actual.get(name, 0)}, Sisense reports {count}"

    assert len(rows) == sum(c for n, c in expected.items() if n not in universal)
    assert set(actual).isdisjoint(universal), "universal groups must be omitted from the all-groups view"

    # ...but each remains reachable by name, and must still match Sisense.
    for name in universal:
        if expected.get(name):
            assert len(am.users_per_group(name)) == expected[name], f"named lookup of '{name}' disagrees with Sisense"

    # The auto-generated groups are the ones that regressed; assert them by
    # name when present, so a user-side regression fails loudly here.
    for system_group in ("Admins", "All users in system"):
        if expected.get(system_group):
            named = am.users_per_group(system_group)
            assert len(named) == expected[system_group], f"'{system_group}' filtered lookup disagrees with Sisense"


@pytest.mark.integration
def test_group_membership_is_consistent_across_canonical_methods() -> None:
    """Every canonical method must answer "which groups is X in?" identically.

    This is the invariant the 2.0 work exists to protect, and the one that
    caught us out: 2.0.0 moved users_per_group to group-side membership but
    left get_user/get_users_all on the user record, so they disagreed about
    the same person (get_user said ['Everyone'] for a user users_per_group
    listed under Admins). Unit tests with fakes cannot catch that — only a
    real tenant, where Sisense's derived groups actually exist, can.

    Checks EVERY user rather than a sample, so a drift affecting one corner of
    the instance still fails.
    """
    am = AccessManagement(api_client=_make_client())

    users = am.get_users_all()
    if not isinstance(users, list) or not users:
        pytest.skip(f"No users available: {users}")

    # Build each user's groups from the users_per_group side. The all-groups
    # view omits the universal groups by design, so ask for those by name and
    # union them in — that is the documented "targeted answers are complete"
    # rule, and this asserts the two sides really do reconcile.
    from_groups: dict[str, set[str]] = {}
    rows = am.users_per_group()
    assert isinstance(rows, list), f"users_per_group failed: {rows}"
    for row in rows:
        from_groups.setdefault(row["USER_ID"], set()).add(row["GROUP_NAME"])

    for universal in ("Everyone", "All users in system"):
        named = am.users_per_group(universal)
        if isinstance(named, dict):
            continue  # group does not exist on this instance
        for row in named:
            from_groups.setdefault(row["USER_ID"], set()).add(row["GROUP_NAME"])

    mismatches = []
    for user in users:
        from_user = set(user["GROUPS"])
        expected = from_groups.get(user["USER_ID"], set())
        if from_user != expected:
            mismatches.append(f"{user['EMAIL']}: get_users_all={sorted(from_user)} vs users_per_group={sorted(expected)}")

    assert not mismatches, "canonical methods disagree about group membership:\n  " + "\n  ".join(mismatches[:10])

    # And the single-user reader must agree with the bulk reader.
    sample = next((u for u in users if u.get("EMAIL")), None)
    if sample:
        one = am.get_user(sample["EMAIL"])
        assert set(one["GROUPS"]) == set(sample["GROUPS"]), "get_user disagrees with get_users_all"
        assert set(one["GROUP_IDS"]) == set(sample["GROUP_IDS"])
