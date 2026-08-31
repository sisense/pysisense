# Upgrading pysisense

How to move between major versions of the SDK. (Not to be confused with the
[`Migration` module](migration.md), which moves users, dashboards and data models between
Sisense *environments* — a different thing entirely.)

---

## From 1.x to 2.0

This section is the single reference for what changed between `pysisense` **1.1.0** and
**2.0.0**, written to be usable in both directions: whether you are upgrading, or you are
reading code that still targets 1.x.

`pysisense` follows semantic versioning — breaking changes only land in a major release.
If you are pinned to `pysisense>=1,<2`, nothing here affects you until you choose to move.

---

## First: which version is actually installed?

Every symptom below depends on it, so establish this before anything else:

```python
import pysisense
print(pysisense.__version__)
```

```bash
pip show pysisense | grep Version
```

---

## The short version

**The user row is additive.** `ROLE_NAME` and `GROUPS` keep the names *and the meanings*
they had in 1.x, so `user["ROLE_NAME"] == "sysAdmin"` and `user["GROUPS"]` keep working.
Two new fields sit alongside them: `ROLE_RAW_NAME` (Sisense's own role value) and
`GROUP_IDS` (the group IDs).

The one changed value is that `Everyone` is no longer stripped from `GROUPS` — it appears
where it used to be hidden. Nothing else in the row silently changes meaning.

The changes that *do* need action are outside the user row: failure shapes, the bulk
outcome shape, and one removed method. They all fail loudly.

## Symptom → cause → fix

Start here if something is already broken.

| Symptom | Cause | Fix |
|---|---|---|
| `"Everyone"` suddenly appears in `user["GROUPS"]` | On 2.x. `get_users_all()` no longer strips it. The key and its meaning are unchanged — only this one value was added. | Filter it out yourself if you don't want it. |
| `Role 'sysAdmin' not found in roles_mapping` from `create_user`/`update_user` | On 1.x. Writes only accepted `viewer`/`designer` plus raw names. | Upgrade to 2.0 (accepts both vocabularies), or pass the raw name `"super"`. |
| `TypeError` iterating `get_unused_columns_bulk(...)`, or rows are missing | On 2.x. It returns a dict, not a list. | Read `result["results"]`; per-model failures are in `result["errors"]`. |
| `AttributeError: 'DataModel' object has no attribute 'get_connections'` | On 2.x. The alias was removed. | Use `get_connections_all()`. |
| A write method returns a dict where your code expected a string | On 2.x. `add_dashboard_shares`, `add_dashboard_script`, `add_widget_script` return dicts. | Read `result["message"]`, or check `result.get("ok") is False`. |
| A read method returns an error dict where your code expected `[]` | On 2.x. Failures no longer disguise themselves as empty lists. | Check `result.get("ok") is False` before iterating. |
| Empty list from a read method that used to signal a problem | On 2.x. `[]` now always means *genuinely empty*. | Treat `[]` as a real, empty result. |
| `DeprecationWarning: ... is deprecated; use <name>` | On 2.x. Six methods were deprecated. | Move to the replacement (table below). They still work for now. |

---

## Detecting failures (the one rule worth learning)

In 2.0 **every** failure return carries an explicit `"ok": False` marker:

```python
result = access_mgmt.get_users_all()
if isinstance(result, dict) and result.get("ok") is False:
    print(result["error"])          # human-readable, safe to relay
else:
    for row in result:
        ...
```

Never match an exact key set (`result.keys() == {"error"}`) — failure dicts gain additive
keys between releases (`status_code` in 1.1.0, `raw_body` in 2.0).

---

## The canonical user row

`get_user()` and `get_users_all()` return the same row shape in 2.0.

**1.1.0** (`get_users_all`)

```python
{
    "USER_ID": "6a5f...c9",
    "USER_NAME": "jane@example.com",
    "FIRST_NAME": "Jane", "LAST_NAME": "Doe",
    "EMAIL": "jane@example.com",
    "IS_ACTIVE": True,
    "ROLE_ID": "6a5f...53",
    "ROLE_NAME": "sysAdmin",        # display name
    "GROUPS": ["Admins"],           # names only; "Everyone" stripped
}
```

**2.0.0**

```python
{
    "USER_ID": "6a5f...c9",
    "USER_NAME": "jane@example.com",
    "EMAIL": "jane@example.com",
    "FIRST_NAME": "Jane", "LAST_NAME": "Doe",
    "IS_ACTIVE": True,
    "ROLE_ID": "6a5f...53",
    "ROLE_NAME": "sysAdmin",                 # unchanged from 1.x (UI name)
    "ROLE_DISPLAY_NAME": "sysAdmin",         # same value, unambiguous name
    "ROLE_RAW_NAME": "super",                # new: Sisense's own value
    "GROUP_IDS": ["6a5f...c7", "6a5f...60"], # new
    "GROUPS": ["Admins", "Everyone"],        # same key as 1.x; now unfiltered
}
```

| 1.1.0 | 2.0.0 |
|---|---|
| `ROLE_NAME` = display name | `ROLE_NAME` = display name (unchanged), plus `ROLE_DISPLAY_NAME` (same value, explicit) and the new `ROLE_RAW_NAME` (raw value) |
| `GROUPS` = group names | `GROUPS` = group names (unchanged), plus the new `GROUP_IDS` |
| `Everyone` stripped by `get_users_all` | `Everyone` always reported in `GROUPS` |
| `get_user` and `get_users_all` disagreed on both fields | one shape, both methods |
| Failure: `[{"error": ...}]` (list-wrapped) | Failure: plain `{"ok": False, "error": ...}` |

Role vocabulary in full:

| `ROLE_RAW_NAME` (raw Sisense) | `ROLE_NAME` = `ROLE_DISPLAY_NAME` (UI) |
|---|---|
| `consumer` | `viewer` |
| `super` | `sysAdmin` |
| `contributor` | `dashboardDesigner` |

`ROLE_NAME` deliberately keeps its 1.x meaning so existing role comparisons keep working.
In new code prefer `ROLE_DISPLAY_NAME` or `ROLE_RAW_NAME`, which each state which
vocabulary they hold.

An instance may define further roles (`dataDesigner`, `dataAdmin`, `admin`, `tenantAdmin`,
`custom_*`); those appear unchanged in both fields.

**Writes accept either vocabulary in 2.0.** `create_user` and `update_user` take
`"super"` or `"sysAdmin"` or `"sys admin"`, matching case-, space- and
punctuation-insensitively, so a value read from `ROLE_DISPLAY_NAME` can be written straight
back. A role the instance actually defines always wins over an alias — `"admin"` resolves
to a real `admin` role rather than to `super`, and `"data designer"` resolves to
`dataDesigner`, never to `contributor`. In 1.x, `sysAdmin` and `dashboardDesigner` were
rejected.

---

## Group membership

`users_per_group()` returns **flat rows**, one per (group, user):

```python
{
    "GROUP_ID": "...", "GROUP_NAME": "Admins",
    "USER_ID": "...", "USER_NAME": "...", "EMAIL": "...",
    "FIRST_NAME": "...", "LAST_NAME": "...", "IS_ACTIVE": True,
    "ROLE_ID": "...", "ROLE_NAME": "sysAdmin",
    "ROLE_DISPLAY_NAME": "sysAdmin", "ROLE_RAW_NAME": "super",
}
```

- `users_per_group()` with no argument returns **all** memberships (this replaces
  `users_per_group_all()`).
- Groups with no members contribute no rows, so the row count equals the real membership
  count.
- The synthetic `Admins` bucket that `users_per_group_all()` fabricated is gone.
- An unknown group name returns an error dict naming it, rather than an empty list.

`get_groups(name=...)` was added as an exact-match lookup; an unknown name returns an error
dict rather than `[]`.

---

## Deprecated methods

These still work in 2.0 and emit a `DeprecationWarning`. Their behavior is **frozen** at the
1.x shape — they are fossils, so do not mix their output with canonical rows.

| Deprecated | Use instead |
|---|---|
| `get_user_with_role_and_group_names` | `get_user` |
| `get_users_with_role_names_and_group_names` | `get_users_all` |
| `get_users_expanded` | `get_users_all` |
| `get_group` | `get_groups(name=...)` |
| `users_per_group_all` | `users_per_group()` |
| `get_unused_columns` | `get_unused_columns_bulk` |

**Removed in 2.0:** `get_connections` (deprecated in 1.1.0) — use `get_connections_all`.

Programmatic consumers can skip deprecated methods via the PEP 702 marker:

```python
if getattr(method, "__deprecated__", None):
    continue
```

---

## Failure shapes that changed

In 1.x some methods signalled failure with `[]`, `None`, or a message string. In 2.0 they
all return the standard error dict. An empty list from a read method now always means a
genuinely empty result.

| Method | 1.1.0 on failure | 2.0.0 on failure |
|---|---|---|
| `get_data`, `get_dashboard_share`, `get_dashboard_columns`, `get_datamodel_shares`, `get_datasecurity`, `get_datasecurity_detail` | `[]` | error dict |
| `create_connections` | `None` | error dict |
| `add_dashboard_shares`, `add_dashboard_script`, `add_widget_script` | `"Error: ..."` string | error dict |
| `get_users_all` | `[{"error": ...}]` | plain error dict |

The same three write methods also changed on **success**, from a prose string to a dict:

```python
add_dashboard_script(...)  -> {"success": True, "message": "..."}
add_dashboard_shares(...)  -> {"success": True, "message": "...",
                               "new_shares": 1, "updated_shares": 0}
```

---

## Other 2.0 changes

- **`get_unused_columns_bulk`** always returns `{"results": [...], "errors": [{"ref", "error"}]}`.
  Good rows and per-model failures travel together, so a typo'd model name among valid ones
  is reported instead of silently skipped. When nothing could be processed the dict also
  carries `"ok": False` and a top-level `"error"`.
- **`add_datamodel_shares` supports EXTRACT models** (the old "will be fixed in V2" error is
  gone). It returns `{"success": True, "message", "new_shares", "updated_shares", "skipped"}`.
  Check `skipped` — it lists requested shares that were **not** submitted, including shares
  for inactive users, which Sisense accepts and then silently drops.
- **`error` / `raw_body` split**: the `error` string is always a clean sentence. When the
  server returns a body we cannot recognise, the redacted, truncated dump travels in a
  separate `raw_body` key.

---

## Full change list

See [`CHANGELOG.md`](../CHANGELOG.md) for the complete 2.0.0 entry.
