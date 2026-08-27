# Folder operations reference

`from pysisense import Folder` — `folder = Folder(api_client=api_client)`

Structural CRUD for the Sisense folder hierarchy only — creating/renaming/moving/deleting folders and reading the folder tree. It does **not** touch dashboard content inside folders (use `Dashboard`), and it does not do bulk ownership transfer across a folder tree (that's `AccessManagement.change_folder_and_dashboard_ownership` — see `references/users_groups.md`).

## Folder CRUD

| Method | Notes |
|---|---|
| `create_folder(name, parent_id=None)` | `POST /api/v1/folders`. Omit `parent_id` for a root-level folder. Returns the created folder object (`oid`, `name`, ...) on HTTP 201, or `{"error": "..."}`. |
| `update_folder(folder_id, name=None, parent_id=None, owner=None)` | `PATCH /api/v1/folders/{id}` — only fields explicitly passed are sent; omitted ones are left alone. `parent_id` moves the folder in the tree; `owner` takes a user OID. Returns the updated folder object, or `{"error": "..."}`. |
| `get_folder_id(folder_id)` | `GET /api/v1/folders/{id}` — single folder by OID. Returns `{"error": "..."}` on failure **or if the folder isn't found** (empty response is treated as an error, not `{}`). |
| `delete_folder(folder_id)` | `DELETE /api/v1/folders/{id}`. The folder must be empty/deletable per server rules. Returns `{"message": "Folder with ID '...' deleted successfully."}` on HTTP 204, or `{"error": "..."}`. |

```python
folder.create_folder("Analytics")
folder.create_folder("Q1 Reports", parent_id="65d62c9wregfhg0e33bc64e8")

folder.update_folder("65d62c9wregfhg0e33bc64e8", name="Analytics (Archive)")

folder.delete_folder("65d62c9wregfhg0e33bc64e8")
```

## Listing: flat vs tree

`get_folders(structure="flat")` hits `GET /api/v1/folders?structure={structure}` and returns whatever the API gives back for that structure value — `list[dict]` normally, `{"error": "..."}` on failure.

- `structure="flat"` (the default) — every folder as a single top-level list, no nesting. Useful for bulk lookups/ID mapping before a migration.
- `structure="tree"` — nested hierarchy; child nodes may carry their own `folders`/`dashboards` keys.

`get_all_folders()` is a plain shortcut for `get_folders("tree")` — nothing else changed, same return shape.

```python
flat = folder.get_folders()  # structure="flat" by default
tree = folder.get_all_folders()  # == folder.get_folders("tree")
```

## `get_folder_ancestors` — read the name carefully

`get_folder_ancestors(structure)` is a **thin alias for `get_folders(structure)`** — it passes `structure` straight through to the same `GET /api/v1/folders?structure=...` endpoint, unchanged. It does not compute or filter to an ancestor chain on its own; it exists for compatibility with win2linux workflows that pass an ancestors-specific `structure` string. Unlike `get_folders`, `structure` here is a required positional argument (no default).

```python
folder.get_folder_ancestors("ancestors")  # whatever "ancestors" means server-side for this structure param
```

## `get_navver` — navigation tree

`get_navver()` — `GET /api/v1/navver`, no arguments. Returns the Sisense navigation payload (includes a `folders` key with the hierarchy as rendered in the UI), or `{"error": "..."}`. This is a different payload shape from `get_folders`/`get_all_folders` — use it when you specifically need the navver-flavored tree rather than the folders API's own structure.

## Ownership — not here

Folder objects accept an `owner` field on `update_folder` (single folder, OID only, no name resolution). For **bulk reassignment of an entire folder tree** (subfolders plus, optionally, every dashboard inside it), that's `AccessManagement.change_folder_and_dashboard_ownership` — documented in `references/users_groups.md`. This module has no bulk-ownership or tree-walking method of its own.
