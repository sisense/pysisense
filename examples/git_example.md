# Git Example Usage

This guide demonstrates how to use the `Git` class from the `pysisense`
package to work with Sisense **Git Integration** projects.

> **Optional feature.** Git Integration must be enabled by a Sisense admin
> (Admin > App Configuration > Feature Management) before any call below
> returns project data. There's no API to check whether it's enabled — if
> it isn't, these calls fail with a normal Sisense error response, the same
> way any other failure below is reported. Linux-only: there is no Windows
> equivalent of this API surface.

Note: like the other guides in this folder, this is a set of standalone
snippets to copy from — not a script meant to be run top to bottom.

---

## Prerequisites

```python
import os
import json
from pysisense import SisenseClient, Git

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

git = Git(api_client=api_client)
```

---

## Example 1: List Git Projects

```python
projects = git.get_git_projects(current_branch_name=True, last_commit_date=True)
print(json.dumps(projects, indent=4))
```

---

## Example 2: Get a Single Project

Accepts a project OID directly.

```python
project = git.get_git_project("630581af4b046d0011cdec49", branch_mappings=True, tag_names=True)
print(json.dumps(project, indent=4))
```

---

## Example 3: Resolve a Project by Name

Every other method in this class accepts either an OID or a project name
and resolves it the same way internally.

```python
resolved = git.resolve_git_project_reference("Marketing_Project")
if resolved["success"]:
    print(resolved["project_id"], resolved["project_name"])
```

---

## Example 4: Get the Status Checksum

Required as the `x-git-status-checksum` header on every write call below —
fetch a fresh one immediately before each such call.

```python
status = git.get_git_project_status("Marketing_Project")
checksum = status["statusChecksum"]
```

---

## Example 5: List and Create Branches

```python
branches = git.get_git_branches("Marketing_Project")
print(json.dumps(branches, indent=4))

checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
new_branch = git.create_git_branch("Marketing_Project", "release/Q4", checksum, parent="main")
print(json.dumps(new_branch, indent=4))
```

---

## Example 6: Check Out a Branch

```python
checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
result = git.checkout_git_branch("Marketing_Project", "release/Q4", checksum)
print(json.dumps(result, indent=4))
```

---

## Example 7: List and Create Commits

```python
commits = git.get_git_commits("Marketing_Project")
print(json.dumps(commits, indent=4))

checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
new_commit = git.create_git_commit(
    "Marketing_Project",
    "Update Q4 dashboard filters",
    ["dashboards/65363add.../widgets/65363add.../widget.json"],
    checksum,
)
print(json.dumps(new_commit, indent=4))
```

---

## Example 8: Check Out a Commit

```python
checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
result = git.checkout_git_commit("Marketing_Project", new_commit["hash"], checksum)
print(json.dumps(result, indent=4))
```

---

## Example 9: Discard Uncommitted Changes

```python
checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
result = git.discard_git_project_changes(
    "Marketing_Project",
    ["dashboards/65363add.../widgets/65363add.../widget.json"],
    checksum,
)
print(json.dumps(result, indent=4))
```

---

## Example 10: Fetch, Pull, and Push a Remote

```python
credentials = {"username": "ci-bot", "password": os.environ["GIT_TOKEN"], "save": False}

git.git_fetch("Marketing_Project", https_credentials=credentials)

checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
git.git_pull("Marketing_Project", "main", checksum, https_credentials=credentials)

checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
push_result = git.git_push("Marketing_Project", "release/Q4", "release/Q4", checksum, https_credentials=credentials)
print(json.dumps(push_result, indent=4))
```

---

## Example 11: Sync Project Files with the Application

```python
checksum = git.get_git_project_status("Marketing_Project")["statusChecksum"]
result = git.sync_git_project("Marketing_Project", checksum)
print(json.dumps(result, indent=4))
```

---

## Example 12: Unlock a Stuck Project (admin token required)

```python
result = git.unlock_git_project("Marketing_Project")
print(json.dumps(result, indent=4))
```

---

## Notes

- A locked project (HTTP 423) means another in-flight request is currently
  modifying it — retry, or use `unlock_git_project` if the lock was left
  behind by an interrupted request.
- `checkout_git_branch`, `checkout_git_commit`, and
  `discard_git_project_changes` can answer HTTP 453 when the Git operation
  itself succeeded but some assets failed validation on import — the rest
  of the assets are still imported.
