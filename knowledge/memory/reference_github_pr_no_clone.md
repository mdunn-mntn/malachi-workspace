---
name: reference_github_pr_no_clone
description: Use the GitHub MCP tools on repos you have not cloned — both to READ/investigate prod source across the whole SteelHouse org (search_code, get_file_contents, commit-pinned permalinks to cite exact lines) and to WRITE a PR (create_branch, blob-sha update, create_pull_request), plus how to read another team's CI on the PR.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [github mcp, search_code, get_file_contents, list_commits, commit permalink, blob sha, read prod source, code forensics, cite exact line, find the code, create_pull_request, create_or_update_file, create_branch, open pr without cloning, another team repo, cross-repo pr, shopper_graph, SteelHouse, org code search, lint_comms pr, coverage report non-blocking, required review gate, on-call durable fix pr, INC-006]
domain: [repos, workflow, infra]
lifecycle: active
last_verified: 2026-07-29
---
The GitHub MCP tools reach every repo in the **`SteelHouse`** org (auth = `mdunn-mntn`, Targeting team) without a local clone — for READING prod source as well as opening PRs.

**READING / investigating prod source (answer "how does system X actually work / what's the exact value" from the code itself, and hand back a clickable line link).** Used 2026-07-30 to pin the DS14 build windows + serving TTL to source for a cross-team question:
1. `search_code` scoped to the org with qualifiers: `org:SteelHouse filename:create_mntn_global_data_pyspark.py`, `org:SteelHouse repo:SteelHouse/membership-db data_source_ttls`, `org:SteelHouse path:spark <term>`. Run several term variants; do not stop at one query.
2. `get_file_contents {owner, repo, path}` (no `ref` = default branch) to read the file; the resource line names the commit sha it resolved.
3. **Commit-pinned permalink** to cite an exact line: `list_commits {owner, repo, perPage:1}` → head sha, then build `https://github.com/SteelHouse/<repo>/blob/<sha>/<path>#L<start>-L<end>`. Pin to the sha (not `main`) so line numbers can't drift. Count lines from the raw file content the tool returned to get the anchor right.
4. Prod repos reached this way include `mntn_global_data`, `airflow-ti`, `airflow`, `membership-db` (Rust), `audience-service`, `idso`, `sqlmesh`, `olympus`, `rtb-corpus`. Compass is stronger for a guided multi-repo trace, but direct `search_code`+`get_file_contents` is faster when you already know the file/symbol, and it gives you the exact permalink to hand someone. See [[reference_compass]].

**WRITING a PR on a repo you have NOT cloned**, entirely through the GitHub MCP tools. Used for on-call durable fixes in repos I don't keep checked out (e.g. `SteelHouse/shopper_graph` PR #296, INC-006; airflow-ti PRs also work this way).

**Flow:**
1. Confirm the **base/default branch** (`list_branches`, or `get_file_contents` with no `ref` returns the default). e.g. shopper_graph default = `main` (protected).
2. `get_file_contents` on each file you'll change. The result's `SHA:` line is the **blob sha** you must pass to update it.
3. `create_branch` `{owner, repo, branch, from_branch: <default>}`.
4. `create_or_update_file` per file: `{owner, repo, branch, path, content, sha, message}`. `content` is the **WHOLE new file**, not a diff; `sha` = the blob sha from step 2 (required for an existing file). Each call is its own commit.
5. `create_pull_request` `{owner, repo, title, head: <branch>, base: <default>, body}`.

**Before opening:** lint the PR body against the Terse Comms caps yourself — the Jira-write PreToolUse hook does NOT fire on GitHub MCP calls: `python3 .claude/scripts/lint_comms.py --kind pr --file draft.txt` (cap 900 chars / 130 words / 10 bullets). Branch naming I use: `audi/<short-desc>-inc<NNN>`.

**Reading another team's CI on the PR:** a **coverage report** posted by `github-actions[bot]` is an informational COMMENT, not a merge gate. A "Coverage 0%" badge on untested entrypoint scripts is usually pre-existing (check whether the same files read 0% before your change), not something you introduced. The real merge gate on a protected repo is **"Review required — 1 approving review by a write-access reviewer"**, which only clears when the owning team approves. Confirm "All checks have passed" (tests green) separately. You cannot self-approve or merge someone else's protected repo; route to the owner.

**Editing another team's repo:** keep the diff surgical, preserve THEIR existing comments/style (the sparse-comment rule is for OUR code), state in the PR body that you could not run their tests, and frame it for the owner to review and merge. See [[reference_airflow_ti]] (our own model-repo deploy flow), [[reference_compass]], [[feedback_airflow_prod_safety]].
