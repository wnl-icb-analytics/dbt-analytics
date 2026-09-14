"""Create, update, or resolve dbt failure issues from run_results.json.

Called after every dbt build (deploy and scheduled workflows) with:
  DBT_CONTEXT   "deploy" or a scheduled run type (daily, weekly, ...)
  BUILD_OUTCOME "success" or "failure" (the dbt step's outcome)
  RUN_URL       link to the workflow run
  GH_TOKEN      token with issues:write
  ISSUE_LABEL   label used for this workflow's failure issues
  LEGACY_ISSUE_LABEL  optional previous label to migrate/resolve

On failure: opens (or comments on) an issue listing the failed nodes with
their error messages, plus a machine-readable failed-nodes block. When the
failed resources or their upstream dbt dependencies changed since the same
scheduled run type last succeeded, the most likely merged PR author is
mentioned. Deploy failures consider only the commit being deployed.

On success:
  - scheduled contexts: close the run type's issue (same selection retried
    each run, so a green run means genuine recovery)
  - deploy: close an issue only when every node it names has since built
    green or left the project - unrelated green deploys skip broken nodes
    (publish-always state baseline), so "next green run" proves nothing
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

CONTEXT = os.environ["DBT_CONTEXT"]
OUTCOME = os.environ["BUILD_OUTCOME"]
RUN_URL = os.environ["RUN_URL"]
REPO = os.environ["GITHUB_REPOSITORY"]
LABEL = os.environ.get("ISSUE_LABEL", "dbt-run-failure")
LEGACY_LABEL = os.environ.get("LEGACY_ISSUE_LABEL")

IS_DEPLOY = CONTEXT == "deploy"
TITLE = "dbt deploy failing" if IS_DEPLOY else f"Scheduled dbt run failing: {CONTEXT}"
LABEL_DESCRIPTION = "dbt deploy failure" if IS_DEPLOY else "dbt scheduled run failure"
SUCCESS_STATUSES = {"success", "pass"}
MAX_LISTED = 20
MAX_ATTRIBUTED_PRS = 5
ATTRIBUTION_LOOKBACK_DAYS = {
    "daily": 3,
    "weekly": 10,
    "monthly-full-refresh": 40,
    "dev-monthly-full-refresh": 40,
    "snapshots": 10,
}
COMMIT_MARKER = "__DBT_FAILURE_COMMIT__"


def gh(*args: str) -> str:
    r = subprocess.run(["gh", *args], check=True, text=True, capture_output=True, encoding="utf-8")
    return r.stdout


def git(*args: str) -> str:
    r = subprocess.run(["git", *args], check=True, text=True, capture_output=True, encoding="utf-8")
    return r.stdout


def load_results() -> list[dict]:
    try:
        with open("target/run_results.json", encoding="utf-8") as f:
            return json.load(f).get("results", [])
    except OSError:
        return []


def load_manifest() -> dict | None:
    try:
        with open("target/manifest.json", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def manifest_node_ids() -> tuple[bool, set[str]]:
    """(available, node ids). An unreadable manifest must NOT read as an empty
    project - that would make every tracked node look removed and wrongly
    auto-close unresolved deploy issues."""
    manifest = load_manifest()
    if manifest is None:
        return False, set()
    return True, set(manifest.get("nodes", {})) | set(manifest.get("sources", {}))


def find_open_issues() -> list[dict]:
    """Find current issues plus any still carrying the pre-split label."""
    issues: dict[int, dict] = {}
    for label in dict.fromkeys(filter(None, (LABEL, LEGACY_LABEL))):
        out = gh("issue", "list", "--repo", REPO, "--label", label, "--state", "open",
                 "--search", f'in:title "{TITLE}"', "--json", "number,title,labels")
        for issue in json.loads(out):
            if issue["title"] == TITLE:
                issues[issue["number"]] = issue
    return list(issues.values())


def migrate_legacy_label(issue: dict) -> None:
    """Move a pre-split issue to its context-specific label on repeat failure."""
    labels = {label["name"] for label in issue.get("labels", [])}
    if LEGACY_LABEL and LEGACY_LABEL != LABEL and LEGACY_LABEL in labels:
        gh("issue", "edit", str(issue["number"]), "--repo", REPO,
           "--add-label", LABEL, "--remove-label", LEGACY_LABEL)
        print(f"migrated issue #{issue['number']} from {LEGACY_LABEL} to {LABEL}")


def issue_texts(issue_number: int) -> list[str]:
    out = gh("issue", "view", str(issue_number), "--repo", REPO, "--json", "body,comments")
    data = json.loads(out)
    return [data.get("body") or ""] + [c.get("body") or "" for c in data.get("comments", [])]


def failed_node_blocks(issue_number: int) -> set[str]:
    """Union of unique_ids from every ```failed-nodes block in body + comments."""
    ids: set[str] = set()
    for t in issue_texts(issue_number):
        for block in re.findall(r"```failed-nodes\n(.*?)```", t, flags=re.DOTALL):
            ids.update(line.strip() for line in block.splitlines() if line.strip())
    return ids


def already_attributed_prs(issue_number: int | None) -> set[int]:
    if issue_number is None:
        return set()
    numbers: set[int] = set()
    for text in issue_texts(issue_number):
        for block in re.findall(r"<!-- dbt-attributed-prs: ([\d, ]+) -->", text):
            numbers.update(int(n) for n in block.split(",") if n.strip())
    return numbers


def dependency_paths(manifest: dict, unique_id: str) -> dict[str, int]:
    """Map a failed node and its dbt ancestors to paths and dependency distance.

    A generic test's directly attached model is distance zero too: changing the
    model output is at least as relevant as changing the YAML test definition.
    """
    resources = manifest.get("nodes", {}) | manifest.get("sources", {})
    root = resources.get(unique_id)
    if root is None:
        return {}

    queue: list[tuple[str, int]] = [(unique_id, 0)]
    if root.get("resource_type") == "test":
        queue.extend((uid, 0) for uid in root.get("depends_on", {}).get("nodes", []))

    seen: dict[str, int] = {}
    paths: dict[str, int] = {}
    while queue:
        uid, distance = queue.pop(0)
        if uid in seen and seen[uid] <= distance:
            continue
        seen[uid] = distance
        node = resources.get(uid)
        if node is None:
            continue
        path = node.get("original_file_path")
        if path:
            paths[path] = min(distance, paths.get(path, distance))
        for dependency in node.get("depends_on", {}).get("nodes", []):
            queue.append((dependency, distance + 1))
    return paths


def previous_success_sha() -> str | None:
    """Find the previous green run for this exact scheduled run type."""
    if IS_DEPLOY:
        return None
    out = gh("run", "list", "--repo", REPO, "--workflow", "dbt-scheduled.yml",
             "--status", "success", "--limit", "100",
             "--json", "databaseId,displayTitle,headSha")
    expected_title = f"dbt {CONTEXT}"
    current_run_id = int(os.environ.get("GITHUB_RUN_ID", "0"))
    for run in json.loads(out):
        if run.get("displayTitle") == expected_title and run.get("databaseId") != current_run_id:
            return run.get("headSha")
    return None


def commits_to_consider() -> list[dict]:
    """Return changed paths by commit, newest first, for the causal window."""
    if IS_DEPLOY:
        revision_args = ["-1", os.environ.get("GITHUB_SHA", "HEAD")]
    else:
        baseline = previous_success_sha()
        if baseline:
            try:
                git("merge-base", "--is-ancestor", baseline, "HEAD")
                revision_args = [f"{baseline}..HEAD"]
            except subprocess.CalledProcessError:
                baseline = None
        if not baseline:
            days = ATTRIBUTION_LOOKBACK_DAYS.get(CONTEXT, 10)
            revision_args = [f"--since={days} days ago"]

    out = git("log", *revision_args, f"--format={COMMIT_MARKER}%H%x1f%ct%x1f%s",
              "--name-only", "--no-renames")
    commits: list[dict] = []
    current: dict | None = None
    for line in out.splitlines():
        if line.startswith(COMMIT_MARKER):
            sha, timestamp, subject = line[len(COMMIT_MARKER):].split("\x1f", 2)
            current = {"sha": sha, "timestamp": int(timestamp), "subject": subject, "paths": set()}
            commits.append(current)
        elif current is not None and line.strip():
            current["paths"].add(line.strip())
    return commits


def pull_request_for_commit(commit: dict) -> dict | None:
    """Return the merged PR associated with a commit, if there is one."""
    out = gh("api", f"repos/{REPO}/commits/{commit['sha']}/pulls")
    pulls = [pr for pr in json.loads(out) if pr.get("merged_at")]
    if not pulls:
        # Squash merges in this repository conventionally end in "(#123)".
        match = re.search(r"\(#(\d+)\)$", commit["subject"])
        if not match:
            return None
        out = gh("pr", "view", match.group(1), "--repo", REPO,
                 "--json", "number,title,url,author,mergedAt")
        pr = json.loads(out)
        if not pr.get("mergedAt"):
            return None
        return {
            "number": pr["number"], "title": pr["title"], "url": pr["url"],
            "author": pr.get("author", {}).get("login"),
        }
    pr = pulls[0]
    return {
        "number": pr["number"], "title": pr["title"], "url": pr["html_url"],
        "author": pr.get("user", {}).get("login"),
    }


def likely_pull_requests(failed: list[dict]) -> list[dict]:
    """Choose the closest changed dependency's PR for each failed node."""
    manifest = load_manifest()
    if manifest is None:
        return []
    commits = commits_to_consider()
    pr_cache: dict[str, dict | None] = {}
    likely: dict[int, dict] = {}

    for result in failed[:MAX_LISTED]:
        distances = dependency_paths(manifest, result["unique_id"])
        candidates = []
        for order, commit in enumerate(commits):
            overlap = commit["paths"] & distances.keys()
            if overlap:
                candidates.append((min(distances[path] for path in overlap), order, commit, overlap))
        for _, _, commit, overlap in sorted(candidates, key=lambda item: item[:2]):
            if commit["sha"] not in pr_cache:
                pr_cache[commit["sha"]] = pull_request_for_commit(commit)
            pr = pr_cache[commit["sha"]]
            if pr is not None:
                entry = likely.setdefault(pr["number"], pr | {"paths": set(), "nodes": set()})
                entry["paths"].update(overlap)
                entry["nodes"].add(short(result["unique_id"]))
                break
    return list(likely.values())[:MAX_ATTRIBUTED_PRS]


def attribution_lines(failed: list[dict], issue_number: int | None) -> list[str]:
    """Format cautious PR attribution; attribution failure must never hide the run failure."""
    try:
        pull_requests = likely_pull_requests(failed)
        previously_pinged = already_attributed_prs(issue_number)
    except (json.JSONDecodeError, OSError, subprocess.CalledProcessError, ValueError) as error:
        print(f"PR attribution unavailable: {error}", file=sys.stderr)
        return []
    if not pull_requests:
        return []

    lines = ["", "Likely related merged change(s):"]
    for pr in pull_requests:
        author = pr.get("author") or "unknown author"
        can_ping = author != "unknown author" and not author.endswith("[bot]")
        author_text = f"@{author}" if can_ping and pr["number"] not in previously_pinged else author
        paths = sorted(pr["paths"])
        path_text = ", ".join(f"`{path}`" for path in paths[:3])
        if len(paths) > 3:
            path_text += f", and {len(paths) - 3} more"
        lines.append(f"- [#{pr['number']}: {pr['title']}]({pr['url']}) by {author_text} — {path_text}")
    lines.append("_Inferred from changed files on the failed nodes' dbt dependency paths; this is a lead, not an assignment of fault._")
    lines.append(f"<!-- dbt-attributed-prs: {','.join(str(pr['number']) for pr in pull_requests)} -->")
    return lines


def short(uid: str) -> str:
    return uid.split(".")[-1]


def report_failure() -> None:
    results = load_results()
    failed = [r for r in results if r.get("status") in ("error", "fail")]
    skipped = [r for r in results if r.get("status") == "skipped"]

    gh("label", "create", LABEL, "--repo", REPO, "--color", "B60205",
       "--description", LABEL_DESCRIPTION, "--force")
    issues = find_open_issues()
    issue_number = issues[0]["number"] if issues else None

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"[{CONTEXT} run failed]({RUN_URL}) at {now}.", ""]
    if failed:
        lines.append("Failed nodes:")
        for r in failed[:MAX_LISTED]:
            msg = (r.get("message") or "").replace("\n", " ").strip()
            lines.append(f"- `{short(r['unique_id'])}` ({r['status']}): {msg[:250]}")
        if len(failed) > MAX_LISTED:
            lines.append(f"- ...and {len(failed) - MAX_LISTED} more")
        if skipped:
            lines.append(f"- plus {len(skipped)} skipped downstream nodes")
        lines += ["", "```failed-nodes"]
        lines += sorted(r["unique_id"] for r in failed)
        lines.append("```")
        lines += attribution_lines(failed, issue_number)
    else:
        lines.append("No run_results available - the build died before or during parse; see the run log.")

    body = "\n".join(lines)
    if issues:
        migrate_legacy_label(issues[0])
        gh("issue", "comment", str(issues[0]["number"]), "--repo", REPO, "--body", body)
        print(f"commented on issue #{issues[0]['number']}")
    else:
        if IS_DEPLOY:
            body += ("\n\nFailed nodes are not retried by unrelated deploys; this issue closes "
                     "automatically once every node listed here builds green (its fix merged) "
                     "or is removed from the project.")
        else:
            body += f"\n\nAuto-closes on the next successful {CONTEXT} run."
        gh("issue", "create", "--repo", REPO, "--title", TITLE, "--label", LABEL, "--body", body)
        print("created issue")


def resolve_success() -> None:
    issues = find_open_issues()
    if not issues:
        print("no open failure issue")
        return

    if not IS_DEPLOY:
        for i in issues:
            gh("issue", "close", str(i["number"]), "--repo", REPO,
               "--comment", f"Recovered: [successful {CONTEXT} run]({RUN_URL}).")
            print(f"closed issue #{i['number']}")
        return

    built_green = {r["unique_id"] for r in load_results() if r.get("status") in SUCCESS_STATUSES}
    manifest_available, current_nodes = manifest_node_ids()
    if not manifest_available:
        print("manifest unavailable - leaving deploy failure issues open")
        return
    for i in issues:
        tracked = failed_node_blocks(i["number"])
        unresolved = {uid for uid in tracked if uid in current_nodes and uid not in built_green}
        if tracked and not unresolved:
            gh("issue", "close", str(i["number"]), "--repo", REPO,
               "--comment", f"All tracked nodes built green or were removed - [run]({RUN_URL}).")
            print(f"closed issue #{i['number']}")
        else:
            print(f"issue #{i['number']} stays open ({len(unresolved)} nodes still broken)")


def main() -> None:
    if OUTCOME == "failure":
        report_failure()
    elif OUTCOME == "success":
        resolve_success()
    else:
        print(f"outcome {OUTCOME} - nothing to do")


if __name__ == "__main__":
    main()
