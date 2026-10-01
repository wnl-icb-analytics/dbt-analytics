"""Upload dbt artifacts to the DBT_ARTIFACTS stage.

Puts target/manifest.json + target/run_results.json (auto-compressed to .gz)
and a meta.json sidecar to @<stage>/<target>/latest/. Runs that do not pass
compile are stored under attempts/<run_id>/ without replacing deploy state.
Auth: key-pair via SNOWFLAKE_PRIVATE_KEY_PATH (same env as the workflows).
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

import snowflake.connector

STAGE = os.environ["ARTIFACT_STAGE"]
TARGET = os.environ["DBT_TARGET"]

if not re.match(r"^[A-Za-z0-9_.]+$", STAGE) or not re.match(r"^[A-Za-z0-9_-]+$", TARGET):
    sys.exit(f"Invalid stage/target: {STAGE} / {TARGET}")

manifest = "target/manifest.json"
run_results = "target/run_results.json"
build_status = os.environ.get("DBT_BUILD_OUTCOME", "success")
compile_status = os.environ.get("DBT_COMPILE_OUTCOME", "success")
if not os.path.exists(manifest):
    # Build died before parse (compile/setup error) - keep the previous
    # baseline rather than failing the publish step on an already-red run.
    print(f"{manifest} not found - skipping publish")
    sys.exit(0)

build_ran = build_status in {"success", "failure"}
promote_manifest = compile_status == "success" and build_ran

# Record the engine that produced the artifacts (unpinned installs = latest).
fusion_version = None
try:
    import subprocess

    out = subprocess.run(["dbt", "--version"], capture_output=True, text=True, timeout=30).stdout
    fusion_version = out.strip().splitlines()[0] if out.strip() else None
except Exception:
    pass

meta = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "commit_sha": os.environ.get("GITHUB_SHA"),
    "ref": os.environ.get("GITHUB_REF_NAME"),
    "run_id": os.environ.get("GITHUB_RUN_ID"),
    "run_url": "{server}/{repo}/actions/runs/{run}".format(
        server=os.environ.get("GITHUB_SERVER_URL", "https://github.com"),
        repo=os.environ.get("GITHUB_REPOSITORY", ""),
        run=os.environ.get("GITHUB_RUN_ID", ""),
    ),
    "workflow": os.environ.get("GITHUB_WORKFLOW"),
    "dbt_command": os.environ.get("DBT_COMMAND"),
    "target": TARGET,
    "fusion_version": fusion_version,
    "build_status": build_status,
    "compile_status": compile_status,
    "state_manifest_promoted": promote_manifest,
}
with open("target/meta.json", "w") as f:
    json.dump(meta, f, indent=2)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
    private_key_file_pwd=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode() or None,
    role="DBT_ADMIN",
    warehouse="WH_NCL_ENGINEERING_XS",
)
try:
    cur = conn.cursor()
    uploads = [(manifest, True), ("target/meta.json", False)]
    if os.path.exists(run_results):
        uploads.insert(1, (run_results, True))
    else:
        print(f"{run_results} not found - skipping")

    if promote_manifest:
        dest = f"@{STAGE}/{TARGET}/latest/"
    else:
        run_id = os.environ.get("GITHUB_RUN_ID", "unknown")
        if not re.match(r"^[A-Za-z0-9_-]+$", run_id):
            sys.exit(f"Invalid run id: {run_id}")
        dest = f"@{STAGE}/{TARGET}/attempts/{run_id}/"
        print(
            "Keeping the previous deploy state because compile did not pass "
            "or the build step did not run"
        )

    for path, compress in uploads:
        path = os.path.abspath(path).replace("\\", "/")
        cur.execute(
            f"PUT 'file://{path}' '{dest}' "
            f"AUTO_COMPRESS={'TRUE' if compress else 'FALSE'} OVERWRITE=TRUE"
        )
        print(f"Uploaded {path} -> {dest} ({cur.fetchall()[0][6]})")

    if not promote_manifest:
        print(f"Diagnostic artifacts stored in {dest}")
finally:
    conn.close()
