"""Download the last deployed manifest from the DBT_ARTIFACTS stage.

Writes state/manifest.json for use with --select state:modified+ --state state.
Sets has_state=true/false in GITHUB_OUTPUT; false (missing stage, missing file,
or any error) means the caller should fall back to a full build. Also sets
previous_build_status from latest/meta.json (unknown when absent), so a failed
deploy's work can be retried although its manifest became the baseline.
"""

import gzip
import json
import os
import re
import shutil
import sys

import snowflake.connector

STAGE = os.environ.get("ARTIFACT_STAGE", "DATA_LAKE__NCL.DBT_OBSERVABILITY.STG_DBT_ARTIFACTS")
TARGET = os.environ.get("DBT_TARGET", "prod")

if not re.match(r"^[A-Za-z0-9_.]+$", STAGE) or not re.match(r"^[A-Za-z0-9_-]+$", TARGET):
    sys.exit(f"Invalid stage/target: {STAGE} / {TARGET}")


def set_output(has_state: bool, build_status: str = "unknown") -> None:
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"has_state={'true' if has_state else 'false'}\n")
        f.write(f"previous_build_status={build_status}\n")


state_dir = os.path.abspath("state")
os.makedirs(state_dir, exist_ok=True)

try:
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
        cur.execute(
            f"GET '@{STAGE}/{TARGET}/latest/manifest.json.gz' "
            f"'file://{state_dir.replace(chr(92), '/')}/'"
        )
        rows = cur.fetchall()
        print(f"GET result: {rows}")
        cur.execute(
            f"GET '@{STAGE}/{TARGET}/latest/meta.json' "
            f"'file://{state_dir.replace(chr(92), '/')}/'"
        )
    finally:
        conn.close()

    gz_path = os.path.join(state_dir, "manifest.json.gz")
    if not os.path.exists(gz_path):
        print("No previous manifest on stage - full build")
        set_output(False)
        sys.exit(0)

    with gzip.open(gz_path, "rb") as src, open(os.path.join(state_dir, "manifest.json"), "wb") as dst:
        shutil.copyfileobj(src, dst)
    os.remove(gz_path)
    print(f"State manifest ready ({os.path.getsize(os.path.join(state_dir, 'manifest.json'))} bytes)")
    build_status = "unknown"
    try:
        with open(os.path.join(state_dir, "meta.json"), encoding="utf-8") as f:
            build_status = json.load(f).get("build_status") or "unknown"
    except (OSError, ValueError):
        pass
    if not re.match(r"^[a-z_]+$", build_status):
        build_status = "unknown"
    print(f"Previous deploy build status: {build_status}")
    set_output(True, build_status)
except Exception as e:
    # State is an optimisation, not a requirement - never fail the deploy here.
    print(f"State fetch failed ({e}) - full build", file=sys.stderr)
    set_output(False)
