"""Build the queue selection with the UEC encounter needed by its code tests."""

import argparse
import json
import subprocess
import sys


UEC_CODES = {"int_sus_uec_diagnosis", "int_sus_uec_procedure"}
UEC_PARENT = "int_sus_uec_encounter"


def build_selection(nodes):
    # Keep every originally selected test, including tests with deferred parents.
    names = {node["name"] for node in nodes}
    models = {node["name"] for node in nodes if node["resource_type"] == "model"}
    if models & UEC_CODES:
        names.add(UEC_PARENT)
    return sorted(names)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state")
    parser.add_argument("--select", nargs="+", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    common = ["--target", "dev", "--profiles-dir", "."]
    if args.state:
        common += ["--state", args.state]
    result = subprocess.run(
        ["dbt", "ls", *common, "--select", *args.select,
         "--indirect-selection", "eager", "--output", "json",
         "--output-keys", "name", "resource_type", "--quiet"],
        capture_output=True, text=True,
    )
    if result.returncode:
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr)
        raise SystemExit(result.returncode)
    # Fail on unexpected output rather than silently skipping validation.
    nodes = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    selection = build_selection(nodes)
    if not selection:
        print("No nodes selected for DEV validation.")
        return
    if UEC_PARENT in selection and any(n["name"] in UEC_CODES for n in nodes):
        print("Building UEC codes and their encounter parent together.")
    print(f"DEV validation selects {len(selection)} explicit nodes.")
    # Cautious adds the parent's own tests without testing untouched DEV siblings.
    # Original eager tests remain selected explicitly, so none are dropped.
    command = ["dbt", "build", *common, "--select", *selection,
               "--indirect-selection", "cautious"]
    if args.state:
        command += ["--defer", "--favor-state"]
    if args.dry_run:
        print(json.dumps(command))
    else:
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
