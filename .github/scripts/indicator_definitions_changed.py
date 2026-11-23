"""Report whether indicator definitions differ from the deployed state.

def_indicator* models are built from model and column `meta.indicator(s)`
blocks in the manifest and call no ref() for them, so state:modified cannot see
a metadata change. This compares those blocks, plus the macros that build and
archive the definitions, between the state manifest and the compiled manifest.

Prints the reason and writes changed=true/false to GITHUB_OUTPUT. Any error
reports changed=true, so a deploy never skips a definitions refresh it needs.
"""

import json
import os
import sys

PROJECT = "wnl_analytics"
MACROS = ("extract_indicator_metadata", "archive_definitions")


def column_indicator(column):
    meta = column.get("meta") or (column.get("config") or {}).get("meta") or {}
    return meta.get("indicator")


def indicator_metadata(manifest):
    models = {}
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        meta = (node.get("config") or {}).get("meta") or {}
        columns = {
            name: column_indicator(column)
            for name, column in (node.get("columns") or {}).items()
            if column_indicator(column)
        }
        if meta.get("indicator") or meta.get("indicators") or columns:
            models[unique_id] = {
                "name": node.get("name"),
                "indicator": meta.get("indicator"),
                "indicators": meta.get("indicators"),
                "columns": columns,
            }
    macros = {
        name: ((manifest.get("macros", {}).get(f"macro.{PROJECT}.{name}") or {}).get("macro_sql") or "").replace("\r\n", "\n")
        for name in MACROS
    }
    return models, macros


def changes(state, current):
    state_models, state_macros = indicator_metadata(state)
    models, macros = indicator_metadata(current)
    found = sorted(
        models.get(key, state_models.get(key))["name"]
        for key in state_models.keys() | models.keys()
        if state_models.get(key) != models.get(key)
    )
    found += [f"macro {name}" for name in MACROS if state_macros[name] != macros[name]]
    return found


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main(state_path, current_path):
    try:
        found = changes(load(state_path), load(current_path))
    except Exception as e:  # noqa: BLE001 - fail safe to a refresh
        print(f"Could not compare indicator definitions ({e}); refreshing them.")
        found = ["comparison failed"]
    if found:
        print(f"Indicator definitions changed: {', '.join(found)}")
    else:
        print("Indicator definitions unchanged; skipping tag:indicator_definitions+.")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as f:
            f.write(f"changed={'true' if found else 'false'}\n")
    return bool(found)


if __name__ == "__main__":
    main(*(sys.argv[1:3] or ["state/manifest.json", "target/manifest.json"]))
