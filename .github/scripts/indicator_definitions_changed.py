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

# Files holding the extractor and the def_indicator* post-hook archive macros.
# Their macro dependencies are followed too, as Fusion does not record hook
# macro dependencies on the models.
MACRO_FILES = (
    "macros/governance/extract_indicator_metadata.sql",
    "macros/governance/archive_indicator_definitions.sql",
)


def indicator_ids(meta):
    ids = [(meta.get("indicator") or {}).get("id")]
    ids += [ind.get("id") for ind in meta.get("indicators") or []]
    return {i for i in ids if i}


def column_indicator(column):
    meta = column.get("meta") or (column.get("config") or {}).get("meta") or {}
    return meta.get("indicator")


def indicator_models(manifest):
    models = {}
    for unique_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        meta = (node.get("config") or {}).get("meta") or {}
        # Like the extractor, skip column meta repeating a model indicator,
        # which Fusion propagates to every column.
        model_ids = indicator_ids(meta)
        columns = {
            name: column_indicator(column)
            for name, column in (node.get("columns") or {}).items()
            if column_indicator(column) and column_indicator(column).get("id") not in model_ids
        }
        if model_ids or meta.get("indicator") or meta.get("indicators") or columns:
            models[unique_id] = {
                "name": node.get("name"),
                "indicator": meta.get("indicator"),
                "indicators": meta.get("indicators"),
                "columns": columns,
            }
    return models


def definition_macros(manifest):
    macros = manifest.get("macros", {})
    pending = [key for key, macro in macros.items() if macro.get("original_file_path") in MACRO_FILES]
    found = {}
    while pending:
        key = pending.pop()
        if key in found or key not in macros:
            continue
        found[key] = (macros[key].get("macro_sql") or "").replace("\r\n", "\n")
        pending += (macros[key].get("depends_on") or {}).get("macros") or []
    return found


def changes(state, current):
    state_models, models = indicator_models(state), indicator_models(current)
    found = sorted(
        models.get(key, state_models.get(key))["name"]
        for key in state_models.keys() | models.keys()
        if state_models.get(key) != models.get(key)
    )
    state_macros, macros = definition_macros(state), definition_macros(current)
    found += sorted(
        f"macro {key.rsplit('.', 1)[-1]}"
        for key in state_macros.keys() | macros.keys()
        if state_macros.get(key) != macros.get(key)
    )
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
