"""Check NICE catalogue metadata and roll-up coverage in a compiled dbt manifest."""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
manifest_path = (
    Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "target/manifest.json"
)
nodes = json.loads(manifest_path.read_text(encoding="utf-8"))["nodes"]
prefix = "models/reporting/olids/measures/nice/"
measures = {
    key: node
    for key, node in nodes.items()
    if node["resource_type"] == "model"
    and node["original_file_path"].replace("\\", "/").startswith(prefix)
    and re.search(r"_ind\d+$", node["name"])
}
if not measures:
    raise SystemExit("No NICE measures found in the manifest.")
rollup = next(
    node
    for node in nodes.values()
    if node["name"] == "fct_person_nice_indicator_status"
)
ancestors = set()
pending = list(rollup["depends_on"]["nodes"])
while pending:
    key = pending.pop()
    if key not in ancestors:
        ancestors.add(key)
        pending.extend(nodes.get(key, {}).get("depends_on", {}).get("nodes", []))

errors = []
ids = set()
for key, node in measures.items():
    name = node["name"]
    meta = node["config"].get("meta", {})
    indicator = meta.get("indicator", {})
    expected_id = re.search(r"_ind(\d+)$", name)[1]

    def check(condition, message):
        if not condition:
            errors.append(f"{name}: {message}")

    check(
        indicator.get("id") == "IND" + expected_id, "metadata ID differs from model ID"
    )
    check(indicator.get("id") not in ids, "duplicate indicator ID")
    ids.add(indicator.get("id"))
    check(key in ancestors, "measure does not feed the NICE status roll-up")
    check(indicator.get("type") == "MEASURE", "indicator type must be MEASURE")
    check(
        indicator.get("source_column") == "is_in_numerator",
        "source must be the achievement flag",
    )
    check("is_in_numerator" in node.get("columns", {}), "source column is undocumented")
    check(
        indicator.get("is_qof") is False,
        "NICE pre-adjustment results are not QOF payment measures",
    )
    check(
        "NICE_GUIDANCE" in indicator.get("usage_contexts", []),
        "NICE catalogue usage is missing",
    )
    check(
        indicator.get("sort_order") == "NICE_IND" + expected_id,
        "NICE sort key differs from ID",
    )
    check(
        meta.get("clinical_source", "").startswith(
            f"https://www.nice.org.uk/indicators/ind{expected_id}"
        ),
        "official indicator source is missing",
    )
    for field in (
        "category",
        "clinical_domain",
        "name_short",
        "description_short",
        "description_long",
    ):
        check(bool(str(indicator.get(field, "")).strip()), f"{field} is missing")
    check(
        "\n\n" in node.get("description", ""),
        "model description has no paragraph separation",
    )
    check(
        bool(meta.get("custom_message")), "inherited OLIDS data-use warning is missing"
    )

if errors:
    print("\n".join(errors))
    raise SystemExit(1)
print(
    f"PASS: {len(measures)} distinct NICE measures have catalogue metadata, documented source flags and roll-up lineage."
)
