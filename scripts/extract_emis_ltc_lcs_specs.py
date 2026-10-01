"""Extract LTC LCS risk stratification specs from the EMIS XML agent API.

Loads the shared R5 export from blob storage, filters reports to the
Risk Stratification R2 folder, and writes one markdown implementation
guide per report plus an INDEX.md, organised by condition.

The output directory is treated as generated content. Existing markdown
under it is removed before each run so future refetches cannot leave stale
flat files behind.

Usage:
    python scripts/extract_emis_ltc_lcs_specs.py
    python scripts/extract_emis_ltc_lcs_specs.py --pathname "xml-files/NCL LTC LCS R5 updated 27112025.xml"
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

BASE_URL = "https://emis-xml-to-snomed.vercel.app"
DEFAULT_PATHNAME = "xml-files/NCL LTC LCS R5 updated 27112025.xml"
DEFAULT_FOLDER_FILTER = "Risk Stratification R2"
DEFAULT_OUT_DIR = "docs/emis_specs/ltc_lcs_r5/risk_stratification/specs"

SPEC_GROUP_LABELS = {
    "conditions/base_population": "Base population",
    "conditions/af": "Atrial fibrillation",
    "conditions/asthma_adult": "Asthma Adult",
    "conditions/asthma_cyp": "Asthma CYP",
    "conditions/chd": "CHD",
    "conditions/ckd": "CKD",
    "conditions/copd": "COPD",
    "conditions/diabetes": "Diabetes",
    "conditions/hf": "Heart failure",
    "conditions/hypertension": "Hypertension",
    "conditions/nafld": "NAFLD",
    "conditions/pad": "PAD",
    "conditions/stroke_tia": "Stroke/TIA",
    "shared/dental": "Shared - dental",
    "shared/hypertension_asthma": "Shared - hypertension or asthma",
    "shared/moc": "Shared - MOC and call/recall",
    "shared/risk_groups": "Shared - risk groups",
    "shared/workflow": "Shared - workflow",
    "shared/other": "Shared - other",
}

SPEC_GROUP_ORDER = list(SPEC_GROUP_LABELS)


def api(path, body=None, timeout=120):
    url = f"{BASE_URL}{path}"
    if body is not None:
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    else:
        req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def slugify(name):
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return re.sub(r"_+", "_", slug)


def spec_group(search_name):
    name = search_name.lower()

    if "dentist" in name:
        return "shared/dental"
    if "interpreter" in name:
        return "shared/workflow"
    if "moc" in name or "c&t" in name:
        return "shared/moc"
    if "hypertension or asthma" in name:
        return "shared/hypertension_asthma"

    condition_patterns = [
        ("conditions/base_population", ["ltc lcs base"]),
        ("conditions/asthma_cyp", ["asthma cyp", "asthma(cyp)", "cypast"]),
        ("conditions/asthma_adult", ["asthma adult", "asthma(adult)", "adult asthma"]),
        ("conditions/stroke_tia", ["stroke/tia", "stroke tia"]),
        ("conditions/hypertension", ["hypertension register", "htn only"]),
        ("conditions/diabetes", ["diabetes"]),
        ("conditions/nafld", ["nafld"]),
        ("conditions/copd", ["copd"]),
        ("conditions/ckd", ["ckd"]),
        ("conditions/chd", ["chd"]),
        ("conditions/pad", ["pad"]),
        ("conditions/hf", ["hf register"]),
        ("conditions/af", ["af register"]),
    ]
    for group, patterns in condition_patterns:
        if any(pattern in name for pattern in patterns):
            return group

    if (
        "hrandcomplex" in name
        or re.match(r"^\d+_(hr|mr|lr)", name)
        or name.startswith(("a) hr", "b) hr", "c) hr", "group", "priority group"))
    ):
        return "shared/risk_groups"

    return "shared/other"


def clean_markdown_tree(out_dir):
    deleted = 0
    for path in out_dir.rglob("*.md"):
        path.unlink()
        deleted += 1
    for path in sorted((p for p in out_dir.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        try:
            path.rmdir()
        except OSError:
            pass
    return deleted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pathname", default=DEFAULT_PATHNAME, help="Blob pathname of the EMIS XML export")
    parser.add_argument("--folder-filter", default=DEFAULT_FOLDER_FILTER, help="Substring of folderPath to include")
    parser.add_argument("--out", default=DEFAULT_OUT_DIR, help="Output directory for markdown files")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    deleted = clean_markdown_tree(out_dir)
    if deleted:
        print(f"Removed {deleted} existing generated markdown files from {out_dir}")

    print(f"Loading document from blob: {args.pathname}")
    doc = api("/api/agent/load-xml-from-blob", {"pathname": args.pathname})["document"]
    doc_id = doc["id"]
    print(f"Document {doc_id} ({doc['reportCount']} reports, sha {doc['xmlSha256'][:12]})")

    reports = api(f"/api/agent/documents/{doc_id}/reports")["reports"]
    targets = [r for r in reports if args.folder_filter in (r.get("folderPath") or "")]
    print(f"{len(targets)} reports under '{args.folder_filter}'")

    # Identical searches can appear more than once in the export; keep one per name.
    seen, deduped, duplicates = {}, [], []
    for r in sorted(targets, key=lambda r: (r["searchName"], r["id"])):
        key = r["searchName"]
        if key in seen:
            duplicates.append((r["id"], key, seen[key]))
        else:
            seen[key] = r["id"]
            deduped.append(r)
    print(f"{len(deduped)} unique search names ({len(duplicates)} duplicates skipped)")

    index_rows = []
    for i, r in enumerate(deduped, 1):
        slug = slugify(r["searchName"])
        group = spec_group(r["searchName"])
        target_dir = out_dir / group
        target_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{slug}.md"
        relpath = f"{group}/{fname}"
        print(f"[{i}/{len(deduped)}] {r['searchName']} -> {relpath}")
        for attempt in range(3):
            try:
                resp = api("/api/agent/query", {
                    "documentId": doc_id,
                    "action": "getImplementationGuide",
                    "reportId": r["id"],
                })
                break
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt == 2:
                    raise
                print(f"  retry after error: {exc}")
                time.sleep(2 * (attempt + 1))
        guide = resp["report"]["implementationGuideMarkdown"]
        header = (
            f"<!-- Extracted from '{doc['fileName']}' (sha256 {doc['xmlSha256']})\n"
            f"     report id: {r['id']}\n"
            f"     folder: {r.get('folderPath', '')}\n"
            f"     extracted: {date.today().isoformat()} by scripts/extract_emis_ltc_lcs_specs.py\n"
            f"     Readable guide only: for exact operators/ranges query the agent API\n"
            f"     (agentInterpretation.decisionFlow[].criteriaDetails). -->\n\n"
        )
        (target_dir / fname).write_text(header + guide, encoding="utf-8")
        index_rows.append((r["searchName"], relpath, group, r["id"]))

    index = [
        "# LTC LCS Risk Stratification - EMIS R5 specs",
        "",
        f"Implementation guides extracted from `{doc['fileName']}`",
        f"(sha256 `{doc['xmlSha256']}`) on {date.today().isoformat()}.",
        "",
        "Regenerate with `python scripts/extract_emis_ltc_lcs_specs.py`.",
        "",
        "Guides are organised under `conditions/<condition>/` where a search belongs",
        "to a single register condition. Shared cross-condition searches live under",
        "`shared/<area>/`.",
        "",
        "This directory is regenerated by `scripts/extract_emis_ltc_lcs_specs.py`.",
        "If a future search lands in `shared/other/`, review `spec_group()` in that",
        "script and add a condition mapping if appropriate.",
    ]

    rows_by_group = {}
    for name, relpath, group, _ in index_rows:
        rows_by_group.setdefault(group, []).append((name, relpath))

    for group in SPEC_GROUP_ORDER:
        rows = rows_by_group.pop(group, [])
        if not rows:
            continue
        index += ["", f"## {SPEC_GROUP_LABELS[group]}", "", "| Search | File |", "|---|---|"]
        index += [f"| {name} | [{Path(relpath).name}]({relpath}) |" for name, relpath in rows]

    for group, rows in sorted(rows_by_group.items()):
        index += ["", f"## {SPEC_GROUP_LABELS.get(group, group)}", "", "| Search | File |", "|---|---|"]
        index += [f"| {name} | [{Path(relpath).name}]({relpath}) |" for name, relpath in rows]

    if duplicates:
        index += ["", "## Skipped duplicates", "",
                  "Identical search names appearing more than once in the export:", ""]
        index += [f"- `{name}` ({rid}, kept {kept})" for rid, name, kept in duplicates]
    (out_dir / "INDEX.md").write_text("\n".join(index) + "\n", encoding="utf-8")
    print(f"Wrote {len(index_rows)} guides + INDEX.md to {out_dir}")


if __name__ == "__main__":
    sys.exit(main())
