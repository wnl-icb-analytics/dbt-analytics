"""Build the NHS occupation code seed from the official NHS England workbook."""

from __future__ import annotations

import argparse
import csv
import io
import re
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path


SOURCE_URL = (
    "https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/"
    "areas-of-interest/workforce/nhs-occupation-codes/"
    "nhs-occupation-code-manual-version-22.1_final.xlsx"
)
SHEET_NAME = "Occupation Code Lookup"
OUTPUT_COLUMNS = [
    "occupation_code",
    "occupation_code_name",
    "main_staff_group",
    "broad_staff_group",
    "detailed_staff_group",
    "occupation_level",
    "care_setting_or_specialty",
    "is_currently_valid",
]
SPREADSHEET_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONSHIP_NS = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
)


def read_source(path: Path | None) -> bytes:
    if path:
        return path.read_bytes()
    with urllib.request.urlopen(SOURCE_URL, timeout=60) as response:
        return response.read()


def sheet_rows(workbook_bytes: bytes) -> list[dict[str, str]]:
    namespaces = {"m": SPREADSHEET_NS, "r": RELATIONSHIP_NS}
    with zipfile.ZipFile(io.BytesIO(workbook_bytes)) as workbook:
        shared_root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
        shared_strings = [
            "".join(node.text or "" for node in item.iter(f"{{{SPREADSHEET_NS}}}t"))
            for item in shared_root.findall("m:si", namespaces)
        ]
        workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
        relationships_root = ET.fromstring(
            workbook.read("xl/_rels/workbook.xml.rels")
        )
        relationships = {
            node.attrib["Id"]: node.attrib["Target"]
            for node in relationships_root
        }
        sheet = next(
            node
            for node in workbook_root.find("m:sheets", namespaces)
            if node.attrib["name"] == SHEET_NAME
        )
        relationship_id = sheet.attrib[f"{{{RELATIONSHIP_NS}}}id"]
        sheet_path = "xl/" + relationships[relationship_id].lstrip("/")
        sheet_root = ET.fromstring(workbook.read(sheet_path))

    rows: list[dict[str, str]] = []
    for row in sheet_root.findall(".//m:sheetData/m:row", namespaces):
        values: dict[str, str] = {}
        for cell in row.findall("m:c", namespaces):
            column = re.match(r"[A-Z]+", cell.attrib["r"])
            value_node = cell.find("m:v", namespaces)
            value = "" if value_node is None else value_node.text or ""
            if cell.attrib.get("t") == "s" and value:
                value = shared_strings[int(value)]
            values[column.group() if column else ""] = value.strip()
        if values.get("B") and values.get("B") != "Occupation Code":
            rows.append(values)
    return rows


def build_rows(source_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    selected: dict[str, dict[str, str]] = {}
    for row in source_rows:
        code = row["B"].upper()
        current = selected.get(code)
        if current is None or (row.get("A") == "Valid" and current.get("A") != "Valid"):
            selected[code] = row

    return [
        {
            "occupation_code": code,
            "occupation_code_name": row.get("C", ""),
            "main_staff_group": row.get("D", ""),
            "broad_staff_group": row.get("E", ""),
            "detailed_staff_group": row.get("F", ""),
            "occupation_level": row.get("G", ""),
            "care_setting_or_specialty": row.get("H", ""),
            "is_currently_valid": row.get("A") == "Valid",
        }
        for code, row in sorted(selected.items())
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[3] / "seeds" / "nhs_occupation_codes.csv",
    )
    args = parser.parse_args()

    rows = build_rows(sheet_rows(read_source(args.source)))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} occupation codes to {args.output}")


if __name__ == "__main__":
    main()
