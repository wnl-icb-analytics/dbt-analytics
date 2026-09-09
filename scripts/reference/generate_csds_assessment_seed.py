"""Extract public CSDS assessment definitions from the ETOS v1.6.10 workbook.

Only specification metadata is read. Continuation rows inherit the concept
and precision until the next named concept or section heading.
"""

import argparse
import csv
import re
from pathlib import Path

import openpyxl


FIELDS = [
    'concept_code', 'assessment_tool_name', 'assessment_description',
    'published_value', 'response_description', 'decimal_places',
    'collection_start_date', 'specification_version', 'source_row',
]


def text(value):
    return '' if value is None else ' '.join(str(value).split())


def extract(path):
    book = openpyxl.load_workbook(path, data_only=True, read_only=True)
    context = None
    for number, row in enumerate(book['CSDS Assessment Tools'].values, 1):
        tool, description, concept, _, value, label, precision, start, _ = row
        if re.fullmatch(r'[0-9]{6,18}', text(concept)):
            context = (text(concept), text(tool), text(description), text(precision), start)
        elif tool is not None or concept is not None:
            context = None
        if context is None or value is None:
            continue
        code, tool_name, concept_description, decimals, collection_start = context
        yield dict(zip(FIELDS, [
            code, tool_name, concept_description, text(value), text(label), decimals,
            collection_start.date().isoformat() if hasattr(collection_start, 'date') else '',
            '1.6.10', number,
        ]))
    book.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = list(extract(args.spec))
    keys = {(r['concept_code'], r['published_value']) for r in rows}
    if len(keys) != len(rows):
        raise ValueError('Duplicate concept and response definitions')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r['concept_code'], r['published_value'])))
    print(f'Extracted {len(rows)} public definitions for {len({r["concept_code"] for r in rows})} concepts.')


if __name__ == '__main__':
    main()
