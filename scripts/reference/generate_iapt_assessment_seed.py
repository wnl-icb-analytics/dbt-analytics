"""Extract public IAPT routine outcome measure definitions from TOS "ROM Mapping" sheets.

Only specification metadata is read. Continuation rows inherit the tool, the
concept and its precision. A guidance note that names a "not available" value
adds that value as an explicit non-score response.
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
    'is_non_score_response',
]
NOT_AVAILABLE = re.compile(r'value of ([0-9]+) indicates the score is not available', re.IGNORECASE)
NON_SCORE_LABELS = {'not applicable'}


def text(value):
    return '' if value is None else ' '.join(str(value).split())


def iso_date(value):
    return value.date().isoformat() if hasattr(value, 'date') else ''


def extract(path, version):
    book = openpyxl.load_workbook(path, data_only=True, read_only=True)
    started = False
    tool = start = concept = None
    for number, row in enumerate(book['ROM Mapping'].values, 1):
        tool_name, term, code, value, _, label, precision, guidance, collection = (list(row) + [None] * 9)[:9]
        if text(tool_name) == 'Assessment Tool Name':
            started = True
            continue
        if not started:
            continue
        if tool_name is not None:
            tool, start = text(tool_name), iso_date(collection)
        if code is not None:
            concept = (text(code), text(term), text(precision))
        if concept is None or value is None:
            continue
        code_text, description, decimals = concept
        label_text = text(label)
        base = [code_text, tool, description]
        tail = [decimals, start, version, number]
        yield dict(zip(FIELDS, base + [
            text(value), '' if label_text.lower() == 'score' else label_text,
        ] + tail + ['true' if label_text.lower() in NON_SCORE_LABELS else 'false']))
        match = NOT_AVAILABLE.search(text(guidance))
        if match:
            yield dict(zip(FIELDS, base + [match.group(1), 'Score not available'] + tail + ['true']))
    book.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spec', action='append', required=True, help='VERSION=path, for example 2.1.22=tos.xlsx')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for item in args.spec:
        version, path = item.split('=', 1)
        rows.extend(extract(Path(path), version))
    keys = {(r['concept_code'], r['published_value'], r['specification_version']) for r in rows}
    if len(keys) != len(rows):
        raise ValueError('Duplicate concept, response and version definitions')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (
            r['concept_code'], r['specification_version'], r['published_value'])))
    print(f'Extracted {len(rows)} definitions for {len({r["concept_code"] for r in rows})} concepts.')


if __name__ == '__main__':
    main()
