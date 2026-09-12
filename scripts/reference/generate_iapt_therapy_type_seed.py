"""Extract the IAPT therapy type mapping from the terminology mapping guidance workbook.

Reads the "Therapy Types" sheet: v1.5 national therapy codes and their
SNOMED CT concepts for IDS202 Coded Procedure, grouped by intensity.
"""

import argparse
import csv
import re
from pathlib import Path

import openpyxl


FIELDS = [
    'snomed_code', 'snomed_preferred_term', 'therapy_type_category',
    'legacy_therapy_type_code', 'legacy_therapy_type_description', 'source_row',
]


def text(value):
    return '' if value is None else ' '.join(str(value).split())


def extract(path):
    book = openpyxl.load_workbook(path, data_only=True, read_only=True)
    started = False
    category = None
    for number, row in enumerate(book['Therapy Types'].values, 1):
        group, legacy_code, legacy_name, concept, term = (list(row) + [None] * 5)[:5]
        if text(group) == 'Category':
            started = True
            continue
        if not started:
            continue
        if text(legacy_code).startswith('The following Therapy Types are no longer required'):
            break
        if group is not None:
            category = text(group)
        if not re.fullmatch(r'[0-9]{6,18}', text(concept)):
            continue
        code = text(legacy_code)
        yield dict(zip(FIELDS, [
            text(concept), text(term), category,
            code if re.fullmatch(r'[0-9]+', code) else '', text(legacy_name), number,
        ]))
    book.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workbook', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = list(extract(args.workbook))
    if len({r['snomed_code'] for r in rows}) != len(rows):
        raise ValueError('Duplicate SNOMED CT therapy concepts')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f'Extracted {len(rows)} therapy type concepts.')


if __name__ == '__main__':
    main()
