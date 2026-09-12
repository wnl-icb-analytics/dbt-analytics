"""Extract IAPT therapy type concepts for IDS202 Coded Procedure.

Reads the "Therapy Types" sheet of the terminology mapping guidance: v1.5
national codes and their SNOMED CT concepts, grouped by intensity. Then adds
concepts that the ETOS counts as national therapies but the guidance omits,
from Complex Derivations clauses [8] (employment support), [9] (high
intensity) and [10] (low intensity). An added concept takes a v1.5 code only
when an IDS101 therapy count derivation counts it with a single guidance
concept, as the CBT count does for 228557008.
"""

import argparse
import csv
import re
from pathlib import Path

import openpyxl


FIELDS = [
    'snomed_code', 'source_term', 'therapy_type_category',
    'legacy_therapy_type_code', 'legacy_therapy_type_description',
    'source_document', 'source_sheet', 'source_row', 'source_reference',
    'legacy_code_source',
]
CLAUSES = {'[8]': 'Employment Support', '[9]': 'High Intensity', '[10]': 'Low Intensity'}
# The lookbehind skips data item UIDs such as I202110|CodeProcAndProcStatus.
CONCEPT = re.compile(r'(?<![A-Za-z0-9])([0-9]{6,18}) *\|([^|]*)\|')


def text(value):
    return '' if value is None else ' '.join(str(value).split())


def versioned_path(value):
    version, separator, path = value.partition('=')
    if not separator or not version or not path:
        raise argparse.ArgumentTypeError('Use VERSION=path')
    return version, Path(path)


def guidance_rows(path, version):
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
            code if re.fullmatch(r'[0-9]+', code) else '', text(legacy_name),
            f'IAPT v2 terminology mapping guidance v{version}', 'Therapy Types', number, '', '',
        ]))
    book.close()


def etos_clause_concepts(book):
    """(code, printed term, category, row, clause) from the therapy clauses."""
    for number, row in enumerate(book['Complex Derivations'].values, 1):
        cell = next((c for c in row if isinstance(c, str) and '[9] I202110|CodeProcAndProcStatus' in c), None)
        if cell is None:
            continue
        for line in cell.splitlines():
            clause = line.strip().split(' ', 1)[0]
            if clause in CLAUSES and 'CodeProcAndProcStatus =' in line:
                for code, term in CONCEPT.findall(line):
                    yield code, text(term), CLAUSES[clause], number, clause
        return
    raise ValueError('Therapy clauses not found in Complex Derivations')


def etos_count_derivations(book):
    """(row, uid, name, codes) for IDS101 count derivations that name procedure concepts."""
    for number, row in enumerate(book['IDS101Referral'].values, 1):
        cells = [text(c) for c in row]
        uid = next((i for i, c in enumerate(cells) if re.fullmatch(r'I101D[0-9]+', c)), None)
        if uid is None or not cells[uid + 1].endswith('COUNT'):
            continue
        codes = set(re.findall(r'CodeProcAndProcStatus *= *([0-9]{6,18})', ' '.join(cells)))
        if codes:
            yield number, cells[uid], cells[uid + 1], codes


def etos_rows(path, version, guidance):
    book = openpyxl.load_workbook(path, data_only=True, read_only=True)
    counts = list(etos_count_derivations(book))
    for code, term, category, number, clause in etos_clause_concepts(book):
        if code in guidance:
            if guidance[code]['therapy_type_category'] != category:
                raise ValueError(f'{code}: guidance and ETOS {clause} disagree on category')
            continue
        legacy = {'legacy_therapy_type_code': '', 'legacy_therapy_type_description': '', 'legacy_code_source': ''}
        for count_row, uid, name, codes in counts:
            peers = [guidance[c] for c in codes if c in guidance]
            if code in codes and len({p['legacy_therapy_type_code'] for p in peers}) == 1 and peers[0]['legacy_therapy_type_code']:
                legacy = {
                    'legacy_therapy_type_code': peers[0]['legacy_therapy_type_code'],
                    'legacy_therapy_type_description': peers[0]['legacy_therapy_type_description'],
                    'legacy_code_source': f'ETOS v{version} IDS101Referral row {count_row} {uid} {name}',
                }
        yield {
            'snomed_code': code, 'source_term': term, 'therapy_type_category': category,
            'source_document': f'IAPT v2.1 ETOS v{version}', 'source_sheet': 'Complex Derivations',
            'source_row': number, 'source_reference': f'Clause {clause}', **legacy,
        }
    book.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--guidance', type=versioned_path, required=True, help='VERSION=terminology mapping guidance workbook')
    parser.add_argument('--etos', type=versioned_path, required=True, help='VERSION=IAPT v2.1 ETOS workbook')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = list(guidance_rows(args.guidance[1], args.guidance[0]))
    if len({r['snomed_code'] for r in rows}) != len(rows):
        raise ValueError('Duplicate SNOMED CT therapy concepts in the guidance')
    guidance = {r['snomed_code']: r for r in rows}
    added = list(etos_rows(args.etos[1], args.etos[0], guidance))
    if len({r['snomed_code'] for r in added}) != len(added):
        raise ValueError('Duplicate SNOMED CT therapy concepts in the ETOS clauses')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows + added)
    print(f'Extracted {len(rows)} guidance and {len(added)} ETOS-only therapy type concepts.')


if __name__ == '__main__':
    main()
