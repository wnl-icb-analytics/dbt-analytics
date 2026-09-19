#!/usr/bin/env python3
"""Check that clusters and value sets a model reads are covered by existence tests.

A model that calls get_observations, get_medication_orders, get_medication_statements
or get_lipid_observations with a cluster ID must list that ID in a cluster_ids_exist
test, passing the same source and versioned arguments as the call. A model that calls
a get_ltc_lcs_* macro must list each value set token in a valuesets_have_codes test.

Checks the models passed as arguments (changed .sql or .yml files under models/).
With --all, checks every model. Calls whose cluster argument is not a string literal
cannot be checked statically and are reported as notices.
"""

import re
import sys
from pathlib import Path

import yaml

CLUSTER_MACROS = {
    'get_observations',
    'get_medication_orders',
    'get_medication_statements',
    'get_lipid_observations',
}
VALUESET_MACROS = {
    'get_ltc_lcs_observations',
    'get_ltc_lcs_observations_latest',
    'get_ltc_lcs_medication_orders',
    'get_ltc_lcs_medication_orders_latest',
    'get_ltc_lcs_medication_statements',
    'get_ltc_lcs_medication_statements_latest',
}
CALL_PATTERN = re.compile(r'\b(get_[a-z_]+)\s*\(')
JINJA_BLOCK_PATTERN = re.compile(r'{{.*?}}|{%.*?%}', re.DOTALL)
TOKEN_PATTERN = re.compile(r'[A-Za-z0-9_\-]+')


def read_call_args(text: str, start: int) -> str:
    """Return the text between the parenthesis at text[start] and its match."""
    depth, quote, i = 0, None, start
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == quote:
                quote = None
        elif ch in '\'"':
            quote = ch
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                return text[start + 1:i]
        i += 1
    return text[start + 1:]


def split_args(args: str) -> tuple[list[str], dict[str, str]]:
    """Split a macro argument string into positional and keyword arguments."""
    parts, depth, quote, current = [], 0, None, ''
    for ch in args:
        if quote:
            if ch == quote:
                quote = None
        elif ch in '\'"':
            quote = ch
        elif ch in '([{':
            depth += 1
        elif ch in ')]}':
            depth -= 1
        elif ch == ',' and depth == 0:
            parts.append(current.strip())
            current = ''
            continue
        current += ch
    if current.strip():
        parts.append(current.strip())

    positional, keyword = [], {}
    for part in parts:
        match = re.match(r'^([a-z_]+)\s*=\s*(.+)$', part, re.DOTALL)
        if match:
            keyword[match.group(1)] = match.group(2).strip()
        else:
            positional.append(part)
    return positional, keyword


def string_literal(value: str | None) -> str | None:
    """Return the content of a quoted string literal, or None if not a literal."""
    if value is None:
        return None
    match = re.fullmatch(r'"(.*)"|\'(.*)\'', value.strip(), re.DOTALL)
    if not match:
        return None
    return match.group(1) if match.group(1) is not None else match.group(2)


def tokens(literal: str) -> list[str]:
    return [t.upper() for t in TOKEN_PATTERN.findall(literal)]


def find_usages(sql: str) -> tuple[set[tuple[str, str | None, bool]], set[str], list[str]]:
    """Return (cluster, source, versioned) usages, value set tokens and dynamic calls."""
    sql = re.sub(r'{#.*?#}', '', sql, flags=re.DOTALL)
    # Only calls inside Jinja blocks run; mentions in SQL comments or text do not
    jinja = '\n'.join(m.group(0) for m in JINJA_BLOCK_PATTERN.finditer(sql))
    clusters, valuesets, dynamic = set(), set(), []

    for match in CALL_PATTERN.finditer(jinja):
        macro = match.group(1)
        if macro not in CLUSTER_MACROS and macro not in VALUESET_MACROS:
            continue
        args = read_call_args(jinja, match.end() - 1)
        if not args.strip():
            continue
        positional, keyword = split_args(args)

        if macro in VALUESET_MACROS:
            raw = keyword.get('valuesets', positional[0] if positional else None)
            literal = string_literal(raw)
            if literal is None:
                dynamic.append(f'{macro}({args.strip()[:60]})')
            else:
                valuesets.update(tokens(literal))
            continue

        if macro == 'get_observations':
            raw = keyword.get('cluster_ids', positional[0] if positional else None)
            source = keyword.get('source', positional[1] if len(positional) > 1 else None)
        elif macro == 'get_lipid_observations':
            raw = keyword.get('cluster_id', positional[0] if positional else None)
            source = None
        else:
            raw = keyword.get('cluster_id', positional[1] if len(positional) > 1 else None)
            source = keyword.get('source', positional[2] if len(positional) > 2 else None)
            if raw is None or raw.strip().lower() == 'none':
                continue  # BNF-only call

        literal = string_literal(raw)
        source_literal = string_literal(source) if source and source.strip().lower() != 'none' else None
        if literal is None or (source and source.strip().lower() != 'none' and source_literal is None):
            dynamic.append(f'{macro}({args.strip()[:60]})')
            continue
        versioned = keyword.get('versioned', 'false').strip().lower() == 'true'
        for cluster in tokens(literal):
            clusters.add((cluster, source_literal, versioned))

    return clusters, valuesets, dynamic


def find_yaml_files(model_path: Path) -> list[Path]:
    """Find YAML files that might contain the model definition."""
    model_dir = model_path.parent
    yaml_files = list(model_dir.glob('*.yml')) + list(model_dir.glob('*.yaml'))
    if model_dir.parent.exists():
        yaml_files += list(model_dir.parent.glob('*.yml')) + list(model_dir.parent.glob('*.yaml'))
    return yaml_files


def model_tests(model_name: str, yaml_files: list[Path]) -> list[dict]:
    """Return generic test definitions attached to the model or its columns."""
    for yaml_file in yaml_files:
        try:
            content = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
        except yaml.YAMLError:
            continue
        if not content or 'models' not in content:
            continue
        for model in content.get('models') or []:
            if model.get('name') != model_name:
                continue
            found = list(model.get('data_tests') or []) + list(model.get('tests') or [])
            for column in model.get('columns') or []:
                found += list(column.get('data_tests') or []) + list(column.get('tests') or [])
            return [t for t in found if isinstance(t, dict)]
    return []


def test_arguments(test: dict, name: str) -> dict | None:
    """Return the arguments of a test with the given name, or None."""
    for key, value in test.items():
        if key.split('.')[-1] == name:
            value = value or {}
            return {**value, **(value.get('arguments') or {})}
    return None


def as_tokens(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, list):
        return {str(v).strip().strip('\'"').upper() for v in value}
    return set(tokens(str(value)))


def check_model(sql_path: Path) -> tuple[list[str], list[str]]:
    """Return (failures, notices) for one model."""
    clusters, valuesets, dynamic = find_usages(sql_path.read_text(encoding='utf-8', errors='ignore'))
    notices = [f'{sql_path}: cannot check dynamic call {call}' for call in dynamic]
    if not clusters and not valuesets:
        return [], notices

    tests = model_tests(sql_path.stem, find_yaml_files(sql_path))
    cluster_tests = [a for t in tests if (a := test_arguments(t, 'cluster_ids_exist')) is not None]
    valueset_tests = [a for t in tests if (a := test_arguments(t, 'valuesets_have_codes')) is not None]

    failures = []
    for cluster, source, versioned in sorted(clusters, key=lambda c: (c[0], c[1] or '')):
        covered = any(
            cluster in as_tokens(t.get('cluster_ids'))
            and (source is None or str(t.get('source') or '').upper() == source.upper())
            and bool(t.get('versioned', False)) == versioned
            for t in cluster_tests
        )
        if not covered:
            detail = ', '.join(filter(None, [f'source: {source}' if source else '', 'versioned: true' if versioned else '']))
            failures.append(f'{sql_path}: cluster {cluster}' + (f' ({detail})' if detail else ''))

    tested_valuesets = set().union(*(as_tokens(t.get('valuesets')) for t in valueset_tests)) if valueset_tests else set()
    for valueset in sorted(valuesets - tested_valuesets):
        failures.append(f'{sql_path}: value set {valueset}')

    return failures, notices


def models_for(paths: list[str]) -> set[Path]:
    """Map changed .sql and .yml paths to model SQL files."""
    models = set()
    for p in paths:
        path = Path(p)
        if not str(path).replace('\\', '/').startswith('models/') or '/raw/' in str(path).replace('\\', '/'):
            continue
        if path.suffix == '.sql' and path.exists():
            models.add(path)
        elif path.suffix in ('.yml', '.yaml') and path.exists():
            try:
                content = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
            except yaml.YAMLError:
                continue
            for model in content.get('models') or []:
                for candidate in (path.parent / f"{model.get('name')}.sql", *path.parent.glob(f"*/{model.get('name')}.sql")):
                    if candidate.exists():
                        models.add(candidate)
    return models


def main() -> int:
    if '--all' in sys.argv:
        models = {p for p in Path('models').rglob('*.sql') if '/raw/' not in str(p).replace('\\', '/')}
    else:
        models = models_for(sys.argv[1:])
    if not models:
        print('PASSED: No models to check.')
        return 0

    failures, notices = [], []
    for sql_path in sorted(models):
        f, n = check_model(sql_path)
        failures += f
        notices += n

    for notice in notices:
        print(f'NOTICE: {notice}')
    if failures:
        print('FAILED: Clusters or value sets without an existence test:\n')
        for failure in failures:
            print(f'  - {failure}')
        print(
            '\nAdd a cluster_ids_exist test listing each cluster (with the same source and versioned'
            ' arguments as the macro call), or a valuesets_have_codes test listing each value set.'
        )
        return 1

    print('PASSED: All clusters and value sets are covered by existence tests.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
