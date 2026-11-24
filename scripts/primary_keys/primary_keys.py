#!/usr/bin/env python3
"""Declared primary keys for dbt models: check, suggest and backfill.

A model declares its primary key in config.meta.primary_key, a list of column names.
Tests must enforce a declared key, failing the build on any duplicate or null:

- every key column has a not_null test;
- a one-column key has a unique test on that column;
- a composite key has a dbt_utils.unique_combination_of_columns test on exactly
  those columns.

A test enforces the key only when it is enabled, has error severity, no where filter
and the default error_if threshold (any failing row fails the build).

Commands:
    check                              Fail if any declared key is not enforced.
    suggest --base REF --output FILE   Write review suggestions for models changed
                                       since REF that have one enforced key.
    backfill [--dry-run] [PATH ...]    Declare the key on models with one enforced
                                       key. Re-runnable: declared models are skipped.
"""

import argparse
import copy
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

MODELS_DIR = Path('models')
RAW_DIR = MODELS_DIR / 'raw'
COMBINATION_TEST = 'unique_combination_of_columns'
DEFAULT_ERROR_IF = {'!=0', '>0'}
CONFIG_KEYS = {
    'severity', 'where', 'error_if', 'warn_if', 'enabled', 'tags', 'store_failures',
    'store_failures_as', 'limit', 'meta', 'alias', 'database', 'schema', 'fail_calc',
}
NON_ARGUMENT_KEYS = CONFIG_KEYS | {'config', 'arguments', 'name', 'test_name', 'description'}
ENFORCING = 'enabled, error severity, no where filter and the default error_if'


# --- Reading model YAML ---------------------------------------------------------

def model_yaml_files() -> list[Path]:
    """Every model YAML file outside the generated raw layer."""
    files = list(MODELS_DIR.rglob('*.yml')) + list(MODELS_DIR.rglob('*.yaml'))
    return sorted(f for f in files if RAW_DIR not in f.parents)


def load_models(path: Path) -> list[dict]:
    """Model entries in a YAML file, or [] when it has none or does not parse."""
    try:
        content = yaml.safe_load(path.read_text(encoding='utf-8')) or {}
    except (yaml.YAMLError, UnicodeDecodeError):
        return []
    models = content.get('models') if isinstance(content, dict) else None
    return [m for m in models or [] if isinstance(m, dict) and m.get('name')]


def parse_test(test) -> tuple[str, dict, dict] | None:
    """Return (name, arguments, config) for a test entry, with any package prefix removed."""
    if isinstance(test, str):
        return test.rsplit('.', 1)[-1], {}, {}
    if not isinstance(test, dict) or not test:
        return None
    if 'test_name' in test:
        name, body = test['test_name'], test
    elif len(test) == 1:
        name, body = next(iter(test.items()))
        body = body or {}
    else:
        return None
    if not isinstance(name, str) or not isinstance(body, dict):
        return None
    config = {k: v for k, v in body.items() if k in CONFIG_KEYS}
    config.update(body.get('config') or {})
    arguments = {k: v for k, v in body.items() if k not in NON_ARGUMENT_KEYS}
    arguments.update(body.get('arguments') or {})
    return name.rsplit('.', 1)[-1], arguments, config


def is_enforcing(config: dict) -> bool:
    """True when a test fails the build on any failing row."""
    if config.get('enabled', True) not in (True, 'true', 'True'):
        return False
    if str(config.get('severity', 'error')).lower() != 'error':
        return False
    if config.get('where') or config.get('fail_calc'):
        return False
    error_if = config.get('error_if')
    return error_if is None or str(error_if).replace(' ', '') in DEFAULT_ERROR_IF


def tests_of(entry: dict) -> list:
    return list(entry.get('data_tests') or []) + list(entry.get('tests') or [])


def enforced_tests(model: dict) -> tuple[set[str], set[str], list[list[str]]]:
    """Columns with enforcing unique and not_null tests, and enforcing column combinations.

    Column names are compared in lower case, as Snowflake resolves unquoted names.
    """
    unique, not_null, combinations = set(), set(), []

    def record(name: str, column: str | None, arguments: dict) -> None:
        if name == 'unique' and column:
            unique.add(column.lower())
        elif name == 'not_null' and column:
            not_null.add(column.lower())
        elif name == COMBINATION_TEST:
            columns = arguments.get('combination_of_columns') or []
            if isinstance(columns, list) and all(isinstance(c, str) for c in columns):
                combinations.append(columns)

    for column in model.get('columns') or []:
        if not isinstance(column, dict) or not isinstance(column.get('name'), str):
            continue
        for test in tests_of(column):
            parsed = parse_test(test)
            if parsed and is_enforcing(parsed[2]):
                record(parsed[0], column['name'], parsed[1])
    for test in tests_of(model):
        parsed = parse_test(test)
        if parsed and is_enforcing(parsed[2]):
            column = parsed[1].get('column_name')
            record(parsed[0], column if isinstance(column, str) else None, parsed[1])
    return unique, not_null, combinations


def candidate_keys(model: dict) -> list[list[str]]:
    """Keys the model's tests already enforce, in YAML order."""
    unique, not_null, combinations = enforced_tests(model)
    keys = []
    for column in model.get('columns') or []:
        name = column.get('name') if isinstance(column, dict) else None
        if isinstance(name, str) and name.lower() in unique and name.lower() in not_null:
            keys.append([name])
    for columns in combinations:
        lowered = [c.lower() for c in columns]
        if len(set(lowered)) == len(lowered) and set(lowered) <= not_null:
            if not any({k.lower() for k in key} == set(lowered) for key in keys):
                keys.append(columns)
    return keys


def declared_key(model: dict):
    """The model's config.meta.primary_key, or None."""
    config = model.get('config') if isinstance(model.get('config'), dict) else {}
    meta = config.get('meta') if isinstance(config.get('meta'), dict) else {}
    return meta.get('primary_key')


def check_model(model: dict) -> list[str]:
    """Problems with a model's declared primary key; [] when none is declared or it is enforced."""
    errors = []
    top_meta = model.get('meta') if isinstance(model.get('meta'), dict) else {}
    if 'primary_key' in top_meta:
        errors.append('primary_key must be under config.meta, not top-level meta')
    key = declared_key(model)
    if key is None:
        return errors
    if not isinstance(key, list) or not key or not all(isinstance(c, str) and c for c in key):
        return errors + ['primary_key must be a non-empty list of column names']
    lowered = [c.lower() for c in key]
    if len(set(lowered)) != len(lowered):
        return errors + ['primary_key lists a column more than once']

    unique, not_null, combinations = enforced_tests(model)
    documented = {c['name'].lower() for c in model.get('columns') or []
                  if isinstance(c, dict) and isinstance(c.get('name'), str)}
    for column in key:
        if column.lower() not in documented:
            errors.append(f'{column} is not listed under columns')
        elif column.lower() not in not_null:
            errors.append(f'{column} needs a not_null test ({ENFORCING})')
    if len(key) == 1:
        if key[0].lower() not in unique:
            errors.append(f'{key[0]} needs a unique test ({ENFORCING})')
    elif not any({c.lower() for c in combo} == set(lowered) for combo in combinations):
        errors.append(
            f'needs a dbt_utils.{COMBINATION_TEST} test on exactly {", ".join(key)} ({ENFORCING})'
        )
    return errors


# --- Editing model YAML ---------------------------------------------------------

def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(' '))


def is_content(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith('#')


def block_end(lines: list[str], start: int, parent_indent: int, limit: int) -> int:
    """Index after the last line nested under lines[start]."""
    end = start + 1
    while end < limit and (not is_content(lines[end]) or indent_of(lines[end]) > parent_indent):
        end += 1
    while end > start + 1 and not is_content(lines[end - 1]):
        end -= 1
    return end


def child_indent(lines: list[str], start: int, end: int, parent_indent: int) -> int:
    for line in lines[start + 1:end]:
        if is_content(line) and indent_of(line) > parent_indent:
            return indent_of(line)
    return parent_indent + 2


def find_key(lines: list[str], start: int, end: int, indent: int, key: str) -> int | str | None:
    """Line index of a block mapping key at an indent, 'inline' when it has a value, or None."""
    pattern = re.compile(rf'^ {{{indent}}}{key}:(.*)$')
    for i in range(start, end):
        match = pattern.match(lines[i])
        if match:
            rest = match.group(1).split('#', 1)[0].strip()
            return 'inline' if rest else i
    return None


def model_block(lines: list[str], name: str) -> tuple[int, int, int] | None:
    """(name line, key indent, end) of the least-indented `- name: <name>` entry."""
    pattern = re.compile(rf'''^(\s*-\s+)name:\s*["']?{re.escape(name)}["']?\s*(#.*)?$''')
    matches = [(len(m.group(1)), i) for i, line in enumerate(lines) if (m := pattern.match(line))]
    if not matches:
        return None
    key_indent, start = min(matches)
    return start, key_indent, block_end(lines, start, indent_of(lines[start]), len(lines))


def plan_insert(lines: list[str], name: str, key: list[str]) -> tuple[int, str, list[str]] | str:
    """Where to add primary_key: (anchor line, 'after' or 'before', new lines), or a reason not to."""
    block = model_block(lines, name)
    if block is None:
        return 'model entry not found'
    start, key_indent, end = block
    entry = f'primary_key: {yaml.safe_dump(key, default_flow_style=True, width=10_000).strip()}'

    config = find_key(lines, start + 1, end, key_indent, 'config')
    if config == 'inline':
        return 'config is written inline'
    if config is not None:
        config_end = block_end(lines, config, key_indent, end)
        config_indent = child_indent(lines, config, config_end, key_indent)
        meta = find_key(lines, config + 1, config_end, config_indent, 'meta')
        if meta == 'inline':
            return 'config.meta is written inline'
        if meta is not None:
            meta_end = block_end(lines, meta, config_indent, config_end)
            meta_indent = child_indent(lines, meta, meta_end, config_indent)
            return meta, 'after', [' ' * meta_indent + entry]
        return config, 'after', [' ' * config_indent + 'meta:', ' ' * (config_indent + 2) + entry]

    new = [' ' * key_indent + 'config:', ' ' * (key_indent + 2) + 'meta:', ' ' * (key_indent + 4) + entry]
    columns = find_key(lines, start + 1, end, key_indent, 'columns')
    if isinstance(columns, int):
        return columns, 'before', new
    return end - 1, 'after', new


def apply_plan(lines: list[str], plan: tuple[int, str, list[str]]) -> list[str]:
    anchor, position, new = plan
    at = anchor + 1 if position == 'after' else anchor
    return lines[:at] + new + lines[at:]


def without_key(document, name: str, before_model: dict):
    """The parsed document with the named model's primary_key removed, as it was before."""
    document = copy.deepcopy(document)
    for model in document.get('models') or []:
        if isinstance(model, dict) and model.get('name') == name:
            model['config']['meta'].pop('primary_key')
            if 'meta' not in (before_model.get('config') or {}) and not model['config']['meta']:
                model['config'].pop('meta')
            if 'config' not in before_model and not model['config']:
                model.pop('config')
    return document


def declare_key(path: Path, name: str, key: list[str]) -> str | None:
    """Add primary_key to a model's YAML. Returns a reason when the file is left unchanged."""
    raw = path.read_bytes().decode('utf-8')
    newline = '\r\n' if '\r\n' in raw else '\n'
    lines = raw.split(newline)
    plan = plan_insert(lines, name, key)
    if isinstance(plan, str):
        return plan
    updated = newline.join(apply_plan(lines, plan))

    before, after = yaml.safe_load(raw), yaml.safe_load(updated)
    model_before = next(m for m in before['models'] if isinstance(m, dict) and m.get('name') == name)
    model_after = next(m for m in after['models'] if isinstance(m, dict) and m.get('name') == name)
    if declared_key(model_after) != key or without_key(after, name, model_before) != before:
        return 'edit did not round-trip; add it by hand'
    path.write_bytes(updated.encode('utf-8'))
    return None


def suggestion_text(lines: list[str], plan: tuple[int, str, list[str]]) -> str:
    anchor, position, new = plan
    return '\n'.join(([lines[anchor]] + new) if position == 'after' else (new + [lines[anchor]]))


# --- Commands -------------------------------------------------------------------

def model_line(path: Path, name: str) -> int:
    block = model_block(path.read_text(encoding='utf-8').splitlines(), name)
    return block[0] + 1 if block else 1


def command_check(_args) -> int:
    failures, declared = [], 0
    for path in model_yaml_files():
        for model in load_models(path):
            declared += declared_key(model) is not None
            for error in check_model(model):
                failures.append((path, model['name'], error))
    for path, name, error in failures:
        print(f'{path.as_posix()}: {name}: {error}')
        if os.environ.get('GITHUB_ACTIONS'):
            print(f'::error file={path.as_posix()},line={model_line(path, name)}::{name}: {error}')
    if failures:
        print(f'FAILED: {len(failures)} problem(s) with declared primary keys. '
              f'Declare only a key that unique and not_null tests enforce ({ENFORCING}).')
        return 1
    print(f'PASSED: {declared} declared primary key(s) enforced by tests.')
    return 0


def command_backfill(args) -> int:
    paths = [Path(p) for p in args.paths] if args.paths else model_yaml_files()
    counts = {'declared': 0, 'already declared': 0, 'no enforced key': 0, 'several enforced keys': 0}
    skipped = []
    for path in paths:
        for model in load_models(path):
            if declared_key(model) is not None:
                counts['already declared'] += 1
                continue
            keys = candidate_keys(model)
            if not keys:
                counts['no enforced key'] += 1
                continue
            if len(keys) > 1:
                counts['several enforced keys'] += 1
                skipped.append((path, model['name'], 'several enforced keys: '
                                + '; '.join(', '.join(k) for k in keys)))
                continue
            if args.dry_run:
                print(f'{path.as_posix()}: {model["name"]}: {keys[0]}')
                counts['declared'] += 1
                continue
            reason = declare_key(path, model['name'], keys[0])
            if reason:
                skipped.append((path, model['name'], reason))
            else:
                counts['declared'] += 1
    for path, name, reason in skipped:
        print(f'skipped {path.as_posix()}: {name}: {reason}')
    print(', '.join(f'{label}: {count}' for label, count in counts.items()))
    return 0


def git_lines(*args: str) -> list[str]:
    result = subprocess.run(['git', *args], capture_output=True, text=True, check=True)
    return [line for line in result.stdout.splitlines() if line.strip()]


def diff_line_ranges(base: str, path: Path) -> list[tuple[int, int]]:
    """New-file line ranges GitHub shows in the pull request diff for a file."""
    ranges = []
    for line in git_lines('diff', '-U3', f'{base}...HEAD', '--', path.as_posix()):
        match = re.match(r'^@@ -\S+ \+(\d+)(?:,(\d+))? @@', line)
        if match:
            start, count = int(match.group(1)), int(match.group(2) or 1)
            ranges.append((start, start + count - 1))
    return ranges


def command_suggest(args) -> int:
    changed = [Path(p) for p in git_lines('diff', '--name-only', '--diff-filter=AM',
                                          f'{args.base}...HEAD', '--', 'models')]
    changed = [p for p in changed if p.exists() and RAW_DIR not in p.parents]
    added_sql = {Path(p).stem for p in git_lines('diff', '--name-only', '--diff-filter=A',
                                                 f'{args.base}...HEAD', '--', 'models')
                 if p.endswith('.sql')}
    changed_names = {p.stem for p in changed if p.suffix == '.sql'}
    yaml_paths = {p for p in changed if p.suffix in ('.yml', '.yaml')}
    for sql in (p for p in changed if p.suffix == '.sql'):
        yaml_paths.update(sql.parent.glob('*.yml'))

    inline, summary = [], []
    for path in sorted(yaml_paths):
        in_changed_yaml = path in changed
        for model in load_models(path):
            name = model['name']
            if not (in_changed_yaml or name in changed_names) or declared_key(model) is not None:
                continue
            keys = candidate_keys(model)
            if not keys:
                continue
            if len(keys) > 1:
                options = ' or '.join(f'`[{", ".join(k)}]`' for k in keys)
                summary.append({'model': name, 'text': f'`{name}`: its tests enforce more than one key '
                                f'({options}). Declare the one that defines its grain.'})
                continue
            owner = ((model.get('config') or {}).get('meta') or {}).get('owner')
            if name in added_sql and not owner:
                print(f'{name}: waiting for owner metadata before suggesting a key')
                continue
            lines = path.read_text(encoding='utf-8').splitlines()
            plan = plan_insert(lines, name, keys[0])
            flow = f'[{", ".join(keys[0])}]'
            if isinstance(plan, str):
                summary.append({'model': name, 'text': f'`{name}`: add `primary_key: {flow}` under '
                                f'`config.meta` ({plan}).'})
                continue
            line = plan[0] + 1
            if any(start <= line <= end for start, end in diff_line_ranges(args.base, path)):
                inline.append({'model': name, 'file': path.as_posix(), 'line': line,
                               'suggestion': suggestion_text(lines, plan)})
            else:
                summary.append({'model': name, 'text': f'`{name}` (`{path.as_posix()}`): add '
                                f'`primary_key: {flow}` under `config.meta`.'})
    Path(args.output).write_text(json.dumps({'inline': inline, 'summary': summary}, indent=2),
                                 encoding='utf-8')
    print(f'{len(inline)} inline suggestion(s), {len(summary)} summary item(s)')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('check').set_defaults(run=command_check)
    suggest = commands.add_parser('suggest')
    suggest.add_argument('--base', required=True, help='Base ref, e.g. origin/main')
    suggest.add_argument('--output', required=True, help='JSON file for the workflow to post')
    suggest.set_defaults(run=command_suggest)
    backfill = commands.add_parser('backfill')
    backfill.add_argument('--dry-run', action='store_true', help='List keys without editing files')
    backfill.add_argument('paths', nargs='*', help='YAML files to edit (default: every model YAML)')
    backfill.set_defaults(run=command_backfill)
    args = parser.parse_args()
    return args.run(args)


if __name__ == '__main__':
    sys.exit(main())
