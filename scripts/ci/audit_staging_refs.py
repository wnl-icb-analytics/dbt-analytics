#!/usr/bin/env python3
"""Audit the project for raw/source references that cross layer boundaries.

Scans all .sql files in models/ except generated raw models.
Use this to identify existing issues at scale.
"""

import re
import sys
from pathlib import Path

RAW_REF_PATTERN = r"{{\s*ref\s*\(\s*['\"]raw_"
SOURCE_PATTERN = r"{{\s*source\s*\("


def check_file(filepath: Path, *, is_staging: bool) -> list[str]:
    """Check a single file for raw/source references."""
    issues = []
    content = filepath.read_text(encoding='utf-8', errors='ignore')

    if not is_staging and re.search(RAW_REF_PATTERN, content):
        issues.append("References raw_* model directly")

    if re.search(SOURCE_PATTERN, content):
        issues.append("Uses source() directly")

    return issues


def main() -> int:
    root = Path('.')
    models_dir = root / 'models'
    all_issues: dict[str, list[str]] = {}
    files_checked = 0

    if not models_dir.exists():
        print("No models directory found.")
        return 0

    for sql_file in models_dir.rglob('*.sql'):
        if 'dbt_packages' in str(sql_file):
            continue

        path_str = str(sql_file).replace('\\', '/')
        if '/raw/' in path_str:
            continue

        files_checked += 1
        issues = check_file(sql_file, is_staging='/staging/' in path_str)
        if issues:
            all_issues[str(sql_file)] = issues

    print(f"Scanned {files_checked} model files (excluding raw/)\n")

    if all_issues:
        print(f"FOUND {len(all_issues)} files with raw/source references:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(f"  - {issue}")
            print()
        print("Staging models must ref() generated raw models, not use source().")
        print("Other models must reference staging-or-later models.")
        print("See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging")
        return 1

    print("All raw/source references follow the layer boundary.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
