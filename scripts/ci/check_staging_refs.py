#!/usr/bin/env python3
"""Check raw and source references follow the project layer boundary.

Only checks files passed as arguments (typically changed files in a PR).
Staging models may reference raw_* models but must not use source(). Models
outside models/staging/ and models/raw/ may use neither.
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
    if len(sys.argv) < 2:
        print("PASSED: No files to check.")
        return 0

    files = [Path(f) for f in sys.argv[1:] if f.endswith('.sql')]
    all_issues: dict[str, list[str]] = {}

    for filepath in files:
        if not filepath.exists():
            continue
        if 'dbt_packages' in str(filepath):
            continue

        # Generated raw models are the only models allowed to use source().
        path_str = str(filepath).replace('\\', '/')
        if '/raw/' in path_str:
            continue

        # Only check files in models/
        if not path_str.startswith('models/'):
            continue

        issues = check_file(filepath, is_staging='/staging/' in path_str)
        if issues:
            all_issues[str(filepath)] = issues

    if all_issues:
        print("FAILED: Found invalid raw/source references:\n")
        for filepath, issues in sorted(all_issues.items()):
            print(f"{filepath}:")
            for issue in issues:
                print(f"  - {issue}")
            print()
        print("Staging models must ref() generated raw models, not use source().")
        print("Other models must reference staging-or-later models.")
        print("See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging")
        return 1

    print("PASSED: All raw/source references follow the layer boundary.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
