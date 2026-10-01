#!/usr/bin/env bash
# Check raw and source references follow the project layer boundary.
# Only checks files passed as arguments (typically changed files in a PR).
# Staging models may reference raw_* models but must not use source(). Models
# outside models/staging/ and models/raw/ may use neither.

set -euo pipefail

if [[ $# -eq 0 ]]; then
    echo "PASSED: No files to check."
    exit 0
fi

failed=0
declare -A file_issues

for file in "$@"; do
    [[ "$file" != *.sql ]] && continue
    [[ ! -f "$file" ]] && continue
    [[ "$file" == *dbt_packages* ]] && continue

    # Generated raw models are the only models allowed to use source().
    [[ "$file" == */raw/* ]] && continue

    # Only check files in models/
    [[ "$file" != models/* ]] && continue

    issues=""

    # Staging may ref() generated raw models; other layers may not.
    if [[ "$file" != */staging/* ]] && grep -qE '\{\{\s*ref\s*\(\s*['"'"'"]raw_' "$file"; then
        issues+="  - References raw_* model directly"$'\n'
    fi

    # Check for source() usage
    if grep -qE '\{\{\s*source\s*\(' "$file"; then
        issues+="  - Uses source() directly"$'\n'
    fi

    if [[ -n "$issues" ]]; then
        file_issues["$file"]="$issues"
        failed=1
    fi
done

if [[ $failed -eq 1 ]]; then
    echo "FAILED: Found invalid raw/source references:"
    echo ""
    for file in "${!file_issues[@]}"; do
        echo "$file:"
        echo -n "${file_issues[$file]}"
        echo ""
    done
    echo "Staging models must ref() generated raw models, not use source()."
    echo "Other models must reference staging-or-later models."
    echo "See: https://docs.getdbt.com/best-practices/how-we-structure/2-staging"
    exit 1
fi

echo "PASSED: All raw/source references follow the layer boundary."
exit 0
