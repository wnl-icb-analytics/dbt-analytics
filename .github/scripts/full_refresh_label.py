"""Report whether a pull request in a commit range has the full-refresh label.

Squash merges title each commit "<pull request title> (#N)", so a merge-queue
candidate or a push to main names its pull requests in commit subjects. Writes
full_refresh=true/false to GITHUB_OUTPUT. A failed label lookup fails the step
rather than building without the refresh the pull request asked for.
"""

import argparse
import os
import re
import subprocess

LABEL = "🔄full-refresh"
PULL_NUMBER = re.compile(r"\(#(\d+)\)$")


def pull_numbers(subjects):
    matches = (PULL_NUMBER.search(s.strip()) for s in subjects)
    return sorted({int(m.group(1)) for m in matches if m})


def labels(number):
    result = subprocess.run(
        ["gh", "api", "--paginate",
         f"repos/{os.environ['GITHUB_REPOSITORY']}/issues/{number}/labels",
         "--jq", ".[].name"],
        capture_output=True, text=True, encoding="utf-8", check=True,
    )
    return result.stdout.splitlines()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("range", help="git revision range, e.g. BASE..HEAD")
    args = parser.parse_args()
    subjects = subprocess.run(
        ["git", "log", "--format=%s", args.range],
        capture_output=True, text=True, encoding="utf-8", check=True,
    ).stdout.splitlines()
    numbers = pull_numbers(subjects)
    labelled = [n for n in numbers if LABEL in labels(n)]
    print(f"Pull requests in {args.range}: {numbers or 'none'}")
    if labelled:
        print(f"{LABEL} on #{', #'.join(map(str, labelled))}: building with --full-refresh")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as f:
        f.write(f"full_refresh={'true' if labelled else 'false'}\n")


if __name__ == "__main__":
    main()
