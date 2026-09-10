#!/usr/bin/env python3
"""Audit entire project for models missing tests.

Scans all .sql files in models/ and checks for tests in YAML files.
Use this to identify existing issues at scale.
"""

import sys
from pathlib import Path

import yaml


def find_yaml_files(model_path: Path) -> list[Path]:
    """Find YAML files that might contain the model definition."""
    model_dir = model_path.parent
    yaml_files = []

    yaml_files.extend(model_dir.glob('*.yml'))
    yaml_files.extend(model_dir.glob('*.yaml'))

    if model_dir.parent.exists():
        yaml_files.extend(model_dir.parent.glob('*.yml'))
        yaml_files.extend(model_dir.parent.glob('*.yaml'))

    return yaml_files


def model_has_tests(model_name: str, yaml_files: list[Path]) -> bool:
    """Check if model has at least one test in any of the YAML files."""
    for yaml_file in yaml_files:
        try:
            content = yaml.safe_load(yaml_file.read_text(encoding='utf-8'))
            if not content or 'models' not in content:
                continue

            for model in content.get('models', []):
                if model.get('name') == model_name:
                    if model.get('tests'):
                        return True
                    if model.get('data_tests'):
                        return True

                    for column in model.get('columns', []):
                        if column.get('tests'):
                            return True
                        if column.get('data_tests'):
                            return True

        except (yaml.YAMLError, Exception):
            continue

    return False


def main() -> int:
    root = Path('.')
    models_dir = root / 'models'
    missing_tests: list[str] = []
    files_checked = 0

    if not models_dir.exists():
        print("No models directory found.")
        return 0

    for sql_file in models_dir.rglob('*.sql'):
        if 'dbt_packages' in str(sql_file):
            continue

        # Match check_model_tests.sh: raw models do not need tests, and Snowflake
        # semantic views cannot be queried by ordinary SELECT-based data tests.
        path_str = str(sql_file).replace('\\', '/')
        if '/raw/' in path_str or path_str.startswith('models/semantic/'):
            continue

        files_checked += 1
        model_name = sql_file.stem
        yaml_files = find_yaml_files(sql_file)

        if not model_has_tests(model_name, yaml_files):
            missing_tests.append(str(sql_file))

    print(f"Scanned {files_checked} model files\n")

    if missing_tests:
        print(f"FOUND {len(missing_tests)} models missing tests:\n")
        for filepath in sorted(missing_tests):
            print(f"  - {filepath}")
        print("\nAdd at least one test for each model in a corresponding .yml file.")
        print("See: https://docs.getdbt.com/docs/build/data-tests")
        return 1

    print("All models have tests.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
