"""Checks for declared primary key validation and YAML edits."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

import yaml

MODULE_PATH = Path(__file__).parents[1] / 'primary_keys.py'
SPEC = importlib.util.spec_from_file_location('primary_keys', MODULE_PATH)
PK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PK)


def model(text: str) -> dict:
    return yaml.safe_load(text)['models'][0]


SINGLE = """
models:
  - name: fct_example
    description: One row per example.
    columns:
      - name: example_id
        data_tests:
          - unique
          - not_null
      - name: label
"""

COMPOSITE = """
models:
  - name: fct_example
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          arguments:
            combination_of_columns: [person_id, reporting_date]
    columns:
      - name: person_id
        data_tests: [not_null]
      - name: reporting_date
        data_tests: [not_null]
"""


class EnforcementTest(unittest.TestCase):
    def test_single_and_composite_keys_are_candidates(self):
        self.assertEqual(PK.candidate_keys(model(SINGLE)), [['example_id']])
        self.assertEqual(PK.candidate_keys(model(COMPOSITE)), [['person_id', 'reporting_date']])

    def test_tests_that_tolerate_failures_do_not_enforce(self):
        for config in ('severity: warn', 'where: "label is not null"', 'error_if: ">100"', 'enabled: false'):
            text = SINGLE.replace('          - unique\n', f'          - unique:\n              config:\n'
                                  f'                {config}\n')
            self.assertEqual(PK.candidate_keys(model(text)), [], config)

    def test_composite_needs_not_null_on_every_column(self):
        text = COMPOSITE.replace('        data_tests: [not_null]\n      - name: reporting_date', '      - name: reporting_date', 1)
        self.assertEqual(PK.candidate_keys(model(text)), [])

    def test_declared_key_must_match_tests(self):
        declared = SINGLE.replace('    description: One row per example.\n',
                                  '    description: One row per example.\n    config:\n      meta:\n'
                                  '        primary_key: [example_id]\n')
        self.assertEqual(PK.check_model(model(declared)), [])
        wrong = declared.replace('[example_id]', '[label]')
        self.assertTrue(any('needs a not_null test' in e for e in PK.check_model(model(wrong))))
        composite = declared.replace('[example_id]', '[example_id, label]')
        self.assertTrue(any('unique_combination_of_columns' in e for e in PK.check_model(model(composite))))
        self.assertTrue(PK.check_model(model(declared.replace('[example_id]', 'example_id'))))


class DeclareKeyTest(unittest.TestCase):
    def declare(self, text: str, key: list[str]) -> str:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fct_example.yml'
            path.write_text(text, encoding='utf-8')
            self.assertIsNone(PK.declare_key(path, 'fct_example', key))
            updated = path.read_text(encoding='utf-8')
        self.assertEqual(PK.declared_key(model(updated)), key)
        return updated

    def test_adds_config_before_columns(self):
        updated = self.declare(SINGLE, ['example_id'])
        self.assertIn('    config:\n      meta:\n        primary_key: [example_id]\n    columns:', updated)

    def test_adds_to_existing_meta(self):
        text = SINGLE.replace('    columns:', '    config:\n      materialized: table\n      meta:\n'
                              '        owner:\n          name: Example team\n    columns:')
        updated = self.declare(text, ['example_id'])
        self.assertIn('      meta:\n        primary_key: [example_id]\n        owner:', updated)

    def test_adds_meta_to_existing_config(self):
        text = SINGLE.replace('    columns:', '    config:\n      materialized: table\n    columns:')
        updated = self.declare(text, ['example_id'])
        self.assertIn('    config:\n      meta:\n        primary_key: [example_id]\n      materialized: table', updated)

    def test_keeps_crlf_and_other_models(self):
        text = (SINGLE + '  - name: fct_other\n    description: Other.\n').replace('\n', '\r\n')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fct_example.yml'
            path.write_bytes(text.encode('utf-8'))
            self.assertIsNone(PK.declare_key(path, 'fct_example', ['example_id']))
            updated = path.read_bytes().decode('utf-8')
        self.assertNotIn('\n', updated.replace('\r\n', ''))
        self.assertIn('primary_key: [example_id]\r\n', updated)
        self.assertIn('fct_other', updated)

    def test_inline_meta_is_left_alone(self):
        text = SINGLE.replace('    columns:', '    config:\n      meta: {owner: {name: Example team}}\n    columns:')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fct_example.yml'
            path.write_text(text, encoding='utf-8')
            self.assertEqual(PK.declare_key(path, 'fct_example', ['example_id']), 'config.meta is written inline')
            self.assertEqual(path.read_text(encoding='utf-8'), text)


if __name__ == '__main__':
    unittest.main()
