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

    def test_limit_zero_does_not_enforce(self):
        text = SINGLE.replace('          - unique\n', '          - unique:\n              config:\n'
                              '                limit: 0\n')
        self.assertEqual(PK.candidate_keys(model(text)), [])

    def test_composite_columns_must_be_documented(self):
        text = COMPOSITE.replace('    columns:\n', '    data_tests_extra: []\n    columns:\n').replace(
            '      - name: reporting_date\n        data_tests: [not_null]\n', '')
        text = text.replace('    data_tests:\n', '    data_tests:\n      - not_null:\n'
                            '          arguments:\n            column_name: reporting_date\n', 1)
        self.assertEqual(PK.candidate_keys(model(text)), [])

    def test_declared_spelling_and_tag_length(self):
        declared = SINGLE.replace('    columns:', '    config:\n      meta:\n        primary_key: [Example_ID]\n'
                                  '    columns:')
        self.assertTrue(any('must be spelled as under columns' in e for e in PK.check_model(model(declared))))
        long_name = 'c' * 130
        text = COMPOSITE.replace('person_id', long_name).replace('reporting_date', long_name + 'x')
        text = text.replace('    columns:', f'    config:\n      meta:\n        primary_key: [{long_name}, {long_name}x]\n'
                            '    columns:', 1)
        self.assertTrue(any('tag limit' in e for e in PK.check_model(model(text))))

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

    def test_inline_meta_is_rewritten_as_a_block(self):
        text = SINGLE.replace('    columns:', '    config:\n      meta: {owner: {name: Example team}}\n    columns:')
        updated = self.declare(text, ['example_id'])
        self.assertIn('      meta:\n        primary_key: [example_id]\n        owner:\n          name: Example team\n',
                      updated)

    def test_alias_is_expanded_and_shared_anchor_left_alone(self):
        text = (
            'models:\n  - name: dim_a\n    config: &owner\n      meta:\n        owner:\n          name: Team\n'
            '    columns:\n      - name: code\n  - name: fct_example\n    config: *owner\n    columns:\n'
            '      - name: example_id\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'shared.yml'
            path.write_bytes(text.encode('utf-8'))
            self.assertEqual(PK.declare_key(path, 'dim_a', ['code']), 'config is a YAML anchor other models share; add it by hand')
            self.assertIsNone(PK.declare_key(path, 'fct_example', ['example_id']))
            models = yaml.safe_load(path.read_bytes().decode('utf-8'))['models']
        self.assertIsNone(PK.declared_key(models[0]))
        self.assertEqual(PK.declared_key(models[1]), ['example_id'])
        self.assertEqual(models[1]['config']['meta']['owner'], {'name': 'Team'})

    def test_inline_anchor_with_aliases_is_skipped(self):
        text = (
            'models:\n  - name: fct_example\n    config: &shared {meta: {owner: {name: Team}}}\n    columns:\n'
            '      - name: example_id\n  - name: dim_b\n    config: *shared\n'
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'shared.yml'
            path.write_bytes(text.encode('utf-8'))
            reason = PK.declare_key(path, 'fct_example', ['example_id'])
            self.assertEqual(reason, 'edit would break YAML anchors or aliases; add it by hand')
            self.assertEqual(path.read_bytes().decode('utf-8'), text)

    def test_mixed_line_endings_are_kept(self):
        text = SINGLE.replace('\n', '\r\n', 3)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fct_example.yml'
            path.write_bytes(text.encode('utf-8'))
            self.assertIsNone(PK.declare_key(path, 'fct_example', ['example_id']))
            updated = path.read_bytes().decode('utf-8')
        self.assertEqual(updated.replace('    config:\n      meta:\n        primary_key: [example_id]\n', ''), text)


if __name__ == '__main__':
    unittest.main()
