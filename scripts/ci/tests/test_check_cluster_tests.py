"""Regression checks for the shared evidence-list existence tests."""

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).parents[1] / 'check_cluster_tests.py'
SPEC = importlib.util.spec_from_file_location('cluster_tests', MODULE_PATH)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


class EvidenceListsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.macro = self.root / CHECKER.EVIDENCE_MACRO
        self.macro.parent.mkdir(parents=True)
        self.macro.write_text("{% set lists = {'review': {'PCD': ['SYN_A']}} %}", encoding='utf-8')
        self.root_patch = patch.object(CHECKER, 'PROJECT_ROOT', self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.sql = self.root / 'review.sql'
        self.sql.write_text(
            "{{ get_observations(observation_evidence_cluster_ids('review', 'PCD'), source='PCD') }}",
            encoding='utf-8',
        )
        self.yaml = self.root / 'review.yml'
        self.yaml.write_text(
            'models:\n- name: review\n  data_tests:\n  - cluster_ids_exist:\n'
            '      arguments:\n        cluster_ids: SYN_A\n        source: PCD\n',
            encoding='utf-8',
        )

    def test_literal_consumer_source_and_versioned_are_checked(self):
        self.assertEqual(CHECKER.check_model(self.sql), ([], []))
        self.sql.write_text(self.sql.read_text().replace("source='PCD'", "source='PCD', versioned=true"))
        failures, notices = CHECKER.check_model(self.sql)
        self.assertIn('versioned: true', failures[0])
        self.assertEqual(notices, [])

    def test_macro_addition_requires_a_matching_test(self):
        self.macro.write_text("{% set lists = {'review': {'PCD': ['SYN_A', 'SYN_B']}} %}")
        failures, notices = CHECKER.check_model(self.sql)
        self.assertIn('cluster SYN_B (source: PCD)', failures[0])
        self.assertEqual(notices, [])

    def test_wrong_test_source_fails(self):
        self.yaml.write_text(self.yaml.read_text().replace('source: PCD', 'source: ECL_CACHE'))
        failures, notices = CHECKER.check_model(self.sql)
        self.assertIn('source: PCD', failures[0])
        self.assertEqual(notices, [])

    def test_classifier_union_is_checked(self):
        self.sql.write_text(
            "select * from {{ ref('stg_reference_combined_codesets') }} "
            "where upper(cluster_id) in ({{ observation_evidence_cluster_ids() }})"
        )
        self.yaml.write_text(self.yaml.read_text().replace('SYN_A', 'SYN_OTHER'))
        failures, notices = CHECKER.check_model(self.sql)
        self.assertIn('cluster SYN_A', failures[0])
        self.assertEqual(notices, [])

    def test_unknown_consumer_fails_instead_of_skipping(self):
        self.sql.write_text(self.sql.read_text().replace("'review'", "'unknown'"))
        failures, notices = CHECKER.check_model(self.sql)
        self.assertIn('No evidence lists', failures[0])
        self.assertEqual(notices, [])


if __name__ == '__main__':
    unittest.main()
