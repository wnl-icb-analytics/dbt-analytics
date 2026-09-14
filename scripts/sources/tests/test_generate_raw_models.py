import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml


def load_script(filename):
    path = Path(__file__).parents[1] / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GENERATOR = load_script('3_generate_raw_models.py')
RUNNER = load_script('run_all_source_generation.py')


class RawModelChangeWarningTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sources = self.root / 'models' / 'sources'
        self.sources.mkdir(parents=True)
        self.staging = self.root / 'models' / 'staging'
        self.staging.mkdir()
        self.mappings_file = self.root / 'source_mappings.yml'
        self.source = {
            'name': 'sample', 'database': 'SYNTHETIC', 'schema': 'EXAMPLE',
            'tables': [{'name': 'ITEMS', 'columns': [{'name': 'ItemID'}]}],
        }
        self.mapping = {
            'source_name': 'sample', 'domain': 'commissioning', 'raw_prefix': 'raw_sample',
        }
        self.write_inputs()
        self.generate()

    def write_inputs(self):
        (self.sources / 'manual_sample.yml').write_text(
            yaml.safe_dump({'sources': [self.source]}), encoding='utf-8',
        )
        self.mappings_file.write_text(
            yaml.safe_dump({'sources': [self.mapping]}), encoding='utf-8',
        )

    def generate(self):
        output = io.StringIO()
        with patch.multiple(GENERATOR, PROJECT_DIR=str(self.root),
                            SOURCES_DIR=str(self.sources), MAPPINGS_FILE=str(self.mappings_file)):
            with contextlib.redirect_stdout(output):
                GENERATOR.main()
        return output.getvalue()

    def test_unchanged_generation_has_no_change_warning(self):
        model = self.root / 'models/raw/commissioning/raw_sample_items.sql'
        original = model.read_text()
        self.assertNotIn('WARNING:', self.generate())
        self.assertEqual(original, model.read_text())

    def test_prefix_rename_reports_source_and_broken_staging_ref(self):
        (self.staging / 'stg_items.sql').write_text(
            "select * from {{ ref('raw_sample_items') }}\n", encoding='utf-8',
        )
        self.mapping['raw_prefix'] = 'raw_shared_sample'
        self.write_inputs()
        output = self.generate()
        self.assertIn('Renamed: models/raw/commissioning/raw_sample_items.sql -> '
                      'models/raw/commissioning/raw_shared_sample_items.sql', output)
        self.assertIn('Source: sample.ITEMS', output)
        self.assertIn('Broken staging refs:\n      models/staging/stg_items.sql:1', output)
        self.assertFalse((self.root / 'models/raw/commissioning/raw_sample_items.sql').exists())

    def test_table_removal_reports_broken_ref(self):
        (self.staging / 'stg_items.sql').write_text(
            '{{ ref("raw_sample_items") }}', encoding='utf-8',
        )
        self.source['tables'] = []
        self.write_inputs()
        output = self.generate()
        self.assertIn('Removed: models/raw/commissioning/raw_sample_items.sql', output)
        self.assertIn('Broken staging refs:', output)

    def test_source_table_rename_is_not_guessed_as_same_identity(self):
        self.source['tables'][0]['name'] = 'NEW_ITEMS'
        self.write_inputs()
        output = self.generate()
        self.assertIn('Removed: models/raw/commissioning/raw_sample_items.sql', output)
        self.assertIn('Created commissioning raw model: raw_sample_new_items.sql', output)
        self.assertNotIn('Renamed:', output)

    def test_domain_move_retains_ref_name(self):
        (self.staging / 'stg_items.sql').write_text(
            "{{ ref('raw_sample_items') }}", encoding='utf-8',
        )
        self.mapping['domain'] = 'shared'
        self.write_inputs()
        output = self.generate()
        self.assertIn('Moved: models/raw/commissioning/raw_sample_items.sql -> '
                      'models/raw/shared/raw_sample_items.sql (ref name unchanged)', output)
        self.assertIn('Review staging refs:', output)
        self.assertNotIn('Broken staging refs:', output)

    def test_manual_routing_still_overrides_mapping(self):
        self.source['config'] = {'meta': {'domain': 'shared', 'raw_prefix': 'raw_manual'}}
        self.write_inputs()
        output = self.generate()
        self.assertIn('Renamed: models/raw/commissioning/raw_sample_items.sql -> '
                      'models/raw/shared/raw_manual_items.sql', output)

    def test_reused_model_name_in_another_domain_reports_changed_source(self):
        (self.staging / 'stg_items.sql').write_text(
            "{{ ref('raw_sample_items') }}", encoding='utf-8',
        )
        self.source['name'] = 'replacement'
        self.mapping['source_name'] = 'replacement'
        self.mapping['domain'] = 'shared'
        self.write_inputs()
        output = self.generate()
        self.assertIn('Reused name: models/raw/commissioning/raw_sample_items.sql -> '
                      'models/raw/shared/raw_sample_items.sql', output)
        self.assertIn('Previous source: sample.ITEMS', output)
        self.assertIn('New source: replacement.ITEMS', output)
        self.assertIn('Review staging refs:\n      models/staging/stg_items.sql:1', output)
        self.assertNotIn('Moved:', output)
        self.assertNotIn('Broken staging refs:', output)

    def test_default_domain_and_prefix_are_unchanged(self):
        self.mapping = {'source_name': 'sample'}
        self.write_inputs()
        output = self.generate()
        self.assertNotIn('WARNING:', output)
        self.assertTrue((self.root / 'models/raw/commissioning/raw_sample_items.sql').exists())

    def test_unreferenced_removal_requests_consumer_review(self):
        self.source['tables'] = []
        self.write_inputs()
        output = self.generate()
        self.assertIn('No literal staging refs found; check indirect and external consumers.', output)

    def test_refs_include_multiline_versioned_and_yaml_calls_but_not_jinja_comments(self):
        (self.staging / 'stg_items.sql').write_text(
            "{# {{ ref('raw_sample_items') }}\n#}\n"
            "select * from {{ ref (\n 'raw_sample_items', version=1) }}\n"
            "-- depends_on: {{ ref('project', 'raw_sample_items') }}\n"
            "{{ ref('raw_other_items') }}\n",
            encoding='utf-8',
        )
        (self.staging / 'stg_items.yml').write_text(
            'models:\n  - to: "{{ ref(\'raw_sample_items\') }}"\n', encoding='utf-8',
        )
        self.assertEqual(GENERATOR.staging_refs(self.root, {'raw_sample_items'}), {
            'raw_sample_items': {
                'models/staging/stg_items.sql:3', 'models/staging/stg_items.sql:5',
                'models/staging/stg_items.yml:2',
            },
        })

    def test_full_workflow_retains_successful_generator_warning(self):
        self.source['tables'] = []
        self.write_inputs()
        output = self.generate()
        result = subprocess.CompletedProcess([], 0, stdout=output, stderr='')
        with patch.object(RUNNER.subprocess, 'run', return_value=result):
            with self.assertLogs(RUNNER.logger, level='INFO') as logs:
                self.assertTrue(RUNNER.run_script('3_generate_raw_models.py'))
        self.assertIn('WARNING: Raw model names or locations changed.', '\n'.join(logs.output))


if __name__ == '__main__':
    unittest.main()
