import copy
import json
import subprocess
import unittest
from unittest.mock import patch

from full_refresh_label import pull_numbers
from indicator_definitions_changed import changes
from validate_dbt_models import UEC_PARENT, build_selection, main


class BuildSelectionTests(unittest.TestCase):
    def test_each_code_model_adds_parent_and_keeps_original_tests(self):
        for name in ("int_sus_uec_diagnosis", "int_sus_uec_procedure"):
            with self.subTest(name=name):
                nodes = [
                    {"name": name, "resource_type": "model"},
                    {"name": "relationship_with_deferred_parent", "resource_type": "test"},
                ]
                self.assertEqual(set(build_selection(nodes)), {
                    name, UEC_PARENT, "relationship_with_deferred_parent",
                })

    def test_other_models_do_not_add_uec_parent(self):
        self.assertEqual(build_selection([
            {"name": "int_sus_apc_diagnosis", "resource_type": "model"},
        ]), ["int_sus_apc_diagnosis"])

    def test_empty_selection_stays_empty(self):
        self.assertEqual(build_selection([]), [])

class BuildCommandTests(unittest.TestCase):
    def run_build(self, target, *flags):
        nodes = [
            {"name": "int_sus_uec_diagnosis", "resource_type": "model"},
            {"name": "original_relationship_test", "resource_type": "test"},
        ]
        listed = subprocess.CompletedProcess(
            [], 0, stdout="\n".join(json.dumps(node) for node in nodes), stderr=""
        )
        with patch("sys.argv", [
            "validate_dbt_models.py", "--target", target, "--state", "state",
            "--select", "state:modified+", "tag:indicator_definitions+", *flags,
        ]), patch("validate_dbt_models.subprocess.run", return_value=listed) as run:
            main()
        self.assertEqual(run.call_count, 2)
        listing, building = [call.args[0] for call in run.call_args_list]
        self.assertEqual(listing[listing.index("--target") + 1], target)
        self.assertIn("state:modified+", listing)
        self.assertIn("tag:indicator_definitions+", listing)
        self.assertEqual(building[building.index("--target") + 1], target)
        for name in ("int_sus_uec_diagnosis", UEC_PARENT, "original_relationship_test"):
            self.assertIn(name, building)
        for command in (listing, building):
            self.assertEqual(command[command.index("--exclude") + 1], "tag:reconciliation")
        self.assertTrue(run.call_args.kwargs["check"])
        return building

    def test_production_refreshes_parent_without_deferral(self):
        command = self.run_build("prod")
        self.assertNotIn("--defer", command)
        self.assertNotIn("--favor-state", command)

    def test_dev_keeps_production_deferral(self):
        command = self.run_build("dev")
        self.assertIn("--defer", command)
        self.assertIn("--favor-state", command)

    def test_full_refresh_is_opt_in(self):
        for target in ("dev", "prod"):
            with self.subTest(target=target):
                self.assertNotIn("--full-refresh", self.run_build(target))
                self.assertIn("--full-refresh", self.run_build(target, "--full-refresh"))


class FullRefreshLabelTests(unittest.TestCase):
    def test_reads_trailing_pull_request_number(self):
        self.assertEqual(pull_numbers([
            "feat: add columns to stg_lsacm (#1531)",
            "fix: follow up on issue (#12) (#1532)",
            "chore: direct commit without a pull request",
            "fix: mentions (#99) mid-subject",
        ]), [1531, 1532])


def manifest(meta=None, columns=None, macro_sql="{% macro extract_indicator_metadata() %}{% endmacro %}"):
    return {
        "nodes": {
            "model.wnl_analytics.fct_x": {
                "resource_type": "model", "name": "fct_x",
                "config": {"meta": {"owner": {"name": "Team"}, **(meta or {})}},
                "columns": columns or {},
            },
        },
        "macros": {
            "macro.wnl_analytics.extract_indicator_metadata": {"macro_sql": macro_sql},
            "macro.wnl_analytics.archive_definitions": {"macro_sql": "archive"},
        },
    }


class IndicatorDefinitionsChangedTests(unittest.TestCase):
    INDICATOR = {"indicator": {"id": "IND_X", "name_short": "X"}}

    def test_identical_metadata_is_unchanged(self):
        self.assertEqual(changes(manifest(self.INDICATOR), manifest(self.INDICATOR)), [])

    def test_unrelated_meta_change_is_unchanged(self):
        current = manifest(self.INDICATOR)
        current["nodes"]["model.wnl_analytics.fct_x"]["config"]["meta"]["owner"] = {"name": "Other"}
        self.assertEqual(changes(manifest(self.INDICATOR), current), [])

    def test_model_indicator_change(self):
        current = copy.deepcopy(self.INDICATOR)
        current["indicator"]["name_short"] = "Y"
        self.assertEqual(changes(manifest(self.INDICATOR), manifest(current)), ["fct_x"])

    def test_added_and_removed_indicators(self):
        self.assertEqual(changes(manifest(), manifest(self.INDICATOR)), ["fct_x"])
        self.assertEqual(changes(manifest(self.INDICATOR), manifest()), ["fct_x"])

    def test_column_indicator_change(self):
        columns = {"flag": {"meta": {"indicator": {"id": "IND_C"}}}}
        changed = {"flag": {"meta": {"indicator": {"id": "IND_D"}}}}
        self.assertEqual(changes(manifest(columns=columns), manifest(columns=changed)), ["fct_x"])

    def test_macro_change_ignores_line_endings(self):
        self.assertEqual(changes(manifest(macro_sql="a\nb"), manifest(macro_sql="a\r\nb")), [])
        self.assertEqual(changes(manifest(macro_sql="a"), manifest(macro_sql="b")),
                         ["macro extract_indicator_metadata"])


if __name__ == "__main__":
    unittest.main()
