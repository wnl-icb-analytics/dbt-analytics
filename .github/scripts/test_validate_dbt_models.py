import json
import subprocess
import unittest
from unittest.mock import patch

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

    def test_existing_parent_is_not_duplicated(self):
        self.assertEqual(build_selection([
            {"name": "int_sus_uec_diagnosis", "resource_type": "model"},
            {"name": UEC_PARENT, "resource_type": "model"},
        ]), ["int_sus_uec_diagnosis", UEC_PARENT])


class BuildCommandTests(unittest.TestCase):
    def run_build(self, target):
        nodes = [
            {"name": "int_sus_uec_diagnosis", "resource_type": "model"},
            {"name": "original_relationship_test", "resource_type": "test"},
        ]
        listed = subprocess.CompletedProcess(
            [], 0, stdout="\n".join(json.dumps(node) for node in nodes), stderr=""
        )
        with patch("sys.argv", [
            "validate_dbt_models.py", "--target", target, "--state", "state",
            "--select", "state:modified+", "tag:indicator_definitions+",
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


if __name__ == "__main__":
    unittest.main()
