import unittest

from validate_dbt_models import UEC_PARENT, build_selection


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


if __name__ == "__main__":
    unittest.main()
