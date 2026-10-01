import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).parents[1] / "2_generate_sources.py"
SPEC = importlib.util.spec_from_file_location("generate_sources", MODULE_PATH)
GENERATE_SOURCES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATE_SOURCES)


class SourceTableFilterTest(unittest.TestCase):
    def test_excluded_table_is_omitted(self):
        mapping = {"exclude_tables": ["WNL"]}
        table_names = ["WNL", "WNL_ORGANISATION"]

        generated = [
            name
            for name in table_names
            if GENERATE_SOURCES.should_generate_table(name, mapping)
        ]

        self.assertEqual(generated, ["WNL_ORGANISATION"])


if __name__ == "__main__":
    unittest.main()
