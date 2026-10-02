# /// script
# dependencies = ["jinja2"]
# ///
"""Run with uv run scripts/checks/test_navigation_warehouse.py."""
from pathlib import Path
from types import SimpleNamespace
import unittest

from jinja2 import Environment


class NavigationWarehouseTests(unittest.TestCase):
    def render(self, role, incremental, restore=False, execute=True):
        root = Path(__file__).resolve().parents[2]
        sql = (root / "macros/config/navigation_build_warehouse.sql").read_text()
        template = Environment().from_string(sql + "{{ navigation_build_warehouse(restore=restore) }}")
        return template.render(
            target=SimpleNamespace(role=role, warehouse="WH_WNL_ENGINEERING_M"),
            adapter=SimpleNamespace(quote=lambda value: '"' + value + '"'),
            execute=execute, is_incremental=lambda: incremental, restore=restore,
        ).strip()

    def test_admin_initial_build_or_full_refresh_uses_large(self):
        self.assertEqual(self.render("DBT_ADMIN", False), 'use warehouse "WH_WNL_OLIDS_L"')

    def test_admin_incremental_keeps_profile_warehouse(self):
        self.assertEqual(self.render("DBT_ADMIN", True), "")

    def test_other_roles_never_request_restricted_warehouse(self):
        for role in ("ENGINEER", "ANALYST"):
            for incremental in (False, True):
                with self.subTest(role=role, incremental=incremental):
                    self.assertEqual(self.render(role, incremental), "")
                    self.assertEqual(self.render(role, incremental, restore=True), "")

    def test_admin_restores_profile_warehouse(self):
        self.assertEqual(self.render("DBT_ADMIN", False, restore=True), 'use warehouse "WH_WNL_ENGINEERING_M"')

    def test_parse_does_not_choose_a_warehouse(self):
        self.assertEqual(self.render("DBT_ADMIN", False, execute=False), "")


if __name__ == "__main__":
    unittest.main()
