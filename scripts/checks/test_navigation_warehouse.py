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


class NavigationDeliveryFilterTests(unittest.TestCase):
    def render(self, incremental=True, execute=True, call="'s.received_at', 'contact'"):
        root = Path(__file__).resolve().parents[2]
        sql = (root / "macros/transformations/navigation_delivery_filter.sql").read_text()
        self.queries = []

        def run_query(query):
            self.queries.append(query)
            return SimpleNamespace(columns=[SimpleNamespace(values=lambda: ["2026-01-10 12:34:56.123456789"])])

        template = Environment().from_string(sql + "{{ navigation_delivery_filter(" + call + ") }}")
        return template.render(
            is_incremental=lambda: incremental, execute=execute,
            this="fixture_target", run_query=run_query,
        ).strip()

    def test_increment_resolves_per_type_literal_and_replays_boundary_and_nulls(self):
        self.assertEqual(
            self.render(),
            "and (s.received_at is null or s.received_at >= '2026-01-10 12:34:56.123456789'::timestamp_ntz)",
        )
        self.assertEqual(self.queries, [
            "select to_varchar(coalesce(max(source_received_at), '1900-01-01'::timestamp_ntz), "
            "'YYYY-MM-DD HH24:MI:SS.FF9') from fixture_target where source_record_type = 'contact'"
        ])

    def test_parse_uses_default_without_querying(self):
        self.assertIn("'1900-01-01 00:00:00.000000000'::timestamp_ntz", self.render(execute=False))
        self.assertEqual(self.queries, [])

    def test_full_refresh_does_not_filter_or_query(self):
        self.assertEqual(self.render(incremental=False), "")
        self.assertEqual(self.queries, [])

    def test_type_column_compares_each_row_with_its_own_watermark(self):
        call = "'i.received_at', source_record_type_column='i.item_type'"
        for execute in (True, False):
            sql = " ".join(self.render(execute=execute, call=call).split())
            self.assertEqual(
                sql,
                "and ( i.received_at is null or i.received_at >= coalesce(( "
                "select max(watermark.source_received_at) from fixture_target as watermark "
                "where watermark.source_record_type = i.item_type ), '1900-01-01'::timestamp_ntz) )",
            )
            self.assertEqual(self.queries, [])
        self.assertEqual(self.render(incremental=False, call=call), "")


if __name__ == "__main__":
    unittest.main()
