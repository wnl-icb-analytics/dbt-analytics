"""Local synthetic checks: uv run --with duckdb --with jinja2 --with sqlglot python -m unittest discover -s scripts/qa/nice/tests."""

import csv
import unittest
from pathlib import Path
from types import SimpleNamespace

import duckdb
import jinja2
import sqlglot
import yaml

ROOT = Path(__file__).resolve().parents[4]
NICE = ROOT / "models/reporting/olids/measures/nice"


def render(path, **context):
    env = jinja2.Environment(
        extensions=["jinja2.ext.do"], undefined=jinja2.StrictUndefined
    )
    env.globals.update(config=lambda **kwargs: "", ref=lambda name: name)
    env.globals.update(context)
    return env.from_string(path.read_text(encoding="utf-8")).render()


class ClassificationTests(unittest.TestCase):
    def test_catalogue_coverage_and_classification_rules(self):
        entries = {}
        for path in (ROOT / "models").rglob("*.yml"):
            for model in (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get(
                "models", []
            ):
                meta = model.get("config", {}).get("meta", {})
                indicators = meta.get("indicators", []) + (
                    [meta["indicator"]] if meta.get("indicator") else []
                )
                for column in model.get("columns", []):
                    indicator = column.get("meta", {}).get("indicator")
                    if indicator:
                        indicators.append(indicator)
                for indicator in indicators:
                    self.assertNotIn(indicator["id"], entries)
                    entries[indicator["id"]] = indicator
        with (ROOT / "seeds/indicator_classification.csv").open(
            encoding="utf-8", newline=""
        ) as f:
            rows = list(csv.DictReader(f))
        with (ROOT / "seeds/ltc_register_denominator_rules.csv").open(
            encoding="utf-8", newline=""
        ) as f:
            conditions = {row["condition_code"] for row in csv.DictReader(f)}
        self.assertEqual(len(rows), len(entries))
        self.assertEqual({row["indicator_id"] for row in rows}, entries.keys())
        for row in rows:
            key = row["indicator_id"]
            expected = (
                "NICE"
                if key.startswith("IND")
                else "QOF"
                if entries[key].get("is_qof")
                else "LTC_LCS"
                if key.startswith("LTC_LCS_")
                else "LOCAL"
                if key in ("COVID_FLU_DASHBOARD_BASE", "COVID_FLU_UPTAKE_COMBINED")
                else "FLU"
                if key.startswith("FLU_")
                else "COVID"
                if key.startswith("COVID_")
                else "LOCAL"
            )
            self.assertEqual(row["programme"], expected, key)
            if row["condition_code"]:
                self.assertIn(row["condition_code"], conditions, key)
                self.assertFalse(
                    row["clinical_domain"] or row["clinical_subdomain"], key
                )
            else:
                self.assertTrue(
                    row["clinical_domain"] and row["clinical_subdomain"], key
                )

    def test_singular_test_rejects_missing_partial_and_double_classification(self):
        with duckdb.connect() as db:
            db.execute(
                "CREATE TABLE indicator_classification(indicator_id VARCHAR, condition_code VARCHAR, clinical_domain VARCHAR, clinical_subdomain VARCHAR)"
            )
            db.executemany(
                "INSERT INTO indicator_classification VALUES (?, ?, ?, ?)",
                [
                    ("condition", "DM", None, None),
                    ("topic", None, "Lifestyle", "Smoking"),
                    ("missing", None, None, None),
                    ("partial", None, "Lifestyle", None),
                    ("double", "DM", "Lifestyle", "Smoking"),
                    ("whitespace", " ", " ", " "),
                ],
            )
            failures = db.execute(
                render(ROOT / "tests/indicator_classification_has_one_domain.sql")
            ).fetchall()
            self.assertEqual(
                {row[0] for row in failures},
                {"missing", "partial", "double", "whitespace"},
            )

    def test_catalogue_uses_condition_lookup_and_keeps_unclassified_rows(self):
        indicators = []
        for key in ["condition", "topic", "missing"]:
            indicators.append(
                {
                    "indicator_id": key,
                    "indicator_type": "MEASURE",
                    "category": "Clinical",
                    "clinical_domain": "Legacy value",
                    "name_short": key,
                    "description_short": key,
                    "description_long": key,
                    "source_model": "synthetic",
                    "source_column": "flag",
                    "is_qof": False,
                    "qof_indicator": None,
                    "sort_order": key,
                }
            )
        with duckdb.connect() as db:
            db.execute(
                "CREATE TABLE indicator_classification AS SELECT 'condition' indicator_id, 'NICE' programme, 'DM' condition_code, NULL::VARCHAR clinical_domain, NULL::VARCHAR clinical_subdomain UNION ALL SELECT 'topic','LOCAL',NULL,'Lifestyle','Smoking'"
            )
            db.execute(
                "CREATE TABLE ltc_register_denominator_rules AS SELECT 'DM' condition_code, 'Metabolic' clinical_domain, 'Diabetes Mellitus' condition_name"
            )
            sql = render(
                ROOT / "models/reporting/olids/definitions/def_indicator.sql",
                extract_indicator_metadata=lambda: {"indicators": indicators},
            )
            result = db.execute(
                sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]
            )
            self.assertEqual(
                [c[0] for c in result.description][2:5],
                ["programme", "clinical_domain", "clinical_subdomain"],
            )
            rows = {row[0]: row[2:5] for row in result.fetchall()}
            self.assertEqual(
                rows["condition"], ("NICE", "Metabolic", "Diabetes Mellitus")
            )
            self.assertEqual(rows["topic"], ("LOCAL", "Lifestyle", "Smoking"))
            self.assertEqual(rows["missing"], (None, None, None))


class AchievementTests(unittest.TestCase):
    def test_current_and_monthly_counts_dates_names_rounding_and_unmapped_practices(
        self,
    ):
        for suffix, practice_field in [
            ("", "current_practice_code"),
            ("_by_month", "practice_code"),
        ]:
            with self.subTest(suffix=suffix), duckdb.connect() as db:
                table = "fct_person_nice_indicator_status" + suffix
                db.execute(
                    f"CREATE TABLE {table}(reporting_date DATE, indicator_id VARCHAR, indicator_name VARCHAR, {practice_field} VARCHAR, is_in_denominator BOOLEAN, is_in_numerator BOOLEAN)"
                )
                db.executemany(
                    f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            "2026-09-30",
                            "INDTEST",
                            "Published title",
                            "SYNTH_A",
                            True,
                            True,
                        ),
                        (
                            "2026-09-30",
                            "INDTEST",
                            "Published title",
                            "SYNTH_A",
                            True,
                            False,
                        ),
                        (
                            "2026-09-30",
                            "INDTEST",
                            "Published title",
                            "SYNTH_A",
                            True,
                            None,
                        ),
                        (
                            "2026-09-30",
                            "INDTEST",
                            "Published title",
                            "SYNTH_A",
                            False,
                            True,
                        ),
                        (
                            "2026-09-30",
                            "INDTEST",
                            "Published title",
                            "SYNTH_B",
                            True,
                            True,
                        ),
                        (
                            "2026-08-31",
                            "INDTEST",
                            "Published title",
                            "SYNTH_A",
                            True,
                            False,
                        ),
                        (
                            "2026-09-30",
                            "INDOTHER",
                            "Other title",
                            "SYNTH_A",
                            True,
                            True,
                        ),
                    ],
                )
                db.execute(
                    "CREATE TABLE def_indicator AS SELECT 'INDTEST' indicator_id, 'NICE' programme, 'Metabolic' clinical_domain, 'Diabetes Mellitus' clinical_subdomain, 'Short label' name_short, 'Short description' description_short UNION ALL SELECT 'INDOTHER','NICE','Lifestyle','Smoking','Other label','Other description'"
                )
                db.execute(
                    "CREATE TABLE dim_practice AS SELECT 'SYNTH_A' practice_code, 'Synthetic practice' practice_name, 'SYNTH_PCN' pcn_code, 'Synthetic PCN' pcn_name, 'Synthetic borough' borough_registered"
                )
                result = db.execute(
                    render(
                        NICE / f"fct_practice_nice_indicator_achievement{suffix}.sql"
                    )
                )
                columns = [c[0] for c in result.description]
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                self.assertEqual(len(rows), 4)
                september = next(
                    row
                    for row in rows
                    if str(row["reporting_date"]) == "2026-09-30"
                    and row["practice_code"] == "SYNTH_A"
                    and row["indicator_id"] == "INDTEST"
                )
                self.assertEqual(
                    (
                        september["denominator"],
                        september["numerator"],
                        september["not_achieved"],
                        september["achievement_pct"],
                    ),
                    (3, 1, 2, 33.3),
                )
                self.assertEqual(september["indicator_name"], "Published title")
                self.assertEqual(september["description_short"], "Short description")
                self.assertEqual(september["pcn_code"], "SYNTH_PCN")
                self.assertEqual(sum(row["denominator"] for row in rows), 6)
                self.assertEqual(sum(row["numerator"] for row in rows), 3)
                unmapped = next(
                    row for row in rows if row["practice_code"] == "SYNTH_B"
                )
                self.assertIsNone(unmapped["practice_name"])
                self.assertEqual(unmapped["achievement_pct"], 100)
                self.assertEqual(
                    next(
                        row["achievement_pct"]
                        for row in rows
                        if str(row["reporting_date"]) == "2026-08-31"
                    ),
                    0,
                )

    def test_status_enrichment_preserves_existing_values_and_unmapped_indicators(self):
        text = (ROOT / "macros/nice/nice_practice_columns.sql").read_text(
            encoding="utf-8"
        )
        text += (ROOT / "macros/nice/nice_indicator_status.sql").read_text(
            encoding="utf-8"
        )
        for reference in ["current", "by_month"]:
            with self.subTest(reference=reference), duckdb.connect() as db:
                prefix = "current_" if reference == "current" else ""
                old_columns = [
                    "person_id",
                    "indicator_id",
                    "indicator_name",
                    "reporting_date",
                    "measurement_period_start",
                    "age",
                    prefix + "practice_code",
                    prefix + "practice_name",
                    "is_in_denominator",
                    "is_in_numerator",
                    "indicator_status",
                ]
                db.execute(
                    f"CREATE TABLE synthetic_input AS SELECT 'synthetic_person' person_id, 'INDTEST' indicator_id, 'Published title' indicator_name, DATE '2026-09-30' reporting_date, DATE '2025-09-30' measurement_period_start, 45 age, 'SYNTH_A' {prefix}practice_code, 'Synthetic practice' {prefix}practice_name, TRUE is_in_denominator, FALSE is_in_numerator, 'NOT_ASSESSABLE' indicator_status"
                )
                db.execute(
                    "CREATE TABLE def_indicator AS SELECT 'INDTEST' indicator_id, 'Metabolic' clinical_domain, 'Diabetes Mellitus' clinical_subdomain"
                )
                env = jinja2.Environment(undefined=jinja2.StrictUndefined)
                sql = env.from_string(
                    text + "{{ nice_indicator_status(reference) }}"
                ).render(
                    reference=reference,
                    ref=lambda name: (
                        "synthetic_input" if name != "def_indicator" else name
                    ),
                )
                result = db.execute(sql)
                columns = [c[0] for c in result.description]
                self.assertEqual(
                    columns[3:5], ["clinical_domain", "clinical_subdomain"]
                )
                self.assertEqual(
                    [
                        column
                        for column in columns
                        if column not in ["clinical_domain", "clinical_subdomain"]
                    ],
                    old_columns,
                )
                rows = result.fetchall()
                self.assertEqual(len(rows), 30)
                original = db.execute("SELECT * FROM synthetic_input").fetchone()
                self.assertTrue(
                    all(
                        tuple(row[columns.index(c)] for c in old_columns) == original
                        for row in rows
                    )
                )
                db.execute("DELETE FROM def_indicator")
                self.assertEqual(len(db.execute(sql).fetchall()), 30)


class ArchiveTests(unittest.TestCase):
    def test_existing_history_migration_daily_versions_and_idempotence(self):
        with duckdb.connect() as db:
            columns = [
                "indicator_id VARCHAR",
                "indicator_type VARCHAR",
                "category VARCHAR",
                "clinical_domain VARCHAR",
                "name_short VARCHAR",
                "description_short VARCHAR",
                "description_long VARCHAR",
                "source_model VARCHAR",
                "source_column VARCHAR",
                "is_qof BOOLEAN",
                "qof_indicator VARCHAR",
                "sort_order VARCHAR",
                "metadata_extracted_at TIMESTAMP",
                "version_number INTEGER",
                "valid_from DATE",
                "valid_to DATE",
                "is_current BOOLEAN",
                "archived_at TIMESTAMP",
                "archived_by VARCHAR",
                "dbt_run_id VARCHAR",
            ]
            db.execute("CREATE TABLE def_indicator_history (" + ",".join(columns) + ")")
            base = [
                "INDTEST",
                "MEASURE",
                "Clinical",
                "Legacy domain",
                "Short",
                "Description",
                "Long",
                "synthetic",
                "flag",
                False,
                None,
                "TEST",
                "2026-10-01 00:00:00",
            ]
            db.executemany(
                "INSERT INTO def_indicator_history VALUES ("
                + ",".join(["?"] * 20)
                + ")",
                [
                    base
                    + [
                        1,
                        "2026-09-01",
                        "2026-09-30",
                        False,
                        "2026-09-01 00:00:00",
                        "synthetic",
                        "old",
                    ],
                    base
                    + [
                        2,
                        "2026-10-01",
                        None,
                        True,
                        "2026-10-01 00:00:00",
                        "synthetic",
                        "current",
                    ],
                ],
            )
            db.execute(
                "CREATE TABLE def_indicator AS SELECT indicator_id, indicator_type, 'NICE'::VARCHAR programme, category, 'Metabolic'::VARCHAR clinical_domain, 'Diabetes Mellitus'::VARCHAR clinical_subdomain, name_short, description_short, description_long, source_model, source_column, is_qof, qof_indicator, sort_order, metadata_extracted_at FROM def_indicator_history WHERE is_current"
            )
            path = ROOT / "macros/governance/archive_indicator_definitions.sql"
            text = path.read_text(encoding="utf-8").split("{% macro archive_usage()")[0]
            env = jinja2.Environment(undefined=jinja2.StrictUndefined)
            relation_type = type(
                "Relation",
                (SimpleNamespace,),
                {"__str__": lambda self: "memory.main.def_indicator"},
            )
            relation = relation_type(database="memory", schema="main")
            sql = env.from_string(text + "{{ archive_definitions() }}").render(
                this=relation,
                target=SimpleNamespace(user="synthetic"),
                invocation_id="synthetic-run",
                generate_history_table_comment=lambda *args: "Synthetic history",
            )
            sql = sql.replace("CURRENT_DATE()", "CAST('2026-10-06' AS DATE)")
            # DuckDB temporary tables cannot use the main catalogue qualifier.
            sql = sql.replace(
                "memory.main.__def_indicator_history_current_dedup",
                "__def_indicator_history_current_dedup",
            )

            def archive(day="2026-10-06"):
                dated_sql = sql.replace("2026-10-06", day)
                for statement in sqlglot.transpile(
                    dated_sql, read="snowflake", write="duckdb"
                ):
                    if statement.strip():
                        db.execute(statement)

            archive()
            old = db.execute(
                "SELECT version_number, valid_from, valid_to, programme, clinical_subdomain FROM def_indicator_history WHERE NOT is_current ORDER BY version_number"
            ).fetchall()
            self.assertEqual(len(old), 2)
            self.assertEqual(str(old[0][1]), "2026-09-01")
            self.assertEqual(str(old[1][2]), "2026-10-05")
            self.assertTrue(all(row[3:] == (None, None) for row in old))
            current = db.execute(
                "SELECT version_number, programme, clinical_domain, clinical_subdomain FROM def_indicator_history WHERE is_current"
            ).fetchall()
            self.assertEqual(current, [(3, "NICE", "Metabolic", "Diabetes Mellitus")])
            archive()
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM def_indicator_history").fetchone()[0],
                3,
            )
            db.execute(
                "UPDATE def_indicator SET programme='LOCAL', clinical_subdomain='Changed topic'"
            )
            archive()
            self.assertEqual(
                db.execute(
                    "SELECT version_number, programme, clinical_subdomain FROM def_indicator_history WHERE is_current"
                ).fetchall(),
                [(3, "LOCAL", "Changed topic")],
            )
            db.execute("UPDATE def_indicator SET programme='NICE'")
            archive("2026-10-07")
            self.assertEqual(
                db.execute(
                    "SELECT version_number, programme FROM def_indicator_history WHERE is_current"
                ).fetchall(),
                [(4, "NICE")],
            )
            db.execute("UPDATE def_indicator SET clinical_subdomain='Revised topic'")
            archive("2026-10-08")
            self.assertEqual(
                db.execute(
                    "SELECT version_number, clinical_subdomain FROM def_indicator_history WHERE is_current"
                ).fetchall(),
                [(5, "Revised topic")],
            )
            db.execute("DROP TABLE def_indicator_history")
            archive()
            self.assertEqual(
                db.execute(
                    "SELECT version_number, programme, clinical_subdomain FROM def_indicator_history"
                ).fetchall(),
                [(1, "NICE", "Revised topic")],
            )


if __name__ == "__main__":
    unittest.main()
