"""Run synthetic pregnancy exclusions through the model SQL without Snowflake.

Run: python3 -m unittest discover -s scripts/qa/tests -v
SQLite supplies the joins and windows; the date functions below cover the
Snowflake functions used by these models. This does not replace dbt compilation.
"""

from datetime import date, timedelta
from pathlib import Path
import re
import sqlite3
import unittest


ROOT = Path(__file__).resolve().parents[3]
EPISODES = "int_pregnancy_episodes_all"
WINDOWS = "int_hdp_exclusion_windows_all"
ELIGIBLE = "int_qmul_gp_bp_registry_bp_readings_eligible"


def calendar_date(value):
    return date.fromisoformat(value[:10]) if value is not None else None


def dateadd(unit, amount, value):
    days = amount * {"day": 1, "week": 7}[unit]
    return (calendar_date(value) + timedelta(days=days)).isoformat()


def render_model(name):
    """Replace only dbt configuration, refs and these models' default variables."""
    sql = next((ROOT / "models").rglob(f"{name}.sql")).read_text()
    sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"\1", sql)
    defaults = {
        "pregnancy_episode_max_weeks": "42",
        "hdp_postpartum_extension_weeks": "8",
        "qmul_gp_bp_registry_extraction_start_date": "'2010-01-01'",
        "qmul_gp_bp_registry_extraction_end_date": "'2025-12-31'",
    }
    for macro, value in defaults.items():
        sql = sql.replace("{{ " + macro + "() }}", value)
    if "{{" in sql or "{%" in sql:
        raise AssertionError(f"Unrecognised dbt expression in {name}")
    return sql


class PregnancyExclusionsTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.create_function("TO_DATE", 1, lambda v: v[:10] if v else None)
        self.db.create_function("DATEADD", 3, dateadd)
        self.db.create_function(
            "DATEDIFF", 3, lambda unit, a, b: (calendar_date(b) - calendar_date(a)).days
        )
        self.db.create_function("LEAST", 2, min)
        self.db.create_function("IFF", 3, lambda condition, yes, no: yes if condition else no)
        self.db.executescript("""
            CREATE TABLE int_pregnancy_observations_all (
                person_id TEXT, clinical_effective_date TEXT,
                is_pregnancy_code BOOLEAN, is_delivery_outcome_code BOOLEAN, code_type TEXT
            );
            CREATE TABLE int_hypertensive_disorders_of_pregnancy_all (
                person_id TEXT, clinical_effective_date TEXT
            );
            CREATE TABLE int_qmul_gp_bp_registry_base_population (person_id TEXT);
            INSERT INTO int_qmul_gp_bp_registry_base_population VALUES ('synthetic');
            CREATE TABLE int_blood_pressure_all (
                person_id TEXT, effective_date TEXT, systolic_value INTEGER,
                diastolic_value INTEGER, is_home_bp_event BOOLEAN,
                is_abpm_bp_event BOOLEAN, is_hypertensive_range BOOLEAN
            );
        """)

    def observations(self, events):
        self.db.executemany(
            "INSERT INTO int_pregnancy_observations_all VALUES (?, ?, ?, ?, ?)",
            [("synthetic", day, kind == "pregnancy", kind != "pregnancy", kind)
             for day, kind in events],
        )

    def build(self, *models):
        for model in models:
            self.db.execute(f"CREATE TABLE {model} AS {render_model(model)}")

    def readings(self, dates):
        self.db.executemany(
            "INSERT INTO int_blood_pressure_all VALUES ('synthetic', ?, 150, 95, 0, 0, 1)",
            [(day,) for day in dates],
        )

    def test_same_day_pregnancy_precedes_outcomes_and_deduplicates(self):
        self.observations([
            ("2020-07-01 00:00:00", "delivery"),
            ("2020-07-01 01:00:00", "pregnancy"),
            ("2020-07-01 00:00:00", "pregnancy"),
            ("2020-07-01 01:00:00", "pregnancy_loss"),
        ])
        self.build(EPISODES, WINDOWS)
        self.assertEqual(self.db.execute(
            f"SELECT episode_start, episode_end, outcome_type, is_orphan_outcome_episode FROM {EPISODES}"
        ).fetchall(), [("2020-07-01", "2020-07-01", "delivery", 0)])
        self.readings(["2020-07-01 01:00:00", "2020-07-02 01:00:00"])
        self.build(ELIGIBLE)
        self.assertEqual(self.db.execute(f"SELECT effective_date FROM {ELIGIBLE}").fetchall(),
                         [("2020-07-02 01:00:00",)])

    def test_midnight_delivery_and_later_same_day_pregnancy_form_one_episode(self):
        self.observations([
            ("2020-07-01 00:00:00", "delivery"),
            ("2020-07-01 01:00:00", "pregnancy"),
        ])
        self.build(EPISODES, WINDOWS)
        self.assertEqual(self.db.execute(
            f"SELECT episode_start, episode_end, is_orphan_outcome_episode FROM {EPISODES}"
        ).fetchall(), [("2020-07-01", "2020-07-01", 0)])
        self.readings(["2020-07-02 01:00:00"])
        self.build(ELIGIBLE)
        self.assertEqual(self.db.execute(f"SELECT effective_date FROM {ELIGIBLE}").fetchall(),
                         [("2020-07-02 01:00:00",)])

    def test_hdp_on_delivery_day_extends_window_through_last_calendar_day(self):
        self.observations([
            ("2020-01-01 01:00:00", "pregnancy"),
            ("2020-07-01 00:00:00", "delivery"),
        ])
        self.db.execute("""
            INSERT INTO int_hypertensive_disorders_of_pregnancy_all
            VALUES ('synthetic', '2020-07-01 01:00:00')
        """)
        self.build(EPISODES, WINDOWS)
        self.assertEqual(self.db.execute(
            f"SELECT window_start, window_end, reason FROM {WINDOWS}"
        ).fetchall(), [("2020-01-01", "2020-08-26", "pregnancy_hdp")])
        self.readings(["2020-01-01 00:00:00", "2020-08-26 01:00:00", "2020-08-27 00:00:00"])
        self.build(ELIGIBLE)
        self.assertEqual(self.db.execute(f"SELECT effective_date FROM {ELIGIBLE}").fetchall(),
                         [("2020-08-27 00:00:00",)])

    def test_extraction_includes_whole_final_day(self):
        self.build(EPISODES, WINDOWS)
        self.readings(["2025-12-31 01:00:00", "2026-01-01 00:00:00"])
        self.build(ELIGIBLE)
        self.assertEqual(self.db.execute(f"SELECT effective_date FROM {ELIGIBLE}").fetchall(),
                         [("2025-12-31 01:00:00",)])

    def test_short_gaps_cannot_extend_episode_beyond_maximum_duration(self):
        self.observations([
            ("2020-01-01", "pregnancy"),
            ("2020-07-01", "pregnancy"),
            ("2020-12-01", "pregnancy"),
            ("2021-02-01", "delivery"),
        ])
        self.build(EPISODES, WINDOWS)
        self.assertEqual(self.db.execute(
            f"SELECT episode_id, episode_start, episode_end, outcome_type FROM {EPISODES}"
        ).fetchall(), [
            (1, "2020-01-01", "2020-10-21", "fallback_max_duration"),
            (2, "2020-12-01", "2021-02-01", "delivery"),
        ])
        self.readings(["2020-12-15", "2021-02-02"])
        self.build(ELIGIBLE)
        self.assertEqual(self.db.execute(f"SELECT effective_date FROM {ELIGIBLE}").fetchall(),
                         [("2021-02-02",)])

    def test_maximum_date_is_inclusive_and_each_new_episode_resets_it(self):
        self.observations([
            ("2020-01-01", "pregnancy"),
            ("2020-10-21", "pregnancy"),  # Exactly 42 weeks from first code.
            ("2020-10-22", "pregnancy"),
            ("2021-08-12", "pregnancy"),  # Exactly 42 weeks from second start.
            ("2021-08-13", "pregnancy"),
        ])
        self.build(EPISODES)
        self.assertEqual(self.db.execute(
            f"SELECT episode_id, episode_start, episode_end FROM {EPISODES}"
        ).fetchall(), [
            (1, "2020-01-01", "2020-10-21"),
            (2, "2020-10-22", "2021-08-12"),
            (3, "2021-08-13", "2022-06-03"),
        ])

    def test_outcome_rules_and_person_partition_are_preserved(self):
        self.observations([
            ("2020-01-01", "delivery"),
            ("2020-02-01", "pregnancy"),
            ("2020-03-01", "pregnancy_loss"),
            ("2020-03-02", "delivery"),
            ("2020-04-01", "pregnancy"),
        ])
        self.db.execute("""
            INSERT INTO int_pregnancy_observations_all
            VALUES ('synthetic_other', '2020-02-01', 1, 0, 'pregnancy')
        """)
        self.build(EPISODES)
        self.assertEqual(self.db.execute(
            f"SELECT person_id, episode_id, episode_start, episode_end, outcome_type FROM {EPISODES} "
            "ORDER BY person_id, episode_id"
        ).fetchall(), [
            ("synthetic", 1, "2019-03-13", "2020-01-01", "delivery"),
            ("synthetic", 2, "2020-02-01", "2020-03-01", "pregnancy_loss"),
            ("synthetic", 3, "2020-04-01", "2021-01-20", "fallback_max_duration"),
            ("synthetic_other", 1, "2020-02-01", "2020-11-21", "fallback_max_duration"),
        ])


if __name__ == "__main__":
    unittest.main()
