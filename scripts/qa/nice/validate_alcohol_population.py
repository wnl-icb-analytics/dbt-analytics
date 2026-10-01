"""Validate NICE depression/anxiety alcohol populations with synthetic records."""

from pathlib import Path
import re

import duckdb
import jinja2
import sqlglot

ROOT = Path(__file__).resolve().parents[3]
db = duckdb.connect()


def table(name, schema, rows):
    db.execute(f"CREATE TABLE {name} ({schema})")
    if rows:
        db.executemany(
            f"INSERT INTO {name} VALUES ({','.join('?' for _ in rows[0])})", rows
        )


def render(text):
    sql = (
        jinja2.Environment()
        .from_string(text)
        .render(config=lambda **kwargs: "", ref=lambda name: name)
    )
    sql = sql.replace("CURRENT_DATE()", "DATE '2026-09-12'")
    return sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]


people = {
    "age9": 9,
    "age10": 10,
    "age17": 17,
    "age18": 18,
    "resolved_child": 15,
    "resolved_same_day": 15,
    "review_only": 15,
    "future_child": 15,
    "old_first_episode": 15,
    "earlier_anxiety": 15,
    "anxiety_only": 15,
}
table("dim_person_age", "person_id VARCHAR, age INTEGER", list(people.items()))
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
table(
    "fct_person_ltc_summary",
    "person_id VARCHAR, condition_code VARCHAR, is_on_register BOOLEAN, earliest_diagnosis_date DATE, latest_diagnosis_date DATE",
    [
        ("age18", "DEP", True, "2026-08-01", "2026-08-01"),
        ("earlier_anxiety", "ANX", True, "2026-06-01", "2026-06-01"),
        ("anxiety_only", "ANX", True, "2026-08-01", "2026-08-01"),
    ],
)
diagnoses = [
    (p, "2026-08-01", True, True, False)
    for p in people
    if p not in ["review_only", "future_child", "anxiety_only"]
]
diagnoses += [
    ("resolved_child", "2026-08-02", False, False, True),
    ("resolved_same_day", "2026-08-01", False, False, True),
    ("review_only", "2026-08-01", True, False, False),
    ("future_child", "2026-09-13", True, True, False),
    ("old_first_episode", "2020-08-01", True, True, False),
    ("old_first_episode", "2021-08-01", False, False, True),
]
table(
    "int_depression_diagnoses_all",
    "person_id VARCHAR, clinical_effective_date DATE, is_diagnosis_code BOOLEAN, is_first_or_new_episode BOOLEAN, is_resolved_code BOOLEAN",
    diagnoses,
)
profile_sql = (
    ROOT / "models/modelling/olids/person_attributes/int_ltc_review_profile.sql"
).read_text()
ctes = profile_sql[
    profile_sql.index("WITH conditions AS (") : profile_sql.index("smoking AS (")
]
ctes = ctes.rstrip().removesuffix(",")
date_expression = re.search(
    r"LEAST_IGNORE_NULLS\(c\.earliest_depression_anxiety_date,\s*"
    r"child_depression\.earliest_diagnosis_date\) AS earliest_depression_anxiety_date",
    profile_sql,
).group(0)
db.execute(
    "CREATE TABLE diagnosis_dates AS "
    + render(
        ctes
        + f" SELECT person.person_id, {date_expression} FROM dim_person_age AS person"
        + " LEFT JOIN conditions AS c ON person.person_id = c.person_id"
        + " LEFT JOIN child_depression ON person.person_id = child_depression.person_id"
    )
)
dates = {
    p: str(date) if date else None
    for p, date in db.execute("SELECT * FROM diagnosis_dates").fetchall()
}
expected = dict.fromkeys(people)
expected.update(
    dict.fromkeys(["age10", "age17", "age18", "anxiety_only"], "2026-08-01")
)
expected.update(earlier_anxiety="2026-06-01", old_first_episode="2020-08-01")
assert dates == expected, dates

db.execute("""
    CREATE TABLE int_ltc_review_profile AS
    SELECT person_id, earliest_depression_anxiety_date,
        DATE '2026-08-02' AS latest_alcohol_screen_date,
        'FAST' AS latest_alcohol_screen_tool, 3 AS latest_alcohol_screen_score,
        DATE '2026-08-02' AS latest_positive_alcohol_screen_date,
        DATE '2026-08-03' AS latest_intervention_after_positive_screen_date,
        FALSE AS has_alcohol_disorder
    FROM diagnosis_dates
""")
table(
    "int_alcohol_screening_all",
    "id INTEGER, source_cluster_id VARCHAR, person_id VARCHAR, clinical_effective_date DATE, screening_tool VARCHAR, score_value INTEGER",
    [(i, "FAST_COD", p, "2026-08-02", "FAST", 3) for i, p in enumerate(people)],
)
table(
    "int_alcohol_intervention",
    "person_id VARCHAR, clinical_effective_date DATE, alcohol_advice_services VARCHAR",
    [(p, "2026-08-03", "Yes") for p in people],
)
pair_path = (
    ROOT
    / "models/modelling/olids/observations/int_nice_alcohol_screen_intervention.sql"
)
db.execute(
    "CREATE TABLE int_nice_alcohol_screen_intervention AS "
    + render(pair_path.read_text())
)
eligible = {"age10", "age17", "age18", "earlier_anxiety", "anxiety_only"}
for indicator in [198, 199]:
    path = (
        ROOT
        / f"models/reporting/olids/measures/nice/alcohol/fct_person_alcohol_ind{indicator}.sql"
    )
    actual = dict(
        db.execute(
            "SELECT person_id,is_in_numerator FROM (" + render(path.read_text()) + ")"
        ).fetchall()
    )
    assert actual == dict.fromkeys(eligible, True), (indicator, actual)
print(
    "33 synthetic profile/indicator population cases passed, including depression-only ages 9, 10, 17 and 18."
)
