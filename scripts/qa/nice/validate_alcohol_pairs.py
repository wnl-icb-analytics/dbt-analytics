from pathlib import Path
import re
import duckdb
import jinja2
import yaml

root = Path(__file__).resolve().parents[3]
db = duckdb.connect()


def table(name, schema, rows):
    db.execute(f"CREATE OR REPLACE TABLE {name} ({schema})")
    if rows:
        db.executemany(
            f"INSERT INTO {name} VALUES ({','.join('?' for _ in rows[0])})", rows
        )


def build(name):
    path = next(root.glob(f"models/**/{name}.sql"))
    sql = (
        jinja2.Environment()
        .from_string(path.read_text())
        .render(config=lambda **kw: "", ref=lambda name: name)
    )
    sql = sql.replace("CURRENT_DATE()", "DATE '2026-09-12'")
    sql = re.sub(
        r"DATEADD\(month,\s*(-?\d+),\s*([^()]+)\)",
        lambda m: f"({m[2]} + INTERVAL '{m[1]} months')",
        sql,
        flags=re.I,
    )
    db.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")


screens = [
    ("earlier", "2026-06-01", "FAST", 3),
    ("earlier", "2026-09-01", "FAST", 3),
    ("wrong_tool", "2026-06-01", "AUDIT", 20),
    ("fast_low", "2026-06-01", "FAST", 2),
    ("auditc_low", "2026-06-01", "AUDIT-C", 4),
    # An unmatched earliest screen must not hide a later completed pair.
    ("auditc_ok", "2025-12-01", "AUDIT-C", 5),
    ("auditc_ok", "2026-06-01", "AUDIT-C", 5),
    ("exact_three", "2026-05-31", "FAST", 3),
    ("after_three", "2026-06-01", "FAST", 3),
    ("before", "2026-06-01", "FAST", 3),
    ("same_day", "2026-06-01", "FAST", 3),
    ("future_intervention", "2026-09-01", "FAST", 3),
    ("future_screen", "2026-09-13", "FAST", 3),
    ("declined", "2026-06-01", "FAST", 3),
    ("old_pair", "2025-06-01", "FAST", 3),
    ("ancient_pair", "2020-06-01", "FAST", 3),
    ("outside_window", "2024-06-01", "FAST", 3),
    ("outside_window", "2026-09-01", "FAST", 3),
    ("disorder", "2026-06-01", "FAST", 3),
    ("under10", "2026-06-01", "FAST", 3),
    ("old_diagnosis", "2026-06-01", "FAST", 3),
    ("smi_active", "2026-06-01", "FAST", 3),
    ("smi_remitted", "2026-06-01", "FAST", 3),
    ("lithium_only", "2026-06-01", "FAST", 3),
]
people = sorted({p for p, *_ in screens})
table(
    "int_alcohol_screening_all",
    "id INTEGER, source_cluster_id VARCHAR, person_id VARCHAR, clinical_effective_date DATE, screening_tool VARCHAR, score_value FLOAT",
    [
        (
            i,
            "FAST_COD"
            if tool == "FAST"
            else "AUDITC_COD"
            if tool == "AUDIT-C"
            else "AUDIT_COD",
            p,
            date,
            tool,
            score,
        )
        for i, (p, date, tool, score) in enumerate(screens)
    ],
)
interventions = [
    (p, "2026-06-02", "Yes")
    for p in people
    if p
    in [
        "earlier",
        "wrong_tool",
        "fast_low",
        "auditc_low",
        "auditc_ok",
        "disorder",
        "under10",
        "old_diagnosis",
        "smi_active",
        "smi_remitted",
        "lithium_only",
    ]
]
interventions += [
    ("exact_three", "2026-08-31", "Yes"),
    ("after_three", "2026-09-02", "Yes"),
    ("before", "2026-05-31", "Yes"),
    ("same_day", "2026-06-01", "Yes"),
    ("future_intervention", "2026-09-13", "Yes"),
    ("declined", "2026-06-02", "Declined"),
    ("old_pair", "2025-07-01", "Yes"),
    ("ancient_pair", "2020-07-01", "Yes"),
    ("outside_window", "2024-07-01", "Yes"),
]
table(
    "int_alcohol_intervention",
    "person_id VARCHAR, clinical_effective_date DATE, alcohol_advice_services VARCHAR",
    interventions,
)
profile = []
for p in people:
    positive = [
        date
        for person, date, tool, score in screens
        if person == p
        and date <= "2026-09-12"
        and ((tool == "FAST" and score >= 3) or (tool == "AUDIT-C" and score >= 5))
    ]
    latest = max(positive) if positive else None
    diagnosis = "2020-01-01" if p == "old_diagnosis" else "2026-07-01"
    profile.append(
        (
            p,
            diagnosis,
            diagnosis,
            latest,
            latest,
            "FAST",
            3,
            None,
            p == "disorder",
            True,
            True,
            False,
            False,
            False,
            False,
            False,
            None if p == "lithium_only" else "2020-01-01",
            p not in ["lithium_only", "smi_remitted"],
        )
    )
table(
    "int_ltc_review_profile",
    "person_id VARCHAR, earliest_hypertension_date DATE, earliest_depression_anxiety_date DATE, latest_positive_alcohol_screen_date DATE, latest_alcohol_screen_date DATE, latest_alcohol_screen_tool VARCHAR, latest_alcohol_screen_score FLOAT, latest_intervention_after_positive_screen_date DATE, has_alcohol_disorder BOOLEAN, has_smi BOOLEAN, has_chd BOOLEAN, has_atrial_fibrillation BOOLEAN, has_heart_failure BOOLEAN, has_stroke_tia BOOLEAN, has_diabetes BOOLEAN, has_dementia BOOLEAN, earliest_smi_diagnosis_date DATE, has_active_smi_diagnosis BOOLEAN",
    profile,
)
table(
    "dim_person_age",
    "person_id VARCHAR, age INTEGER",
    [(p, 9 if p == "under10" else 50) for p in people],
)
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
build("int_nice_alcohol_screen_intervention")
assert (
    db.execute("SELECT COUNT(*) FROM int_nice_alcohol_screen_intervention").fetchone()[
        0
    ]
    == 20
)
assert (
    db.execute(
        "SELECT COUNT(*) FROM int_nice_alcohol_screen_intervention WHERE person_id IN ('wrong_tool','fast_low','auditc_low','future_screen')"
    ).fetchone()[0]
    == 0
)
checks = 2
for ind in [197, 199, 200, 202]:
    build(f"fct_person_alcohol_ind{ind}")
    result = dict(
        db.execute(
            f"SELECT person_id,is_in_numerator FROM fct_person_alcohol_ind{ind}"
        ).fetchall()
    )
    for p in ["earlier", "auditc_ok", "exact_three", "same_day"]:
        assert result[p] is True, (ind, p, result[p])
        checks += 1
    for p in ["after_three", "before", "future_intervention", "declined"]:
        assert result[p] is False, (ind, p, result[p])
        checks += 1
    for p in ["wrong_tool", "fast_low", "auditc_low", "future_screen", "disorder"]:
        assert p not in result, (ind, p)
        checks += 1
    assert ("under10" in result) == (ind != 199)
    checks += 1
    assert ("old_diagnosis" in result) == (ind in [200, 202])
    checks += 1
    assert ("old_pair" in result) == (ind in [197, 202])
    checks += 1
    if ind in [197, 202]:
        assert result["old_pair"] is True
        checks += 1
    assert ("ancient_pair" in result) == (ind == 197)
    checks += 1
    if ind == 197:
        assert result["ancient_pair"] is True
        checks += 1
    assert result["outside_window"] == (ind == 197)
    checks += 1
    if ind == 200:
        assert "lithium_only" not in result
        assert result["smi_active"] is True
        assert result["smi_remitted"] is True
        checks += 3
    # Latest-screen detail stays unchanged when an older completed pair succeeds.
    assert db.execute(
        f"SELECT latest_intervention_after_positive_screen_date IS NULL, latest_record_date=DATE '2026-06-02' FROM fct_person_alcohol_ind{ind} WHERE person_id='earlier'"
    ).fetchone() == (True, True)
    checks += 1
yaml.safe_load(
    (
        root
        / "models/reporting/olids/measures/nice/alcohol/fct_person_alcohol_nice_indicators.yml"
    ).read_text()
)
yaml.safe_load(
    (
        root
        / "models/modelling/olids/observations/int_nice_alcohol_screen_intervention.yml"
    ).read_text()
)
print(
    f"{checks} alcohol-pair assertions passed using the new shared model and all four actual indicator models."
)
