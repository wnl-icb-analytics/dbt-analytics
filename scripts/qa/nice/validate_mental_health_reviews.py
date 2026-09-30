from pathlib import Path
import re
import duckdb
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[3]
con = duckdb.connect()
con.execute(
    "CREATE MACRO DATEADD(unit, n, d) AS d + CAST(n || ' ' || unit AS INTERVAL)"
)
con.execute("CREATE MACRO BOOLOR_AGG(x) AS bool_or(x)")
env = Environment()
env.globals.update(config=lambda **kw: "", ref=lambda x: x)


def render(text):
    sql = env.from_string(text).render()
    sql = sql.replace("CURRENT_DATE()", "DATE '2026-09-12'")
    return re.sub(r"DATEADD\((month|year|day),", r"DATEADD('\1',", sql, flags=re.I)


con.execute(
    "CREATE TABLE int_ltc_review_profile(person_id VARCHAR, has_copd BOOLEAN, has_heart_failure BOOLEAN, has_asthma BOOLEAN, latest_copd_review_date DATE, latest_mrc_dyspnoea_date DATE, latest_copd_exacerbation_count_date DATE, latest_heart_failure_review_date DATE, latest_nyha_date DATE, latest_medication_review_date DATE, latest_asthma_review_date DATE, latest_complete_asthma_review_date DATE)"
)
cases = [
    ("all_recent", "2026-09-01", "2026-09-01", "2026-09-01"),
    ("missing_assessment", "2026-09-01", None, "2026-09-01"),
    ("missing_other", "2026-09-01", "2026-09-01", None),
    ("stale_assessment", "2026-09-01", "2025-09-11", "2026-09-01"),
    ("stale_other", "2026-09-01", "2026-09-01", "2025-09-11"),
    ("boundary", "2025-09-12", "2025-09-12", "2025-09-12"),
    ("missing_review", None, "2026-09-01", "2026-09-01"),
]
for name, review, assessment, other in cases:
    con.execute(
        "INSERT INTO int_ltc_review_profile VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            name,
            True,
            True,
            True,
            review,
            assessment,
            other,
            review,
            assessment,
            other,
            review,
            review if assessment and other else None,
        ],
    )
con.execute(
    "CREATE TABLE dim_person_age AS SELECT person_id, 30 AS age FROM int_ltc_review_profile"
)
con.execute(
    "CREATE TABLE dim_person_active_patients AS SELECT person_id, 'SYNTHETIC' AS current_practice_code, 'Synthetic practice' AS current_practice_name FROM int_ltc_review_profile"
)
expected = {
    "all_recent": True,
    "missing_assessment": False,
    "missing_other": False,
    "stale_assessment": False,
    "stale_other": False,
    "boundary": True,
    "missing_review": False,
}
for model in [
    "fct_person_copd_review_ind191",
    "fct_person_heart_failure_review_ind195",
]:
    sql = render(
        (
            ROOT / "models/reporting/olids/measures/nice/ltc_reviews" / f"{model}.sql"
        ).read_text()
    )
    actual = dict(
        con.execute("SELECT person_id,is_in_numerator FROM (" + sql + ")").fetchall()
    )
    assert actual == expected, (model, actual)
    print(model + ": 7 synthetic component/window cases passed")
con.execute(
    "CREATE TABLE int_ltc_review_all(person_id VARCHAR,clinical_effective_date DATE,review_type VARCHAR)"
)
events = {
    "valid": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_ACTION_PLAN"),
        ("2026-08-02", "ASTHMA_EXACERBATION_COUNT"),
    ],
    "missing_plan": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_EXACERBATION_COUNT"),
    ],
    "different_day": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-02", "ASTHMA_ACTION_PLAN"),
        ("2026-09-01", "ASTHMA_EXACERBATION_COUNT"),
    ],
    "month_boundary": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_ACTION_PLAN"),
        ("2026-08-01", "ASTHMA_EXACERBATION_COUNT"),
    ],
    "after_review": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_ACTION_PLAN"),
        ("2026-09-02", "ASTHMA_EXACERBATION_COUNT"),
    ],
    "older_complete": [
        ("2026-06-01", "ASTHMA_REVIEW"),
        ("2026-06-01", "ASTHMA_ACTION_PLAN"),
        ("2026-06-01", "ASTHMA_EXACERBATION_COUNT"),
        ("2026-09-01", "ASTHMA_REVIEW"),
    ],
    "duplicate_events": [
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_REVIEW"),
        ("2026-09-01", "ASTHMA_ACTION_PLAN"),
        ("2026-09-01", "ASTHMA_EXACERBATION_COUNT"),
    ],
}
for person, records in events.items():
    con.executemany(
        "INSERT INTO int_ltc_review_all VALUES (?,?,?)",
        [(person, *rec) for rec in records],
    )
profile = (
    ROOT / "models/modelling/olids/person_attributes/int_ltc_review_profile.sql"
).read_text()
ctes = (
    profile[profile.index("asthma_review_dates AS (") : profile.index("mrc AS (")]
    .rstrip()
    .removesuffix(",")
)
actual = {
    p: str(d)
    for p, d in con.execute(
        render("WITH " + ctes + " SELECT * FROM complete_asthma_review")
    ).fetchall()
}
assert actual == {
    "valid": "2026-09-01",
    "older_complete": "2026-06-01",
    "duplicate_events": "2026-09-01",
}, actual
print("Asthma complete-review CTE: 7 synthetic timing/grain cases passed")
# Run the actual indicator with the CTE-derived completion dates.
con.execute("DELETE FROM int_ltc_review_profile")
con.execute("DELETE FROM dim_person_age")
con.execute("DELETE FROM dim_person_active_patients")
for person in events:
    con.execute(
        "INSERT INTO int_ltc_review_profile VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            person,
            False,
            False,
            True,
            None,
            None,
            None,
            None,
            None,
            None,
            "2026-09-01",
            actual.get(person),
        ],
    )
    con.execute("INSERT INTO dim_person_age VALUES (?,30)", [person])
    con.execute(
        "INSERT INTO dim_person_active_patients VALUES (?,'SYNTHETIC','Synthetic practice')",
        [person],
    )
sql = render(
    (
        ROOT
        / "models/reporting/olids/measures/nice/ltc_reviews/fct_person_asthma_review_ind273.sql"
    ).read_text()
)
result = dict(
    con.execute("SELECT person_id,is_in_numerator FROM (" + sql + ")").fetchall()
)
assert result == {p: p in actual for p in events}, result
print("fct_person_asthma_review_ind273: 7 synthetic complete-review cases passed")
print("All 28 synthetic SQL cases passed; no warehouse data used")

con.execute(
    "CREATE TABLE synthetic_smoking_observations(person_id VARCHAR, clinical_effective_date TIMESTAMP, cluster_id VARCHAR)"
)
con.execute(
    "CREATE TABLE stg_nhsd_snomed_sct_refset_simple(ref_set_id BIGINT, active BOOLEAN, referenced_component_id VARCHAR)"
)
con.execute(
    "INSERT INTO stg_nhsd_snomed_sct_refset_simple VALUES (12465801000001106,TRUE,'synthetic_drug'),(12465801000001106,FALSE,'inactive_drug'),(1,TRUE,'other_drug')"
)
con.execute(
    "CREATE TABLE stg_olids_observation(person_id VARCHAR, clinical_effective_date DATE, date_recorded DATE, mapped_concept_code VARCHAR)"
)
con.execute(
    "CREATE TABLE stg_olids_medication_order(patient_id VARCHAR, clinical_effective_date TIMESTAMP, mapped_concept_code VARCHAR)"
)
con.execute(
    "CREATE TABLE int_patient_person_unique(patient_id VARCHAR,person_id VARCHAR)"
)
con.executemany(
    "INSERT INTO synthetic_smoking_observations VALUES (?,?,?)",
    [
        ("referral", "2026-09-01", "REFERSSSA_COD"),
        ("pharmacotherapy", "2026-09-01", "PHARM_COD"),
        ("generic_advice", "2026-09-01", "SMOKINGINT_COD"),
        ("declined", "2026-09-01", "SMOKPCADEC_COD"),
        ("future_referral", "2026-09-13", "REFERSSSA_COD"),
        ("same_day_referral", "2026-09-12 14:00:00", "REFERSSSA_COD"),
    ],
)
con.executemany(
    "INSERT INTO stg_olids_observation VALUES (?,?,?,?)",
    [
        ("drug_observation", "2026-09-01", "2026-09-01", "synthetic_drug"),
        ("inactive_drug_observation", "2026-09-01", "2026-09-01", "inactive_drug"),
        ("other_refset_observation", "2026-09-01", "2026-09-01", "other_drug"),
        ("corrected_observation", "2026-09-13", "2026-09-01", "synthetic_drug"),
        ("future_drug_observation", "2026-09-13", "2026-09-13", "synthetic_drug"),
    ],
)
for person, date, code in [
    ("medication_only", "2026-09-01", "synthetic_drug"),
    ("future_medication", "2026-09-13", "synthetic_drug"),
    ("inactive_medication", "2026-09-01", "inactive_drug"),
    ("same_day_medication", "2026-09-12 14:00:00", "synthetic_drug"),
]:
    con.execute(
        "INSERT INTO stg_olids_medication_order VALUES (?,?,?)",
        ["patient_" + person, date, code],
    )
    con.execute(
        "INSERT INTO int_patient_person_unique VALUES (?,?)",
        ["patient_" + person, person],
    )
env.globals.update(
    get_observations=lambda clusters, source: (
        f"SELECT * FROM synthetic_smoking_observations WHERE cluster_id IN ({clusters})"
    )
)
ctes = (
    profile[
        profile.index("smoking_pharmacotherapy_codes AS (") : profile.index("bmi AS (")
    ]
    .rstrip()
    .removesuffix(",")
)
actual = {
    p: str(dt)
    for p, dt in con.execute(
        render("WITH " + ctes + " SELECT * FROM smoking_intervention")
    ).fetchall()
}
expected = dict.fromkeys(
    [
        "referral",
        "pharmacotherapy",
        "drug_observation",
        "medication_only",
        "corrected_observation",
    ],
    "2026-09-01",
)
expected.update(
    dict.fromkeys(["same_day_referral", "same_day_medication"], "2026-09-12")
)
assert actual == expected, actual
assert not con.execute(
    render(
        (ROOT / "tests/nice_smoking_pharmacotherapy_refset_available.sql").read_text()
    )
).fetchall()
con.execute("UPDATE stg_nhsd_snomed_sct_refset_simple SET active = FALSE")
assert con.execute(
    render(
        (ROOT / "tests/nice_smoking_pharmacotherapy_refset_available.sql").read_text()
    )
).fetchall() == [(12465801000001106,)]
print(
    "Smoking support CTE: 15 synthetic evidence/refset/date cases and 2 missing-refset checks passed"
)
con.execute("DROP TABLE int_ltc_review_profile")
con.execute("DELETE FROM dim_person_age")
con.execute("DELETE FROM dim_person_active_patients")
con.execute(
    "CREATE TABLE int_ltc_review_profile(person_id VARCHAR, has_dementia BOOLEAN, earliest_dementia_diagnosis_date DATE, latest_dementia_care_plan_date DATE)"
)
for person, review in [
    ("before", "2026-08-31"),
    ("same_day", "2026-09-01"),
    ("after", "2026-09-02"),
    ("stale", "2025-09-11"),
    ("missing", None),
]:
    con.execute(
        "INSERT INTO int_ltc_review_profile VALUES (?,TRUE,?,?)",
        [person, "2026-09-01", review],
    )
    con.execute("INSERT INTO dim_person_age VALUES (?,75)", [person])
    con.execute(
        "INSERT INTO dim_person_active_patients VALUES (?,'SYNTHETIC','Synthetic practice')",
        [person],
    )
sql = render(
    (
        ROOT
        / "models/reporting/olids/measures/nice/ltc_reviews/fct_person_dementia_care_plan_ind142.sql"
    ).read_text()
)
actual = dict(
    con.execute("SELECT person_id,is_in_numerator FROM (" + sql + ")").fetchall()
)
assert actual == {
    "before": False,
    "same_day": True,
    "after": True,
    "stale": False,
    "missing": False,
}, actual
print("fct_person_dementia_care_plan_ind142: 5 synthetic diagnosis/window cases passed")
con.execute("DROP TABLE int_ltc_review_profile")
con.execute("DELETE FROM dim_person_age")
con.execute("DELETE FROM dim_person_active_patients")
con.execute("CREATE MACRO IFF(c,t,f) AS CASE WHEN c THEN t ELSE f END")
con.execute("CREATE MACRO DATE_FROM_PARTS(y,m,d) AS make_date(y,m,d)")
con.execute(
    "CREATE TABLE int_ltc_review_profile(person_id VARCHAR, earliest_smi_diagnosis_date DATE, has_active_smi_diagnosis BOOLEAN, has_chd BOOLEAN, has_pad BOOLEAN, has_stroke_tia BOOLEAN, has_hypertension BOOLEAN, has_diabetes BOOLEAN, has_copd BOOLEAN, has_ckd BOOLEAN, has_asthma BOOLEAN, latest_smoking_status VARCHAR, latest_smoking_status_date DATE, latest_never_smoked_date DATE, latest_smoking_intervention_date DATE, birth_date_approx DATE, earliest_smoking_smi_ltc_diagnosis_date DATE, latest_blood_pressure_date DATE, latest_bmi_date DATE, latest_alcohol_record_date DATE, latest_lipid_date DATE, latest_glucose_or_hba1c_date DATE)"
)
for person, dx, active, chd in [
    ("active_smi", "2020-01-01", True, False),
    ("remitted_smi", "2020-01-01", False, False),
    ("lithium_only", None, False, False),
    ("chd_only", None, False, True),
]:
    con.execute(
        "INSERT INTO int_ltc_review_profile VALUES ("
        + ",".join("?" for _ in range(22))
        + ")",
        [
            person,
            dx,
            active,
            chd,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            "Current Smoker",
            "2026-09-01",
            None,
            None,
            "1980-01-01",
            dx,
            *(["2026-09-01"] * 5),
        ],
    )
    con.execute("INSERT INTO dim_person_age VALUES (?,46)", [person])
    con.execute(
        "INSERT INTO dim_person_active_patients VALUES (?,'SYNTHETIC','Synthetic practice')",
        [person],
    )
for family, model, expected in [
    ("smoking", "fct_person_smoking_ind97", {"active_smi", "remitted_smi", "chd_only"}),
    ("smi", "fct_person_smi_physical_health_ind248", {"active_smi", "remitted_smi"}),
]:
    sql = render(
        (
            ROOT / "models/reporting/olids/measures/nice" / family / f"{model}.sql"
        ).read_text()
    )
    actual = {
        p for (p,) in con.execute("SELECT person_id FROM (" + sql + ")").fetchall()
    }
    assert actual == expected, (model, actual)
    print(model + ": 4 synthetic diagnosis/remission/lithium cases passed")
print("All 58 synthetic SQL cases passed; no warehouse data used")

con.execute(
    "CREATE TABLE synthetic_alcohol_observation(id VARCHAR,person_id VARCHAR,clinical_effective_date DATE,cluster_id VARCHAR,result_value VARCHAR,mapped_concept_code VARCHAR,mapped_concept_display VARCHAR)"
)
for n, (person, dt, cluster, score) in enumerate(
    [
        ("audit_only", "2026-09-01", "AUDIT_COD", "8"),
        ("fast_boundary", "2026-08-01", "FAST_COD", "3"),
        ("auditc_boundary", "2026-08-01", "AUDITC_COD", "5"),
        ("fast_negative", "2026-09-01", "FAST_COD", "2"),
        ("auditc_negative", "2026-09-01", "AUDITC_COD", "4"),
        ("later_audit", "2026-07-01", "FAST_COD", "3"),
        ("later_audit", "2026-09-01", "AUDIT_COD", "8"),
    ]
):
    con.execute(
        "INSERT INTO synthetic_alcohol_observation VALUES (?,?,?,?,?,?,?)",
        [str(n), person, dt, cluster, score, "SYNTHETIC", "Synthetic screening"],
    )
env.globals.update(
    get_observations=lambda clusters, source: (
        "SELECT * FROM synthetic_alcohol_observation"
    )
)
con.execute(
    "CREATE TABLE int_alcohol_screening_all AS "
    + render(
        (
            ROOT / "models/modelling/olids/observations/int_alcohol_screening_all.sql"
        ).read_text()
    )
)
profile = (
    ROOT / "models/modelling/olids/person_attributes/int_ltc_review_profile.sql"
).read_text()
cte = (
    profile[
        profile.index("latest_positive AS (") : profile.index("brief_intervention AS (")
    ]
    .rstrip()
    .removesuffix(",")
)
actual = {
    p: str(d)
    for p, d in con.execute(
        render("WITH " + cte + " SELECT * FROM latest_positive")
    ).fetchall()
}
assert actual == {
    "fast_boundary": "2026-08-01",
    "auditc_boundary": "2026-08-01",
    "later_audit": "2026-07-01",
}, actual
print(
    "NICE latest_positive alcohol CTE: 6 synthetic screen-type/precedence cases passed"
)
con.execute("DROP TABLE int_ltc_review_profile")
con.execute("DELETE FROM dim_person_age")
con.execute("DELETE FROM dim_person_active_patients")
con.execute(
    "CREATE TABLE int_ltc_review_profile(person_id VARCHAR,latest_new_depression_diagnosis_date DATE,first_depression_review_10_to_35_days_date DATE)"
)
for asof, start, previous in [
    ("2026-03-31", "2025-04-01", "2025-03-31"),
    ("2026-04-01", "2026-04-01", "2026-03-31"),
]:
    con.execute("DELETE FROM int_ltc_review_profile")
    con.execute("DELETE FROM dim_person_age")
    con.execute("DELETE FROM dim_person_active_patients")
    for person, dx, age in [
        ("before_start", previous, 18),
        ("at_start", start, 18),
        ("current_date", asof, 18),
        ("under18", start, 17),
    ]:
        con.execute(
            "INSERT INTO int_ltc_review_profile VALUES (?,?,NULL)", [person, dx]
        )
        con.execute("INSERT INTO dim_person_age VALUES (?,?)", [person, age])
        con.execute(
            "INSERT INTO dim_person_active_patients VALUES (?,'SYNTHETIC','Synthetic practice')",
            [person],
        )
    sql = render(
        (
            ROOT
            / "models/reporting/olids/measures/nice/ltc_reviews/fct_person_depression_review_ind104.sql"
        ).read_text()
    ).replace("DATE '2026-09-12'", f"DATE '{asof}'")
    actual = {
        p: str(d)
        for p, d in con.execute(
            "SELECT person_id,measurement_period_start FROM (" + sql + ")"
        ).fetchall()
    }
    assert actual == {"at_start": start, "current_date": start}, (asof, actual)
    print(
        "fct_person_depression_review_ind104: 4 financial-year/age cases passed at "
        + asof
    )
print("All 72 synthetic SQL cases passed; no warehouse data used")
