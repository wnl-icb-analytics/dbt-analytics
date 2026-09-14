from pathlib import Path
import datetime as dt
import re
import duckdb
import jinja2

ROOT = Path(__file__).resolve().parents[3]
db = duckdb.connect()
day = dt.date(2026, 9, 12)
recent = day - dt.timedelta(days=30)
old = day - dt.timedelta(days=500)


def table(name, columns, rows=()):
    db.execute(f"create or replace table {name} ({columns})")
    if rows:
        db.executemany(
            f"insert into {name} values ({','.join('?' for _ in rows[0])})", rows
        )


def model(path):
    source = (ROOT / path).read_text(encoding="utf-8")
    env = jinja2.Environment()
    sql = env.from_string(source).render(
        config=lambda **kwargs: "",
        ref=lambda name: name,
        get_observations=lambda clusters, **kwargs: (
            "select * from synthetic_egfr_observations where cluster_id = " + clusters
            if clusters == "'EGFR_COD'"
            else "select * from synthetic_foot_observations"
        ),
    )
    sql = re.sub(
        r"DATEADD\(month,\s*(-?\d+),\s*CURRENT_DATE\(\)\)",
        lambda match: "(date '2026-09-12' + interval '" + match[1] + " months')",
        sql,
        flags=re.I,
    )
    sql = sql.replace("CURRENT_DATE()", "date '2026-09-12'")
    sql = re.sub(r"\bIFF\(", "IF(", sql, flags=re.I)
    sql = sql.replace("GREATEST_IGNORE_NULLS(", "GREATEST(")
    sql = sql.replace("REGEXP_LIKE(", "REGEXP_FULL_MATCH(")
    sql = sql.replace("BOOLOR_AGG(", "BOOL_OR(")
    sql = re.sub(
        r"DATEADD\((day|month),\s*(-?\d+),\s*([\w.]+)\)",
        lambda match: (
            "(" + match[3] + " + interval '" + match[2] + " " + match[1] + "')"
        ),
        sql,
        flags=re.I,
    )
    sql = re.sub(r" WITHIN GROUP \(ORDER BY [^)]+\)", "", sql)
    return sql


ids = range(1, 13)
table(
    "fct_person_diabetes_register",
    "person_id int, is_on_register boolean",
    [(i, True) for i in ids],
)
table("dim_person_age", "person_id int, age int", [(i, 50) for i in ids])
table(
    "dim_person_active_patients",
    "person_id int, current_practice_code varchar, current_practice_name varchar",
    [(i, "SYNTHETIC", "Synthetic practice") for i in ids],
)
table(
    "fct_person_frailty_register",
    "person_id int, latest_frailty_severity varchar",
    [(2, "Moderate")],
)
table(
    "int_fructosamine_all",
    "person_id int, clinical_effective_date date",
    [(1, recent), (2, recent), (3, recent)],
)
table(
    "int_diabetes_max_tolerated_treatment_all",
    "person_id int, clinical_effective_date date",
    [(5, recent)],
)
table(
    "int_hba1c_all",
    "person_id int, id int, clinical_effective_date date, hba1c_ifcc double, is_valid_hba1c boolean",
    [
        (1, 1, recent, 58, True),
        (2, 2, recent, 75, True),
        (5, 5, recent, 58, True),
        (6, 6, recent, 55, True),
        (6, 7, day, None, False),
    ],
)
for ind in (135, 136, 165, 179, 180):
    rows = db.execute(
        model(
            f"models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_hba1c_ind{ind}.sql"
        )
    ).fetchdf()
    found = dict(zip(rows.person_id, rows.indicator_status))
    assert 3 not in found and 5 not in found, (ind, found)
    assert (2 in found) == (ind != 179), (ind, found)
    if ind == 180:
        assert found == {2: "ACHIEVED"}, found
    else:
        assert found[1] == "ACHIEVED" and found[4] == "NOT_RECORDED_IN_PERIOD"
        assert found[6] == "NOT_ASSESSABLE"
print(
    "PASS: five HbA1c models retain simultaneous HbA1c and fructosamine, apply frailty and maximum treatment, and retain invalid latest results."
)

table(
    "int_ckd_profile",
    "person_id int, latest_acr_value double, latest_frailty_severity varchar",
    [
        (1, None, None),
        (2, 69.9, None),
        (3, 70, None),
        (4, 100, None),
        (5, 60, "Severe"),
    ],
)
table(
    "fct_person_bp_control",
    "person_id int, latest_bp_date date, latest_systolic_value int, latest_diastolic_value int, applied_measurement_context varchar",
    [(i, recent, 139, 89, "CLINIC") for i in range(1, 6)],
)
rows = db.execute(
    model("models/reporting/olids/measures/nice/ckd/fct_person_ckd_bp_ind235.sql")
).fetchdf()
assert list(rows.person_id) == [2] and bool(rows.iloc[0].is_in_numerator)
print(
    "PASS: IND235 excludes missing ACR, ACR at 70, and severe frailty; includes ACR 69.9."
)

table(
    "synthetic_foot_observations",
    "id int, person_id int, clinical_effective_date date, mapped_concept_code varchar, mapped_concept_display varchar, code_description varchar, cluster_id varchar",
    [
        (1, 1, recent, "SYN1", "Pedal pulses", "Pedal pulses", "FOOTEXAM_COD"),
        (
            2,
            2,
            recent,
            "SYN2",
            "10g monofilament sensation present",
            "10g monofilament sensation present",
            "FOOTEXAM_COD",
        ),
        (
            3,
            3,
            recent,
            "SYN3",
            "Refer to foot screener",
            "Refer to foot screener",
            "FOOTEXAM_COD",
        ),
        (
            4,
            4,
            recent,
            "SYN4",
            "10g monofilament sensation absent",
            "10g monofilament sensation absent",
            "FOOTEXAM_COD",
        ),
        (
            5,
            4,
            recent,
            "SYN5",
            "Foot examination declined",
            "Foot examination declined",
            "FEDEC_COD",
        ),
        (
            6,
            5,
            old,
            "SYN6",
            "Monofilament foot sensation test",
            "Monofilament foot sensation test",
            "FOOTEXAM_COD",
        ),
        (
            7,
            6,
            recent,
            "SYN7",
            "10g monofilament sensation present",
            "10g monofilament sensation present",
            "FOOTEXAM_COD",
        ),
        (
            8,
            6,
            recent,
            "SYN8",
            "Foot examination unsuitable",
            "Foot examination unsuitable",
            "FEPU_COD",
        ),
        (
            9,
            7,
            recent,
            "SYN9",
            "Foot examination declined",
            "Foot examination declined",
            "FEDEC_COD",
        ),
        (
            10,
            8,
            recent,
            "SYN10",
            "Foot examination unsuitable",
            "Foot examination unsuitable",
            "FEPU_COD",
        ),
    ],
)
db.execute(
    "create or replace table int_foot_examination_all as "
    + model("models/modelling/olids/observations/int_foot_examination_all.sql")
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_foot_examination_ind160.sql"
    )
).fetchdf()
assert set(rows.loc[rows.is_in_numerator, "person_id"]) == {2, 4, 6}
print(
    "PASS: IND160 retains positive monofilament tests despite same-day decline/unsuitable codes; pulse-only, referral, decline/unsuitable-only and old records fail."
)

db.executemany(
    "insert into synthetic_foot_observations values (?, ?, ?, ?, ?, ?, ?)",
    [
        (
            11 + i,
            i,
            recent,
            "SYN_RISK",
            "Diabetic foot low risk",
            "Diabetic foot low risk",
            "FRC_COD",
        )
        for i in (2, 4, 6, 9)
    ]
    + [
        (
            21,
            9,
            recent,
            "SYN_AMP",
            "Left foot amputated",
            "Left foot amputated",
            "AMPL_COD",
        )
    ],
)
db.execute(
    "create or replace table int_foot_examination_all as "
    + model("models/modelling/olids/observations/int_foot_examination_all.sql")
)
db.execute(
    "create or replace table int_foot_examination_latest as select person_id, bool_or(left_foot_amputated) left_foot_amputated, bool_or(right_foot_amputated) right_foot_amputated from int_foot_examination_all group by person_id"
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_foot_risk_ind81.sql"
    )
).fetchdf()
assert set(rows.loc[rows.is_in_numerator, "person_id"]) == {2, 4, 6}
assert 9 not in set(rows.person_id)
print(
    "PASS: IND81 keeps completed exam/risk evidence despite same-day exceptions; amputation excludes, exception-only and missing-risk records fail."
)

dates = [
    "latest_bmi_date",
    "latest_bp_date",
    "latest_hba1c_date",
    "latest_cholesterol_date",
    "latest_smoking_date",
    "latest_foot_check_date",
    "latest_acr_date",
]
table(
    "fct_person_diabetes_8_care_processes",
    "person_id int, care_processes_completed int, creatinine_completed_in_last_12m boolean, foot_check_completed_in_last_12m boolean, acr_completed_in_last_12m boolean, "
    + ", ".join(name + " date" for name in dates),
    [
        (1, 8, True, True, True, *([recent] * 7)),
        (2, 8, True, True, True, *([recent] * 7)),
        (3, 8, True, True, True, *([recent] * 7)),
        (4, 5, False, False, False, *([recent] * 7)),
    ],
)
table(
    "int_foot_examination_all",
    "person_id int, clinical_effective_date date, is_unsuitable boolean, is_declined boolean, both_feet_checked boolean, left_foot_checked boolean, right_foot_absent boolean, right_foot_amputated boolean, right_foot_checked boolean, left_foot_absent boolean, left_foot_amputated boolean",
    [
        (i, recent, i == 4, i == 2, True, True, False, False, True, False, False)
        for i in (1, 2, 3, 4)
    ]
    + [(4, day, False, True, False, False, False, False, False, False, False)],
)
table(
    "int_urine_acr_all",
    "person_id int, clinical_effective_date date, is_acr_ratio boolean, acr_value double",
    [(i, recent, True, None if i == 4 else 10) for i in (1, 2, 3, 4)],
)
table(
    "synthetic_egfr_observations",
    "id int, person_id int, clinical_effective_date timestamp, mapped_concept_code varchar, mapped_concept_display varchar, cluster_id varchar, result_value double",
    [
        (1, 1, recent, "SYN1", "Serum creatinine", "CREAT_COD", 80),
        (2, 2, recent, "SYN2", "eGFR", "EGFR_COD", 80),
        (3, 3, day + dt.timedelta(days=1), "SYN3", "eGFR", "EGFR_COD", 80),
        (4, 4, recent, "SYN4", "eGFR", "EGFR_COD", None),
        (
            5,
            5,
            dt.datetime.combine(day, dt.time(12)),
            "SYN5",
            "eGFR",
            "EGFR_COD",
            80,
        ),
        (
            6,
            6,
            dt.datetime.combine(day + dt.timedelta(days=1), dt.time(12)),
            "SYN6",
            "eGFR",
            "EGFR_COD",
            80,
        ),
    ],
)
db.execute(
    "create or replace table int_egfr_test_all as "
    + model("models/modelling/olids/observations/int_egfr_test_all.sql")
)
assert db.execute(
    "select is_result_recorded from int_egfr_test_all where person_id = 4"
).fetchone() == (False,)
assert set(db.execute("select id from int_egfr_test_all").fetchall()) == {
    (2,),
    (4,),
    (5,),
}
assert db.execute(
    "select clinical_effective_date from int_egfr_test_all where id = 5"
).fetchone() == (dt.datetime.combine(day, dt.time(12)),)
print(
    "PASS: actual eGFR test SQL retains reporting-day midday timestamps and excludes next-day evidence."
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_care_processes_ind120.sql"
    )
).fetchdf()
assert dict(zip(rows.person_id, rows.care_processes_completed_count)) == {
    1: 7,
    2: 8,
    3: 7,
    4: 8,
}
print(
    "PASS: IND120 counts eGFR tests without values, rejects creatinine-only/future evidence, and retains completed foot checks despite same-day/later decline or unsuitable codes."
)

flags = [
    "has_chd",
    "has_stroke_tia",
    "has_diabetes",
    "has_ndh",
    "has_hypertension",
    "has_pad",
    "has_heart_failure",
    "has_copd",
    "has_dyslipidaemia",
    "has_learning_disability",
    "has_obstructive_sleep_apnoea",
    "has_smi",
]
table(
    "int_ltc_review_profile",
    "person_id int, latest_bmi_date date, "
    + ", ".join(name + " boolean" for name in flags),
    [
        (i, recent, *[name == "has_dyslipidaemia" and i == 1 for name in flags])
        for i in ids
    ],
)
table(
    "int_lipid_lowering_therapy_latest",
    "person_id int, latest_order_date date",
    [(2, recent), (3, old)],
)
table(
    "int_cholesterol_ldl_latest",
    "person_id int, clinical_effective_date date, has_later_unassessable_result boolean, cholesterol_value double",
    [(4, recent, False, 4.1), (5, recent, False, 4.099), (12, recent, True, 5)],
)
table(
    "int_triglycerides_latest",
    "person_id int, clinical_effective_date date, has_later_unassessable_result boolean, triglycerides_value double",
    [(10, recent, False, 1.7)],
)
table(
    "int_cholesterol_hdl_latest",
    "person_id int, clinical_effective_date date, has_later_unassessable_result boolean, cholesterol_value double",
    [
        (6, recent, False, 0.99),
        (7, recent, False, 1),
        (8, recent, False, 1.29),
        (9, recent, False, 1.3),
        (11, recent, False, 0.5),
    ],
)
table(
    "dim_person_gender",
    "person_id int, gender varchar",
    [(6, "Male"), (7, "Male"), (8, "Female"), (9, "Female"), (11, "Unknown")],
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/bmi/fct_person_bmi_recording_ind320.sql"
    )
).fetchdf()
assert set(rows.person_id) == {2, 4, 6, 8, 10}, set(rows.person_id)
print(
    "PASS: IND320 QOF v51.3 therapy/LDL/HDL/triglyceride boundaries; unknown sex and invalid newer lipids do not qualify."
)

diagnosis = dt.date(2026, 9, 1)
second = dt.date(2026, 8, 1)
table(
    "fct_person_ckd_register",
    "person_id int, earliest_diagnosis_date date, is_on_register boolean",
    [(i, diagnosis, True) for i in (1, 2, 3)],
)
table(
    "int_egfr_all",
    "person_id int, id int, clinical_effective_date date, egfr_value double, is_valid_egfr boolean",
    [
        (1, 1, second - dt.timedelta(days=90), 55, True),
        (1, 2, second, 50, True),
        (1, 3, day, 300, False),
        (2, 4, second - dt.timedelta(days=89), 55, True),
        (2, 5, second, 50, True),
        (2, 6, day + dt.timedelta(days=1), 10, True),
        (3, 7, day + dt.timedelta(days=1), 55, True),
    ],
)
table(
    "int_urine_acr_all",
    "person_id int, id int, clinical_effective_date date, acr_value double, is_acr_ratio boolean, is_result_recorded boolean",
    [
        (1, 1, recent, 69.9, True, True),
        (1, 2, day, None, True, True),
        (2, 3, recent, 80, True, True),
        (2, 4, day + dt.timedelta(days=1), 10, True, True),
        (3, 5, day + dt.timedelta(days=1), 10, True, True),
    ],
)
table(
    "int_proteinuria_all",
    "person_id int, record_type varchar, source_cluster_id varchar",
    [
        (1, "DIABETIC_NEPHROPATHY", "DIABNEPHROP_COD"),
        (2, "PROTEINURIA", "PRT_COD"),
        (3, "MICROALBUMINURIA", "MAL_COD"),
    ],
)
table(
    "int_ras_contraindication_all",
    "person_id int, drug_class varchar, is_persisting boolean, clinical_effective_date date",
)
table("fct_person_hypertension_register", "person_id int, is_on_register boolean")
db.execute(
    "alter table fct_person_diabetes_register add column diabetes_type varchar default 'Type 2'"
)
table(
    "int_renin_angiotensin_therapy_latest",
    "person_id int, latest_order_date date, latest_ras_class varchar",
    [(i, recent, "ARB") for i in (1, 2, 3)],
)
table(
    "int_sglt2_therapy_latest",
    "person_id int, latest_order_date date, latest_sglt2_drug varchar",
)
table("int_ace_inhibitor_medications_all", "person_id int, order_date date")
table("int_arb_medications_all", "person_id int, order_date date")
rows = (
    db.execute(model("models/modelling/olids/person_attributes/int_ckd_profile.sql"))
    .fetchdf()
    .set_index("person_id")
)
assert rows.loc[1].latest_acr_value != rows.loc[1].latest_acr_value
assert rows.loc[1].latest_egfr_value != rows.loc[1].latest_egfr_value
assert bool(rows.loc[1].has_egfr_pair_before_diagnosis) and not bool(
    rows.loc[2].has_egfr_pair_before_diagnosis
)
assert rows.loc[2].latest_acr_value == 80 and rows.loc[2].latest_egfr_value == 50
assert not bool(rows.loc[3].has_acr_within_90_days_of_diagnosis) and not bool(
    rows.loc[3].has_egfr_within_90_days_of_diagnosis
)
print(
    "PASS: CKD profile retains invalid latest results, rejects future renal tests, and applies the 90-day eGFR pair boundary."
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_ras_therapy_ind134.sql"
    )
).fetchdf()
assert set(rows.person_id) == {2, 3}
print(
    "PASS: IND134 requires proteinuria or microalbuminuria; nephropathy alone does not qualify."
)

db.execute(
    "alter table fct_person_diabetes_register add column earliest_diagnosis_date date"
)
db.execute(
    "update fct_person_diabetes_register set earliest_diagnosis_date = date '2026-09-01'"
)
db.execute(
    "update fct_person_diabetes_register set earliest_diagnosis_date = date '2026-09-13' where person_id=3"
)
table(
    "int_diabetes_structured_education_all",
    "person_id int, clinical_effective_date date, record_type varchar",
    [
        (1, day, "REFERRED"),
        (2, day + dt.timedelta(days=1), "REFERRED"),
        (3, day + dt.timedelta(days=1), "REFERRED"),
    ],
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_diabetes_structured_education_ind88.sql"
    )
).fetchdf()
assert 3 not in set(rows.person_id) and set(
    rows.loc[rows.is_in_numerator, "person_id"]
) == {1}
table(
    "fct_person_ndh_register",
    "person_id int, earliest_diagnosis_date date, is_on_register boolean, has_diabetes_diagnosis boolean, is_diabetes_resolved boolean",
    [
        (1, diagnosis, True, False, False),
        (2, diagnosis, True, False, False),
        (3, day + dt.timedelta(days=1), True, False, False),
    ],
)
table(
    "int_referral_ndpp_all",
    "person_id int, clinical_effective_date date, concept_code varchar",
    [
        (1, day, "1025321000000109"),
        (2, day + dt.timedelta(days=1), "1025321000000109"),
        (3, day + dt.timedelta(days=1), "1025321000000109"),
    ],
)
rows = db.execute(
    model(
        "models/reporting/olids/measures/nice/diabetes/fct_person_ndh_prevention_programme_ind171.sql"
    )
).fetchdf()
assert 3 not in set(rows.person_id) and set(
    rows.loc[rows.is_in_numerator, "person_id"]
) == {1}
print("PASS: IND88 and IND171 reject future diagnoses and referrals.")
