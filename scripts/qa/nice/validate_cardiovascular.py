"""Execute the changed repository SQL against wholly synthetic DuckDB fixtures."""

from pathlib import Path
import re
import duckdb
import jinja2
import yaml

ROOT = Path(__file__).resolve().parents[3]
DB = duckdb.connect()
ENV = jinja2.Environment()
CHECKS = 0
BUILDS = 0


def table(name, schema, rows=()):
    DB.execute(f"CREATE OR REPLACE TABLE {name} ({schema})")
    if rows:
        DB.executemany(
            f"INSERT INTO {name} VALUES ({','.join('?' for _ in rows[0])})", rows
        )


def render(name, observation_table="mock_observations"):
    path = next(ROOT.glob(f"models/**/{name}.sql"))
    sql = ENV.from_string(path.read_text()).render(
        config=lambda **kwargs: "",
        ref=lambda name: name,
        get_observations=lambda *args, **kwargs: f"SELECT * FROM {observation_table}",
    )
    sql = sql.replace("CURRENT_DATE()", "DATE '2026-09-12'")
    sql = re.sub(
        r"DATEADD\(month,\s*(-?\d+),\s*([^()]+)\)",
        lambda m: f"({m[2]} + INTERVAL '{m[1]} months')",
        sql,
        flags=re.I,
    )
    sql = sql.replace("GREATEST_IGNORE_NULLS(", "GREATEST(").replace(
        "LEAST_IGNORE_NULLS(", "LEAST("
    )
    sql = sql.replace("BOOLOR_AGG(", "BOOL_OR(").replace("MAX_BY(", "ARG_MAX(")
    return sql


def build(name, observation_table="mock_observations"):
    global BUILDS
    BUILDS += 1
    DB.execute(f"CREATE OR REPLACE TABLE {name} AS {render(name, observation_table)}")


def check(actual, expected, label):
    global CHECKS
    assert actual == expected, (label, actual, expected)
    CHECKS += 1


def outcomes(name):
    return dict(DB.execute(f"SELECT person_id, is_in_numerator FROM {name}").fetchall())


# AF latest-score, old CHADS2 and sequential treatment rules.
people = [
    "old_high_then_low",
    "old_high",
    "reassessed",
    "invalid_last",
    "chads_after_2015",
    "chads_before_2015",
    "aps_doac",
    "valvular_doac",
    "valvular_vka",
    "old_decline",
    "recent_decline",
    "ni_ttr64",
    "ni_ttr65",
    "ni_old_ttr",
    "generic_contra",
]
table(
    "fct_person_atrial_fibrillation_register",
    "person_id VARCHAR, earliest_diagnosis_date DATE, is_on_register BOOLEAN",
    [(p, "2010-01-01", True) for p in people],
)
table("dim_person_age", "person_id VARCHAR, age INTEGER", [(p, 60) for p in people])
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
scores = [(p, p, "2026-05-01", "CHA2DS2-VASc", 2) for p in people[6:]]
scores += [
    ("old1", "old_high_then_low", "2023-01-01", "CHA2DS2-VASc", 4),
    ("old2", "old_high_then_low", "2025-01-01", "CHA2DS2-VASc", 1),
    ("old3", "old_high", "2025-01-01", "CHA2DS2-VASc", 4),
    ("old4", "reassessed", "2024-01-01", "CHA2DS2-VASc", 4),
    ("old5", "reassessed", "2026-01-01", "CHA2DS2-VASc", 2),
    ("old6", "invalid_last", "2025-01-01", "CHA2DS2-VASc", 4),
    ("old7", "invalid_last", "2026-06-01", "CHA2DS2-VASc", None),
    ("old8", "chads_after_2015", "2018-01-01", "CHADS2", 3),
    ("old9", "chads_before_2015", "2014-01-01", "CHADS2", 3),
]
table(
    "int_stroke_risk_score_all",
    "id VARCHAR, person_id VARCHAR, clinical_effective_date DATE, score_type VARCHAR, score_value INTEGER",
    scores,
)
table(
    "int_antithrombotic_therapy_latest",
    "person_id VARCHAR, latest_anticoagulant_order_date DATE, latest_anticoagulant_type VARCHAR, latest_doac_order_date DATE, latest_vka_order_date DATE",
    [
        (
            p,
            "2026-08-01",
            "DOAC" if p.endswith("doac") else "VKA",
            "2026-08-01" if p.endswith("doac") else None,
            None if p.endswith("doac") else "2026-08-01",
        )
        for p in people
    ],
)
exceptions = [
    ("aps_doac", "ANTIPHOSPHOLIPID_SYNDROME", "2020-01-01"),
    ("valvular_doac", "VALVULAR_AF", "2020-01-01"),
    ("valvular_vka", "VALVULAR_AF", "2020-01-01"),
    ("old_decline", "DOAC_DECLINED", "2020-01-01"),
    ("recent_decline", "DOAC_DECLINED", "2026-01-01"),
    ("generic_contra", "ANTICOAGULANT_PERSISTING_CONTRAINDICATION", "2020-01-01"),
]
exceptions += [
    (p, "DOAC_NOT_INDICATED", "2026-01-01")
    for p in ["ni_ttr64", "ni_ttr65", "ni_old_ttr"]
]
table(
    "int_anticoagulant_exception_all",
    "person_id VARCHAR, exception_type VARCHAR, clinical_effective_date DATE",
    exceptions,
)
table("int_anticoagulant_review_all", "person_id VARCHAR, clinical_effective_date DATE")
table(
    "mock_ttr",
    "person_id VARCHAR, id VARCHAR, result_value VARCHAR, clinical_effective_date DATE",
    [
        ("ni_ttr64", "t1", "64", "2026-07-01"),
        ("ni_ttr65", "t2", "65", "2026-07-01"),
        ("ni_old_ttr", "t3", "80", "2025-07-01"),
    ],
)
build("int_atrial_fibrillation_profile", "mock_ttr")
for ind in [127, 128, 169, 247]:
    build(f"fct_person_atrial_fibrillation_ind{ind}")
af127 = outcomes("fct_person_atrial_fibrillation_ind127")
check(af127.get("old_high_then_low"), False, "latest low restores AF127 denominator")
check("old_high" in af127, False, "unreassessed latest high excluded")
check(af127.get("reassessed"), True, "recent reassessment succeeds")
check(
    af127.get("invalid_last"),
    True,
    "coded assessment still establishes AF127 assessment",
)
af128 = outcomes("fct_person_atrial_fibrillation_ind128")
check("invalid_last" in af128, False, "last invalid score cannot use older high score")
check("chads_after_2015" in af128, False, "post-2015 CHADS2 not eligible")
check(af128.get("chads_before_2015"), True, "legacy CHADS2 eligible")
check("generic_contra" in af128, False, "generic persisting contraindication excluded")
af247 = outcomes("fct_person_atrial_fibrillation_ind247")
for p, expected in [
    ("aps_doac", True),
    ("valvular_doac", False),
    ("valvular_vka", True),
    ("old_decline", False),
    ("recent_decline", True),
    ("ni_ttr64", False),
    ("ni_ttr65", True),
    ("ni_old_ttr", False),
]:
    check(af247[p], expected, "AF247 " + p)

# CVD profile uses general QOF risk tools, and preserves narrow versus all-stroke CVD definitions.
people = [
    "age24",
    "age25",
    "age84",
    "age85",
    "low_then_high",
    "high_then_low",
    "haemorrhage",
    "jbs",
    "generic_result",
    "invalid_score",
]
table("dim_person", "person_id VARCHAR", [(p,) for p in people])
table(
    "dim_person_age",
    "person_id VARCHAR, age INTEGER",
    [(p, int(p[3:]) if p.startswith("age") else 60) for p in people],
)
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
table(
    "int_qrisk_all",
    "person_id VARCHAR, id VARCHAR, clinical_effective_date DATE, qrisk_score FLOAT, qrisk_type VARCHAR, is_valid_qrisk BOOLEAN",
)
assessments = [
    (p, p, "2026-08-01", 12, "12", True)
    for p in people
    if p not in ["low_then_high", "high_then_low", "generic_result", "invalid_score"]
]
assessments += [
    ("low_then_high", "r1", "2026-01-01", 5, "5", True),
    ("low_then_high", "r2", "2026-02-01", 15, "15", True),
    ("high_then_low", "r3", "2026-01-01", 15, "15", True),
    ("high_then_low", "r4", "2026-02-01", 5, "5", True),
    ("generic_result", "r5", "2026-03-01", 15, "15", False),
    ("invalid_score", "r6", "2026-03-01", 15, "15", True),
    ("invalid_score", "r7", "2026-04-01", 999, "999", True),
]
table(
    "int_cvd_risk_assessment_all",
    "person_id VARCHAR, id VARCHAR, clinical_effective_date DATE, risk_score_value FLOAT, original_result_value VARCHAR, is_cvd_risk_score_code BOOLEAN",
    assessments,
)
table(
    "int_cvd_secondary_prevention_population",
    "person_id VARCHAR, has_chd BOOLEAN, has_pad BOOLEAN, has_stroke_tia BOOLEAN, has_haemorrhagic_stroke BOOLEAN",
    [("haemorrhage", False, False, True, True)],
)
for reg in ["familial_hypercholesterolaemia", "ckd", "obesity"]:
    table(
        "fct_person_" + reg + "_register", "person_id VARCHAR, is_on_register BOOLEAN"
    )
table(
    "fct_person_diabetes_register",
    "person_id VARCHAR, is_on_register BOOLEAN, diabetes_type VARCHAR, earliest_type2_date DATE",
    [(p, True, "Type 2", "2026-06-01") for p in people],
)
table(
    "fct_person_hypertension_register",
    "person_id VARCHAR, is_on_register BOOLEAN, earliest_diagnosis_date DATE",
)
table(
    "fct_person_frailty_register", "person_id VARCHAR, latest_frailty_severity VARCHAR"
)
table(
    "int_lipid_lowering_therapy_latest",
    "person_id VARCHAR, latest_order_date DATE, latest_statin_order_date DATE, latest_lipid_lowering_class VARCHAR, latest_product_name VARCHAR, is_latest_order_statin BOOLEAN",
)
table("fct_person_smoking_status", "person_id VARCHAR, smoking_status VARCHAR")
table(
    "int_cholesterol_latest",
    "person_id VARCHAR, cholesterol_value FLOAT, clinical_effective_date DATE",
)
build("int_cvd_risk_profile")
for ind in [229, 274, 275, 287]:
    build(f"fct_person_lipid_lowering_therapy_ind{ind}")
for ind in [161, 181, 269, 270]:
    build(f"fct_person_cvd_risk_assessment_ind{ind}")
for ind in [229, 274]:
    result = outcomes(f"fct_person_lipid_lowering_therapy_ind{ind}")
    for p, expected in [
        ("age24", False),
        ("age25", True),
        ("age84", True),
        ("age85", False),
        ("jbs", True),
        ("generic_result", False),
    ]:
        check(p in result, expected, f"IND{ind} {p} inclusion")
for ind in [229, 274, 275, 287]:
    check(
        "haemorrhage" in outcomes(f"fct_person_lipid_lowering_therapy_ind{ind}"),
        False,
        f"IND{ind} all stroke exclusion",
    )
for ind in [161, 181]:
    check(
        "haemorrhage" in outcomes(f"fct_person_cvd_risk_assessment_ind{ind}"),
        False,
        f"IND{ind} all stroke exclusion",
    )
check(
    "invalid_score" in outcomes("fct_person_lipid_lowering_therapy_ind229"),
    False,
    "last invalid CVD risk does not fall back",
)
lip275 = outcomes("fct_person_lipid_lowering_therapy_ind275")
check("low_then_high" in lip275, True, "subsequent higher score restores IND275")
check("high_then_low" in lip275, False, "latest low score excludes IND275")
check(
    DB.execute(
        "SELECT has_cvd, has_cvd_including_haemorrhagic_stroke FROM int_cvd_risk_profile WHERE person_id='haemorrhage'"
    ).fetchone(),
    (False, True),
    "narrow and all-stroke definitions kept separate",
)

# Recorded OTC aspirin and oral anticoagulant prophylaxis count only for the taking-treatment indicators.
people = [
    "otc_only",
    "prophylaxis_only",
    "unspecified_stroke",
    "both_stroke_types",
    "tia",
    "old_otc",
]
table("dim_person_age", "person_id VARCHAR, age INTEGER", [(p, 60) for p in people])
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
for reg in ["chd", "pad", "stroke_tia"]:
    table(
        "fct_person_" + reg + "_register",
        "person_id VARCHAR, is_on_register BOOLEAN",
        [(p, True) for p in people],
    )
table(
    "int_antithrombotic_contraindication_all",
    "person_id VARCHAR, drug_class VARCHAR, is_persisting BOOLEAN, clinical_effective_date DATE",
)
table("int_antiplatelet_medications_all", "person_id VARCHAR, order_date DATE")
table(
    "int_anticoagulant_medications_all",
    "person_id VARCHAR, order_date DATE, anticoagulant_type VARCHAR, is_doac BOOLEAN, is_vka BOOLEAN",
)
table(
    "mock_treatment",
    "person_id VARCHAR, cluster_id VARCHAR, clinical_effective_date DATE",
    [
        (p, "OSAL_COD", "2026-08-01")
        for p in people
        if p not in ["prophylaxis_only", "old_otc"]
    ]
    + [
        ("prophylaxis_only", "ORANTICOAG_COD", "2026-08-01"),
        ("old_otc", "OSAL_COD", "2020-01-01"),
    ],
)
build("int_antithrombotic_therapy_latest", "mock_treatment")
table(
    "mock_non_haem",
    "person_id VARCHAR, clinical_effective_date_raw DATE",
    [
        (p, "2020-01-01")
        for p in ["otc_only", "prophylaxis_only", "both_stroke_types", "old_otc"]
    ],
)
build("int_non_haemorrhagic_stroke_history", "mock_non_haem")
table(
    "int_stroke_tia_diagnoses_all",
    "person_id VARCHAR, is_tia_diagnosis_code BOOLEAN, clinical_effective_date DATE",
    [("tia", True, "2020-01-01")],
)
for ind in [132, 133, 94]:
    build(f"fct_person_antithrombotic_therapy_ind{ind}")
check(
    outcomes("fct_person_antithrombotic_therapy_ind132")["otc_only"],
    True,
    "OTC aspirin succeeds IND132",
)
check(
    outcomes("fct_person_antithrombotic_therapy_ind132")["prophylaxis_only"],
    True,
    "prophylaxis record succeeds IND132",
)
check(
    outcomes("fct_person_antithrombotic_therapy_ind94")["otc_only"],
    True,
    "OTC aspirin succeeds IND94",
)
check(
    "unspecified_stroke" in outcomes("fct_person_antithrombotic_therapy_ind133"),
    False,
    "unspecified stroke excluded IND133",
)
check(
    outcomes("fct_person_antithrombotic_therapy_ind133")["both_stroke_types"],
    True,
    "recorded non-haemorrhagic stroke establishes inclusion despite other stroke type",
)
check(
    outcomes("fct_person_antithrombotic_therapy_ind133")["tia"],
    True,
    "TIA establishes inclusion",
)
check(
    DB.execute(
        "SELECT indicator_status FROM fct_person_antithrombotic_therapy_ind132 WHERE person_id='old_otc'"
    ).fetchone()[0],
    "NOT_TREATED_IN_PERIOD",
    "old recorded use is not never-treated",
)
check(
    DB.execute(
        "SELECT latest_anticoagulant_order_date IS NULL FROM int_antithrombotic_therapy_latest WHERE person_id='prophylaxis_only'"
    ).fetchone()[0],
    True,
    "recorded prophylaxis does not fabricate an AF prescription",
)

# Execute all eight BP models at the age and treatment threshold boundaries.
people = [
    "clinic79_equal",
    "clinic79_below",
    "home79_equal",
    "home79_below",
    "clinic80_equal",
    "clinic80_below",
    "home80_equal",
    "home80_below",
    "old_bp",
]
table(
    "dim_person_age",
    "person_id VARCHAR, age INTEGER",
    [(p, 80 if "80" in p else 79) for p in people],
)
table(
    "dim_person_active_patients",
    "person_id VARCHAR, current_practice_code VARCHAR, current_practice_name VARCHAR",
    [(p, "SYNTHETIC", "Synthetic practice") for p in people],
)
for reg in ["chd", "pad", "stroke_tia", "hypertension"]:
    table(
        "fct_person_" + reg + "_register",
        "person_id VARCHAR, is_on_register BOOLEAN",
        [(p, True) for p in people],
    )
bp = []
for p in people:
    home = p.startswith("home")
    older = "80" in p
    equal = p.endswith("equal")
    systolic = (145 if home else 150) if older else (135 if home else 140)
    bp.append(
        (
            p,
            "2020-01-01" if p == "old_bp" else "2026-08-01",
            systolic - (0 if equal else 1),
            84 if home else 89,
            home,
            False,
            "HBPM_ABPM" if home else "CLINIC",
        )
    )
table(
    "fct_person_bp_control",
    "person_id VARCHAR, latest_bp_date DATE, latest_systolic_value FLOAT, latest_diastolic_value FLOAT, is_home_bp_event BOOLEAN, is_abpm_bp_event BOOLEAN, applied_measurement_context VARCHAR",
    bp,
)
for path in (ROOT / "models/reporting/olids/measures/nice/bp_control").glob(
    "*ind*.sql"
):
    if "_nice_" in path.name:
        continue
    build(path.stem)
    result = outcomes(path.stem)
    older = path.stem.endswith(("240", "242", "244", "246"))
    for p in people:
        expected_member = ("80" in p) == older
        check(p in result, expected_member, path.stem + " " + p + " age")
        if expected_member:
            check(result[p], p.endswith("below"), path.stem + " " + p + " result")

# Validate changed YAML parses without needing dbt or a warehouse connection.
for path in ROOT.glob("models/**/*.yml"):
    if any(
        part in str(path)
        for part in [
            "atrial_fibrillation",
            "antithrombotic",
            "cvd_risk",
            "lipid_lowering",
            "non_haemorrhagic_stroke",
        ]
    ):
        yaml.safe_load(path.read_text())
print(
    f"{CHECKS} synthetic cardiovascular assertions passed; {BUILDS} repository models executed; relevant YAML parsed."
)
