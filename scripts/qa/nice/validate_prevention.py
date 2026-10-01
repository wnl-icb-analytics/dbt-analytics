from pathlib import Path
import datetime as dt
import duckdb, jinja2, sqlglot

ROOT = Path(__file__).resolve().parents[3]
db = duckdb.connect()
day = dt.date(2026, 9, 12)
recent = dt.date(2026, 8, 12)


def table(name, columns, rows=()):
    db.execute(f"create or replace table {name} ({columns})")
    if rows:
        db.executemany(
            f"insert into {name} values ({','.join('?' for _ in rows[0])})", rows
        )


def model(path):
    def observations(clusters, **kwargs):
        return f"select * from synthetic_observations where cluster_id in ({clusters})"

    sql = (
        jinja2.Environment()
        .from_string((ROOT / path).read_text())
        .render(
            config=lambda **kwargs: "",
            ref=lambda name: name,
            get_observations=observations,
            nice_flu_season_year=lambda: "2025",
        )
    )
    sql = sql.replace("CURRENT_DATE()", "DATE '2026-09-12'")
    return sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]


def results(path):
    frame = db.execute(model(path)).fetchdf()
    return {int(row.person_id): bool(row.is_in_numerator) for row in frame.itertuples()}


ids = range(1, 7)
table(
    "dim_person_age",
    "person_id int, age int, birth_date_approx date",
    [(i, 30, dt.date(1996, 9, 12)) for i in ids],
)
table(
    "dim_person_active_patients",
    "person_id int, current_practice_code varchar, current_practice_name varchar",
    [(i, "SYNTHETIC", "Synthetic practice") for i in ids],
)
flags = [
    "has_chd",
    "has_atrial_fibrillation",
    "has_heart_failure",
    "has_stroke_tia",
    "has_diabetes",
    "has_dementia",
    "has_alcohol_disorder",
]
cols = (
    "person_id int, earliest_hypertension_date date, earliest_depression_anxiety_date date, latest_alcohol_screen_date date, latest_alcohol_screen_tool varchar, latest_alcohol_screen_score int, latest_positive_alcohol_screen_date date, latest_intervention_after_positive_screen_date date, "
    + ", ".join(f + " boolean" for f in flags)
)
table(
    "int_ltc_review_profile",
    cols,
    [
        (
            i,
            recent,
            recent,
            recent,
            "AUDIT",
            8,
            None,
            None,
            *[f == "has_chd" for f in flags],
        )
        for i in ids
    ],
)
table(
    "int_alcohol_screening_all",
    "person_id int, clinical_effective_date date, screening_tool varchar",
    [
        (1, recent, "AUDIT"),
        (2, recent, "FAST"),
        (3, recent, "AUDIT-C"),
        (4, dt.date(2026, 1, 1), "FAST"),
        (4, recent, "AUDIT"),
    ],
)
base = "models/reporting/olids/measures/nice/"
for n in (196, 198):
    actual = results(base + f"alcohol/fct_person_alcohol_ind{n}.sql")
    assert actual == {1: False, 2: True, 3: True, 4: False, 5: False, 6: False}, (
        n,
        actual,
    )
actual = results(base + "alcohol/fct_person_alcohol_ind201.sql")
assert actual == {1: False, 2: True, 3: True, 4: True, 5: False, 6: False}, actual
print(
    "PASS: IND196/198/201 reject AUDIT-only evidence and retain qualifying FAST/AUDIT-C inside each window."
)

table(
    "dim_person_demographics",
    "person_id int, gender varchar",
    [(i, "Female") for i in ids],
)
table(
    "fct_cervical_screening_status",
    "person_id int, latest_completed_date date, latest_screening_date date, programme_status varchar, total_unsuitable_records int",
    [(i, recent, recent, "Up to Date", 1 if i == 1 else 0) for i in ids],
)
db.execute(
    "update fct_cervical_screening_status set latest_completed_date=null where person_id=5"
)
table("int_cervix_removal_all", "person_id int", [(2,)])
table(
    "int_cervical_screening_all",
    "person_id int, screening_observation_type varchar, clinical_effective_date date",
    [
        (3, "Non-response to Invitations", dt.date(2024, 9, 12)),
        (5, "Non-response to Invitations", dt.date(2021, 9, 12)),
    ],
)
table(
    "fct_person_pregnancy_status",
    "person_id int, is_currently_pregnant boolean",
    [(4, True), (6, False)],
)
actual = results(base + "cervical_screening/fct_person_cervical_screening_ind176.sql")
assert actual == {1: True, 3: True, 5: False, 6: True}, actual
db.execute("update dim_person_age set age=55")
actual = results(base + "cervical_screening/fct_person_cervical_screening_ind177.sql")
assert actual == {1: True, 3: True, 6: True}, actual
actual = results(base + "cervical_screening/fct_person_cervical_screening_ind321.sql")
assert actual == {1: True, 3: True, 4: True, 5: False, 6: True}, actual
print(
    "PASS: cervical indicators distinguish cervix removal from general unsuitability, use interval-specific nonresponse, and retain ended pregnancies."
)

table(
    "fct_person_stroke_tia_register",
    "person_id int, is_on_register boolean",
    [(1, True), (2, True)],
)
table(
    "int_nice_flu_season_vaccination",
    "person_id int, latest_vaccination_date date, is_laiv boolean",
    [(1, dt.date(2025, 10, 1), False)],
)
actual = results(base + "flu_vaccination/fct_person_flu_vaccination_ind164.sql")
assert actual == {1: True, 2: False}, actual
print(
    "PASS: IND164 includes the full stroke/TIA register before personalised care adjustments."
)

birth = dt.date(2025, 12, 15)
table(
    "int_childhood_imms_current_population",
    "person_id int, birth_date_approx date",
    [(i, birth) for i in ids],
)
table(
    "stg_reference_childhood_imms_codes",
    "snomedconceptid varchar, vaccine varchar, proposedcluster varchar",
    [
        ("SYN6", "DTaP/IPV/Hib/HepB/6-in-1", "6IN1_ADM"),
        ("SYN6DRUG", "DTaP/IPV/Hib/HepB/6-in-1", "6IN1_DRUG"),
        ("SYNDECLINE", "DTaP/IPV/Hib/HepB/6-in-1", "6IN1_DEC"),
        ("SYNMENB", "MenB", "MenB_ADM"),
    ],
)
table(
    "stg_reference_combined_codesets",
    "code varchar, source varchar, cluster_id varchar",
    [
        ("SYN4", "PCD", "4IN1VAC_COD"),
        ("SYN5", "PCD", "5IN1VAC_COD"),
        ("SYN6", "PCD", "6IN1VAC_COD"),
    ],
)
table(
    "stg_nhsd_snomed_sct_refset_simple",
    "referenced_component_id varchar, ref_set_id varchar, active boolean",
    [("SYN5DRUG", "72381000001103", True), ("SYNOLD", "72381000001103", False)],
)
vdates = [dt.date(2026, 2, 1), dt.date(2026, 3, 1), dt.date(2026, 4, 1)]
events = [(1, "SYNDECLINE", dt.date(2026, 1, 1))] + [(1, "SYN6", d) for d in vdates]
events += [(2, c, d) for c, d in zip(["SYN4", "SYN5", "SYN6"], vdates)]
events += [(3, "SYN6", vdates[0]), (3, "SYN6", vdates[0]), (3, "SYN6", vdates[1])]
events += [(4, "SYN6", d) for d in vdates[:2]] + [(4, "SYN6", dt.date(2026, 8, 15))]
events += [(5, "SYN6", d) for d in vdates[:2]] + [(5, "SYN6", dt.date(2026, 10, 1))]
table(
    "stg_olids_observation",
    "person_id int, mapped_concept_code varchar, clinical_effective_date date",
    events,
)
table(
    "stg_olids_medication_order",
    "person_id int, mapped_concept_code varchar, clinical_effective_date date",
    [(6, "SYN6DRUG", d) for d in vdates],
)
db.execute(
    "create or replace table int_childhood_immunisation_profile as "
    + model(
        "models/modelling/olids/person_attributes/int_childhood_immunisation_profile.sql"
    )
)
actual = dict(
    db.execute(
        "select person_id, dtap_doses_by_8_months from int_childhood_immunisation_profile"
    ).fetchall()
)
assert actual == {1: 3, 2: 3, 3: 2, 4: 2, 5: 2, 6: 3}, actual
actual = results(
    base + "childhood_immunisation/fct_person_childhood_immunisation_ind215.sql"
)
assert actual == {1: True, 2: True, 3: False, 4: False, 5: False, 6: True}, actual
print(
    "PASS: actual childhood profile and IND215 preserve doses after declines, accept mixed DTP formulations/orders, deduplicate dates, and exclude milestone/future dates."
)

db.execute(
    "insert into stg_reference_childhood_imms_codes values ('SYN4DRUG','dTaP/IPV/4-in-1','4IN1_DRUG')"
)
db.execute(
    "insert into stg_reference_combined_codesets values ('SYNBOOST','PCD','DTAPIPVVACBOOST_COD')"
)
db.execute(
    "update int_childhood_imms_current_population set birth_date_approx=date '2022-09-15' where person_id in (1,2)"
)
db.execute(
    "insert into stg_olids_medication_order values (1,'SYN4DRUG',date '2026-04-01'),(3,'SYN5DRUG',date '2026-04-01'),(4,'SYNOLD',date '2026-04-01')"
)
db.execute("insert into stg_olids_observation values (2,'SYNBOOST',date '2026-04-01')")
db.execute(
    "create or replace table int_childhood_immunisation_profile as "
    + model(
        "models/modelling/olids/person_attributes/int_childhood_immunisation_profile.sql"
    )
)
assert db.execute(
    "select has_dtap_booster_1_to_5_years from int_childhood_immunisation_profile where person_id=1"
).fetchone() == (False,)
assert db.execute(
    "select has_dtap_booster_1_to_5_years from int_childhood_immunisation_profile where person_id=2"
).fetchone() == (True,)
assert db.execute(
    "select dtap_doses_by_8_months from int_childhood_immunisation_profile where person_id=3"
).fetchone() == (3,)
assert db.execute(
    "select dtap_doses_by_8_months from int_childhood_immunisation_profile where person_id=4"
).fetchone() == (2,)
print(
    "PASS: active 5-in-1 drug membership counts, inactive membership fails, and only explicit booster evidence satisfies the preschool booster."
)

db.execute("update dim_person_age set age=75, birth_date_approx=date '1951-01-15'")
table(
    "int_adult_imms_current_population",
    "person_id int, is_immunosuppressed boolean",
    [(i, i == 3) for i in ids],
)
table(
    "synthetic_observations",
    "id int, person_id int, clinical_effective_date date, cluster_id varchar, mapped_concept_code varchar, mapped_concept_display varchar",
    [
        (1, 1, dt.date(2025, 1, 1), "SHVACGP_COD", "SYN1", "Synthetic vaccination"),
        (
            2,
            1,
            dt.date(2026, 5, 1),
            "SHVACGP_COD",
            "SYN1",
            "Synthetic later vaccination",
        ),
        (3, 2, dt.date(2025, 1, 1), "SHCON_COD", "SYN2", "Synthetic contraindication"),
        (4, 2, dt.date(2025, 3, 1), "SHVACGP_COD", "SYN1", "Synthetic vaccination"),
        (
            5,
            4,
            dt.date(2026, 10, 1),
            "SHVACGP_COD",
            "SYN1",
            "Synthetic future vaccination",
        ),
    ],
)
db.execute(
    "create or replace table int_shingles_vaccination_all as "
    + model("models/modelling/olids/observations/int_shingles_vaccination_all.sql")
)
actual = results(
    base + "shingles_vaccination/fct_person_shingles_vaccination_ind219.sql"
)
assert actual == {1: True, 2: True, 4: False, 5: False, 6: False}, actual
print(
    "PASS: shingles retains in-window doses despite later doses/general contraindications and excludes immunosuppression and future evidence."
)
