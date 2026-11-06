-- QMUL GP BP Registry — cohort build counts
--
-- Funnel from base population through to the final cohort, plus the volume
-- of in-scope BP readings (post pregnancy/HDP exclusion, inside the study
-- extraction window). Used to sanity-check the build against the paper's
-- target of >= 1,700 records.
--
-- Usage: dbt compile -s qmul_gp_bp_registry_cohort_counts, then run the
-- compiled SQL in target/compiled/...

WITH base_pop AS (
    SELECT person_id
    FROM {{ ref('int_qmul_gp_bp_registry_base_population') }}
),

eligible_readings AS (
    SELECT person_id, effective_date
    FROM {{ ref('int_qmul_gp_bp_registry_bp_readings_eligible') }}
),

base_pop_with_any_reading AS (
    SELECT DISTINCT person_id
    FROM eligible_readings
),

cohort AS (
    SELECT person_id
    FROM {{ ref('fct_qmul_gp_bp_registry_cohort') }}
),

cohort_readings AS (
    SELECT er.person_id, er.effective_date
    FROM eligible_readings er
    INNER JOIN cohort c ON er.person_id = c.person_id
)

SELECT
    (SELECT COUNT(*) FROM base_pop)
        AS persons_in_base_population,
    (SELECT COUNT(*) FROM base_pop_with_any_reading)
        AS persons_with_any_eligible_bp,
    (SELECT COUNT(*) FROM cohort)
        AS persons_in_cohort,
    (SELECT COUNT(*) FROM eligible_readings)
        AS eligible_bp_readings_base_pop,
    (SELECT COUNT(*) FROM cohort_readings)
        AS eligible_bp_readings_cohort,
    (SELECT MIN(effective_date) FROM cohort_readings)
        AS earliest_cohort_bp_date,
    (SELECT MAX(effective_date) FROM cohort_readings)
        AS latest_cohort_bp_date
