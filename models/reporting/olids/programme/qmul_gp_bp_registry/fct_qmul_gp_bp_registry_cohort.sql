{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Final QMUL GP BP Registry research cohort.

A person is included when:
  - They are in the base population (on QOF hypertension register, with at
    least one oral antihypertensive medication order).
  - Their eligible BP readings meet the spacing criterion:
    >= qmul_gp_bp_registry_min_qualifying_readings (default 4) readings at
    least qmul_gp_bp_registry_min_reading_gap_weeks (default 4) apart,
    spanning at least qmul_gp_bp_registry_min_span_months (default 36) months,
    within the study extraction period (default 2010-01-01 to 2025-12-31).
  - BP readings inside pregnancy or HDP exclusion windows are dropped before
    the spacing check (handled by int_qmul_gp_bp_registry_bp_readings_eligible).

One row per included person.
*/

SELECT
    pop.person_id,
    pop.age,
    pop.earliest_diagnosis_date,
    pop.latest_diagnosis_date,
    pop.earliest_antihyp_order_date,
    pop.latest_antihyp_order_date,
    sp.qualifying_reading_count,
    sp.span_start,
    sp.span_end,
    sp.span_days,
    sp.inclusion_first_met_date

FROM {{ ref('int_qmul_gp_bp_registry_base_population') }} pop
INNER JOIN {{ ref('int_qmul_gp_bp_registry_bp_spacing') }} sp
    ON pop.person_id = sp.person_id

WHERE sp.meets_bp_criteria
