-- Pair: macros/qof_registers/calculate_cvd_register.sql.
-- Evidence is bounded by today. The PIT pair evaluates supplied reference dates.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
QOF v51 cardiovascular disease register (CD_REG), one row per qualifying person.
Component diagnoses must be on or before today.
*/

WITH component_memberships AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_chd_diagnosis_date,
        NULL::DATE AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE
        AND CAST(earliest_diagnosis_date AS DATE) <= CURRENT_DATE()

    UNION ALL

    SELECT
        person_id,
        NULL::DATE AS earliest_chd_diagnosis_date,
        earliest_diagnosis_date AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE
        AND CAST(earliest_diagnosis_date AS DATE) <= CURRENT_DATE()
),

person_membership AS (
    SELECT
        person_id,
        MIN(earliest_chd_diagnosis_date) AS earliest_chd_diagnosis_date,
        MIN(earliest_stroke_tia_diagnosis_date)
            AS earliest_stroke_tia_diagnosis_date
    FROM component_memberships
    GROUP BY person_id
)

SELECT
    person_id,
    TRUE AS is_on_register,
    earliest_chd_diagnosis_date IS NOT NULL AS is_qualified_via_chd,
    earliest_stroke_tia_diagnosis_date IS NOT NULL
        AS is_qualified_via_stroke_tia,
    LEAST_IGNORE_NULLS(
        earliest_chd_diagnosis_date,
        earliest_stroke_tia_diagnosis_date
    ) AS earliest_qualifying_diagnosis_date,
    earliest_chd_diagnosis_date,
    earliest_stroke_tia_diagnosis_date
FROM person_membership
