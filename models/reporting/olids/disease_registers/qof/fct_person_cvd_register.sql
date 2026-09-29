-- Pair: macros/qof_registers/calculate_cvd_register.sql.
-- This live fact includes future-dated component membership; its PIT pair is
-- strict as-of.

{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
QOF v51 cardiovascular disease register (CD_REG), one row per qualifying person.
This live model includes future-dated records through its component register models.
Its strict as-of sibling is calculate_cvd_register.sql.
*/

WITH component_memberships AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_chd_diagnosis_date,
        NULL::DATE AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register = TRUE

    UNION ALL

    SELECT
        person_id,
        NULL::DATE AS earliest_chd_diagnosis_date,
        earliest_diagnosis_date AS earliest_stroke_tia_diagnosis_date
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register = TRUE
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
