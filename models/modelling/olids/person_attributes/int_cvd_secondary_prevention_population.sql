{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Shared denominator for NICE secondary-prevention indicators (IND230, IND278 and
related): people on the CHD, stroke/TIA or PAD QOF register with a diagnosis dated
on or before the build date, as the NICE specifications define cardiovascular
disease. Familial hypercholesterolaemia and haemorrhagic stroke history (from
int_haemorrhagic_stroke_history) are flags so each measure applies its own exclusions. No registration, living or test-patient
filter; consumers join dim_person_active_patients.

CURRENT_DATE() is fixed at this table's build. Build a consuming measure together
with this model so both use the same date.
*/

WITH cvd_registers AS (
    SELECT person_id, 'CHD' AS condition, earliest_diagnosis_date::DATE AS earliest_diagnosis_date
    FROM {{ ref('fct_person_chd_register') }}
    WHERE is_on_register

    UNION ALL

    SELECT person_id, 'STROKE_TIA', earliest_diagnosis_date::DATE
    FROM {{ ref('fct_person_stroke_tia_register') }}
    WHERE is_on_register

    UNION ALL

    SELECT person_id, 'PAD', earliest_diagnosis_date::DATE
    FROM {{ ref('fct_person_pad_register') }}
    WHERE is_on_register
),

cvd_people AS (
    SELECT
        person_id,
        BOOLOR_AGG(condition = 'CHD') AS has_chd,
        BOOLOR_AGG(condition = 'STROKE_TIA') AS has_stroke_tia,
        BOOLOR_AGG(condition = 'PAD') AS has_pad,
        MIN(CASE WHEN condition = 'CHD' THEN earliest_diagnosis_date END) AS earliest_chd_diagnosis_date,
        MIN(CASE WHEN condition = 'STROKE_TIA' THEN earliest_diagnosis_date END) AS earliest_stroke_tia_diagnosis_date,
        MIN(CASE WHEN condition = 'PAD' THEN earliest_diagnosis_date END) AS earliest_pad_diagnosis_date,
        MIN(earliest_diagnosis_date) AS earliest_cvd_diagnosis_date
    FROM cvd_registers
    WHERE earliest_diagnosis_date <= CURRENT_DATE()
    GROUP BY person_id
),

fh_history AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
    WHERE is_diagnosis_code
        AND (clinical_effective_date_raw IS NULL
            OR clinical_effective_date_raw::DATE <= CURRENT_DATE())
),

haemorrhagic_stroke_history AS (
    SELECT person_id
    FROM {{ ref('int_haemorrhagic_stroke_history') }}
)

SELECT
    cvd.person_id,
    cvd.has_chd,
    cvd.has_stroke_tia,
    cvd.has_pad,
    cvd.earliest_chd_diagnosis_date,
    cvd.earliest_stroke_tia_diagnosis_date,
    cvd.earliest_pad_diagnosis_date,
    cvd.earliest_cvd_diagnosis_date,
    fh.person_id IS NOT NULL AS has_familial_hypercholesterolaemia,
    hs.person_id IS NOT NULL AS has_haemorrhagic_stroke
FROM cvd_people cvd
LEFT JOIN fh_history fh ON cvd.person_id = fh.person_id
LEFT JOIN haemorrhagic_stroke_history hs ON cvd.person_id = hs.person_id
