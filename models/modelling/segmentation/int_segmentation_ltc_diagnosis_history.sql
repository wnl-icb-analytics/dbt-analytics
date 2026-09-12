{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Segmentation LTC registers whose membership is determined only by diagnosis,
-- resolution or exclusion events.

WITH events AS (
    SELECT
        person_id, clinical_effective_date, date_recorded,
        'AF' AS condition_code,
        is_diagnosis_code, is_resolved_code, FALSE AS is_exclusion_code
    FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'CHD', is_diagnosis_code, FALSE, FALSE
    FROM {{ ref('int_chd_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'DEM', is_diagnosis_code, FALSE, FALSE
    FROM {{ ref('int_dementia_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'HF', is_diagnosis_code, is_resolved_code, FALSE
    FROM {{ ref('int_heart_failure_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'HTN', is_diagnosis_code, is_resolved_code, FALSE
    FROM {{ ref('int_hypertension_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'LD', is_diagnosis_code, FALSE, is_exclusion_code
    FROM {{ ref('int_learning_disability_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'STIA', is_diagnosis_code, FALSE, FALSE
    FROM {{ ref('int_stroke_tia_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'PD', is_diagnosis_code, FALSE, FALSE
    FROM {{ ref('int_parkinsons_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'ANX', is_diagnosis_code, is_resolved_code, FALSE
    FROM {{ ref('int_anxiety_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id, clinical_effective_date, date_recorded,
        'NAFLD', is_diagnosis_code, FALSE, FALSE
    FROM {{ ref('int_nafld_diagnoses_all') }}
),

person_month_condition AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        e.condition_code,
        MAX(IFF(e.is_diagnosis_code, e.clinical_effective_date, NULL))
            AS latest_diagnosis_date,
        MAX(IFF(e.is_resolved_code, e.clinical_effective_date, NULL))
            AS latest_resolved_date,
        MAX(IFF(e.is_exclusion_code, e.clinical_effective_date, NULL))
            AS latest_exclusion_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN events AS e
        ON pm.person_id = e.person_id
        AND e.clinical_effective_date <= pm.month_end_date
        AND (
            e.date_recorded IS NULL
            OR CAST(e.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date, e.condition_code
),

register_status AS (
    SELECT
        *,
        CASE
            WHEN condition_code IN ('CHD', 'DEM', 'PD', 'NAFLD', 'STIA')
                THEN latest_diagnosis_date IS NOT NULL
            WHEN condition_code = 'HTN'
                THEN latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date >= latest_resolved_date
                    )
            WHEN condition_code IN ('AF', 'ANX', 'HF')
                THEN latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            WHEN condition_code = 'LD'
                THEN latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_exclusion_date IS NULL
                        OR latest_diagnosis_date > latest_exclusion_date
                    )
        END AS is_on_register
    FROM person_month_condition
)

SELECT
    person_id,
    end_date,
    condition_code,
    is_on_register,
    latest_diagnosis_date,
    latest_resolved_date,
    latest_exclusion_date
FROM register_status
WHERE is_on_register
