-- QOF v51 HFRES_DAT is strictly after HFLAT_DAT, at date precision.
WITH latest_evidence AS (
    SELECT
        person_id,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)::DATE AS diagnosis_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)::DATE AS resolution_date,
        MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) IS NOT NULL AS has_reduced_ef
    FROM {{ ref('int_heart_failure_diagnoses_all') }}
    GROUP BY person_id
),

same_day_diagnoses AS (
    SELECT evidence.person_id, evidence.has_reduced_ef
    FROM latest_evidence AS evidence
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON evidence.person_id = age.person_id
    WHERE evidence.diagnosis_date = evidence.resolution_date
)

SELECT expected.person_id
FROM same_day_diagnoses AS expected
LEFT JOIN {{ ref('fct_person_heart_failure_register') }} AS actual
    ON expected.person_id = actual.person_id
WHERE actual.person_id IS NULL
   OR actual.is_on_register IS DISTINCT FROM TRUE
   OR actual.has_active_diagnosis IS DISTINCT FROM TRUE
   OR actual.is_on_hfref_register IS DISTINCT FROM expected.has_reduced_ef
