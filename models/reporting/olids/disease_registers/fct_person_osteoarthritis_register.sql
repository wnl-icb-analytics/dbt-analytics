{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Osteoarthritis Register**

Simple Register

Business Logic:
- Presence of osteoarthritis diagnosis (OA_COD) = on register
- No resolution codes (degenerative condition)

Note:
- No resolved codes for osteoarthritis (degenerative joint condition)
- No specific age restrictions
- Latest diagnosis date used for care planning
*/

WITH osteoarthritis_diagnoses AS (
    SELECT
        oa.person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,

        -- Register logic: active diagnosis required
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_active_osteoarthritis_diagnosis,

        -- Count of osteoarthritis diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_osteoarthritis_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_osteoarthritis_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_osteoarthritis_concept_displays

    FROM {{ ref('int_osteoarthritis_diagnoses_all') }} oa
    GROUP BY oa.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_osteoarthritis_diagnoses,
    fd.all_osteoarthritis_concept_codes,
    fd.all_osteoarthritis_concept_displays

FROM osteoarthritis_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_osteoarthritis_diagnosis = TRUE
