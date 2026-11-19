{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Hypothyroidism Register

Business Logic:
- Presence of hypothyroidism diagnosis (HYPOTHY_COD or THY_COD) = on register
- No resolution codes (chronic condition)
- No age restrictions
*/

WITH hypothyroidism_diagnoses AS (
    SELECT
        thy.person_id,

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
        FALSE) AS has_active_hypothyroidism_diagnosis,

        -- Count of hypothyroidism diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key() }}, NULL))
            AS total_hypothyroidism_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_hypothyroidism_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_hypothyroidism_concept_displays

    FROM {{ ref('int_hypothyroidism_diagnoses_all') }} thy
    GROUP BY thy.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_hypothyroidism_diagnoses,
    fd.all_hypothyroidism_concept_codes,
    fd.all_hypothyroidism_concept_displays

FROM hypothyroidism_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_hypothyroidism_diagnosis = TRUE  -- Only include persons with active hypothyroidism diagnosis

