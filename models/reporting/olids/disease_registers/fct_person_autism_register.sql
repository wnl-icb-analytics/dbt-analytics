{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Autism Spectrum Disorder Register - Clinical Quality Measures**

Simple Register

Business Logic:
- Presence of autism spectrum disorder diagnosis (AUTISM_COD) = on register
- No resolution codes (lifelong condition)

Note:
- There are no resolved codes for autism (lifelong neurodevelopmental condition)
- No specific age restrictions, though typically diagnosed in childhood
- Latest diagnosis date used for care planning

Clinical Context:
Used for autism quality measures including:
- Neurodevelopmental care pathway monitoring
- Complex needs identification
- Care coordination and support services
- Mental health co-morbidity monitoring
- Healthcare access support
*/

WITH autism_diagnoses AS (
    SELECT
        aut.person_id,

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
        FALSE) AS has_active_autism_diagnosis,

        -- Count of autism diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_autism_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_autism_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_autism_concept_displays

    FROM {{ ref('int_autism_diagnoses_all') }} aut
    GROUP BY aut.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_autism_diagnoses,
    fd.all_autism_concept_codes,
    fd.all_autism_concept_displays

FROM autism_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_autism_diagnosis = TRUE  -- Only include persons with active autism diagnosis

