{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Multiple Sclerosis Register - Clinical Quality Measures**

Simple Register

Business Logic:
- Presence of multiple sclerosis diagnosis (MS_COD) = on register
- No resolution codes (chronic condition)

Note:
- There are no resolved codes for MS (chronic neurological condition)
- No specific age restrictions, though typically diagnosed in young to middle-aged adults
- Latest diagnosis date used for care planning

Clinical Context:
Used for multiple sclerosis quality measures including:
- Neurological care pathway monitoring
- Disease-modifying therapy monitoring
- Multidisciplinary team coordination
- Symptom management and monitoring
- Care planning and support services
*/

WITH ms_diagnoses AS (
    SELECT
        ms.person_id,

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
        FALSE) AS has_active_ms_diagnosis,

        -- Count of MS diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_ms_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_ms_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_ms_concept_displays

    FROM {{ ref('int_ms_diagnoses_all') }} ms
    GROUP BY ms.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_ms_diagnoses,
    fd.all_ms_concept_codes,
    fd.all_ms_concept_displays

FROM ms_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_ms_diagnosis = TRUE  -- Only include persons with active MS diagnosis

