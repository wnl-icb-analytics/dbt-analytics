{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Motor Neurone Disease Register - Clinical Quality Measures**

Simple Register

Business Logic:
- Presence of motor neurone disease diagnosis (MND_COD) = on register
- No resolution codes (progressive condition)

Note:
- There are no resolved codes for MND (progressive neurological condition)
- No specific age restrictions, though more common in older adults
- Latest diagnosis date used for care planning

Clinical Context:
Used for motor neurone disease quality measures including:
- Neurological care pathway monitoring
- Palliative care planning and coordination
- Multidisciplinary team coordination
- Symptom management and monitoring
- Care planning and support services
*/

WITH mnd_diagnoses AS (
    SELECT
        mnd.person_id,

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
        FALSE) AS has_active_mnd_diagnosis,

        -- Count of MND diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_mnd_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_mnd_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_mnd_concept_displays

    FROM {{ ref('int_mnd_diagnoses_all') }} mnd
    GROUP BY mnd.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_mnd_diagnoses,
    fd.all_mnd_concept_codes,
    fd.all_mnd_concept_displays

FROM mnd_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_mnd_diagnosis = TRUE  -- Only include persons with active MND diagnosis

