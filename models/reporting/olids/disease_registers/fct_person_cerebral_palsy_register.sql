{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Cerebral Palsy Register - Clinical Quality Measures**

Simple Register

Business Logic:
- Presence of cerebral palsy diagnosis (CEREBRALP_COD) = on register
- No resolution codes (lifelong condition)

Note:
- There are no resolved codes for cerebral palsy (lifelong neurological condition)
- No specific age restrictions, though typically diagnosed in childhood
- Latest diagnosis date used for care planning

Clinical Context:
Used for cerebral palsy quality measures including:
- Neurological care pathway monitoring
- Multidisciplinary team coordination
- Support services planning
- Mobility and function support
- Care planning throughout life
*/

WITH cerebral_palsy_diagnoses AS (
    SELECT
        cp.person_id,

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
        FALSE) AS has_active_cerebral_palsy_diagnosis,

        -- Count of cerebral palsy diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_cerebral_palsy_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_cerebral_palsy_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_cerebral_palsy_concept_displays

    FROM {{ ref('int_cerebral_palsy_diagnoses_all') }} cp
    GROUP BY cp.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.total_cerebral_palsy_diagnoses,
    fd.all_cerebral_palsy_concept_codes,
    fd.all_cerebral_palsy_concept_displays

FROM cerebral_palsy_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_cerebral_palsy_diagnosis = TRUE  -- Only include persons with active cerebral palsy diagnosis

