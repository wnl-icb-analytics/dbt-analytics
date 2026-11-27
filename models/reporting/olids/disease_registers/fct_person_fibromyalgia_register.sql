{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Fibromyalgia Register** (Post acute infection syndromes, non-QOF)

Business Logic:
- Fibromyalgia diagnosis (FIBROMYALGIA_COD) ever recorded = on register
- No resolution codes exist in SNOMED CT for this condition
- No age restriction
*/

WITH fibromyalgia_diagnoses AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL AS has_active_fibromyalgia_diagnosis,
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_fibromyalgia_diagnoses,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END)
            AS all_fibromyalgia_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END)
            AS all_fibromyalgia_concept_displays
    FROM {{ ref('int_fibromyalgia_diagnoses_all') }}
    GROUP BY person_id
)

SELECT
    agg.person_id,
    age.age,
    TRUE AS is_on_register,
    agg.earliest_diagnosis_date,
    agg.latest_diagnosis_date,
    agg.total_fibromyalgia_diagnoses,
    agg.all_fibromyalgia_concept_codes,
    agg.all_fibromyalgia_concept_displays
FROM fibromyalgia_diagnoses AS agg
INNER JOIN {{ ref('dim_person') }} AS p
    ON agg.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON agg.person_id = age.person_id
WHERE agg.has_active_fibromyalgia_diagnosis
