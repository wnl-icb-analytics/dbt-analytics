{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Long COVID Register** (Post acute infection syndromes, non-QOF)

Business Logic:
- Long COVID diagnosis (LONGCOVID_COD) = on register
- Unresolved: latest diagnosis later than the latest resolution (LONGCOVIDRES_COD)
- No age restriction
*/

WITH long_covid_diagnoses AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        COALESCE(
            MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END) IS NOT NULL
            AND (
                MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END) IS NULL
                OR MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
                > MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            ),
            FALSE
        ) AS has_active_long_covid_diagnosis,
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_long_covid_diagnoses,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END)
            AS all_long_covid_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END)
            AS all_long_covid_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_resolved_code THEN concept_code END)
            AS all_long_covid_resolved_concept_codes
    FROM {{ ref('int_long_covid_diagnoses_all') }}
    GROUP BY person_id
)

SELECT
    agg.person_id,
    age.age,
    TRUE AS is_on_register,
    agg.earliest_diagnosis_date,
    agg.latest_diagnosis_date,
    agg.latest_resolved_date,
    agg.total_long_covid_diagnoses,
    agg.all_long_covid_concept_codes,
    agg.all_long_covid_concept_displays,
    agg.all_long_covid_resolved_concept_codes
FROM long_covid_diagnoses AS agg
INNER JOIN {{ ref('dim_person') }} AS p
    ON agg.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON agg.person_id = age.person_id
WHERE agg.has_active_long_covid_diagnosis
