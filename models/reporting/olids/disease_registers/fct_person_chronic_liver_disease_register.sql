{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Chronic Liver Disease Register

Diagnosis-only register with cirrhosis progression flag.

Business Logic:
- Presence of CLD diagnosis (CLDATRISK1_COD or CIRRHOSIS_COD) = on register
- has_cirrhosis flag indicates end-stage progression (CIRRHOSIS_COD present)
- No resolution codes available
- No age restrictions
*/

WITH cld_diagnoses AS (
    SELECT
        person_id,

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

        -- Register logic: any diagnosis = on register
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_active_cld_diagnosis,

        -- Cirrhosis progression flag
        COALESCE(MAX(
            CASE
                WHEN is_cirrhosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_cirrhosis,

        -- Cirrhosis dates
        MIN(
            CASE WHEN is_cirrhosis_code THEN clinical_effective_date END
        ) AS earliest_cirrhosis_date,
        MAX(
            CASE WHEN is_cirrhosis_code THEN clinical_effective_date END
        ) AS latest_cirrhosis_date,

        -- Count of CLD diagnoses
        COUNT(DISTINCT IFF(is_diagnosis_code, {{ observation_reading_key(value=none) }}, NULL))
            AS total_cld_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_cld_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_cld_concept_displays

    FROM {{ ref('int_chronic_liver_disease_diagnoses_all') }}
    GROUP BY person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.has_cirrhosis,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    fd.earliest_cirrhosis_date,
    fd.latest_cirrhosis_date,
    fd.total_cld_diagnoses,
    fd.all_cld_concept_codes,
    fd.all_cld_concept_displays

FROM cld_diagnoses AS fd
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_cld_diagnosis = TRUE
