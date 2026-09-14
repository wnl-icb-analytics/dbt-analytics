{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Frailty register from coded frailty severity.

Business logic:
- Any code in the PCD clusters MILDFRAIL_COD, MODFRAIL_COD or SEVFRAIL_COD = on register
- latest_frailty_severity is the severity of the most recent coded finding;
  the more severe code wins when two are recorded on the same date
- No resolved codes: frailty fluctuates but does not resolve
- No age restriction

Calculated frailty scores (eFI, eFI2, Rockwood) are not used here; see
int_efi_latest, fct_person_efi2 and int_rockwood_latest.
*/

WITH latest_severity AS (
    -- Get the latest frailty severity for each person
    SELECT 
        person_id,
        frailty_severity AS latest_frailty_severity
    FROM (
        SELECT 
            person_id,
            frailty_severity,
            ROW_NUMBER() OVER (
                PARTITION BY person_id
                ORDER BY
                    clinical_effective_date DESC,
                    CASE frailty_severity WHEN 'Severe' THEN 3 WHEN 'Moderate' THEN 2 ELSE 1 END DESC,
                    ID DESC
            ) AS rn
        FROM {{ ref('int_frailty_diagnoses_all') }}
        WHERE is_diagnosis_code = TRUE
    )
    WHERE rn = 1
),

frailty_diagnoses AS (
    SELECT
        fd.person_id,

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

        -- True for every person here; retained for consumers of the column
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_active_frailty_diagnosis,

        -- Count of frailty diagnoses (may indicate progression or reassessment)
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_frailty_diagnoses,

        -- Severity-specific counts
        COUNT(CASE WHEN frailty_severity = 'Mild' THEN 1 END) AS mild_frailty_count,
        COUNT(CASE WHEN frailty_severity = 'Moderate' THEN 1 END) AS moderate_frailty_count,
        COUNT(CASE WHEN frailty_severity = 'Severe' THEN 1 END) AS severe_frailty_count,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_frailty_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_frailty_concept_displays

    FROM {{ ref('int_frailty_diagnoses_all') }} fd
    GROUP BY fd.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    ls.latest_frailty_severity,
    fd.mild_frailty_count,
    fd.moderate_frailty_count,
    fd.severe_frailty_count,
    fd.total_frailty_diagnoses,
    fd.all_frailty_concept_codes,
    fd.all_frailty_concept_displays

FROM frailty_diagnoses AS fd
INNER JOIN latest_severity AS ls
    ON fd.person_id = ls.person_id
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_frailty_diagnosis = TRUE  -- Only include persons with active frailty diagnosis