-- Pair: macros/qof_registers/calculate_stroke_tia_register.sql.
-- Evidence is bounded by today. The PIT pair evaluates supplied reference dates.

{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Stroke and TIA register fact table - one row per person.
Applies QOF v51 stroke register inclusion criteria.

Clinical Purpose:
- QOF stroke register for secondary prevention measures
- Cardiovascular risk management post-stroke
- Stroke care pathway monitoring

QOF Register Criteria:
- Any STRK_COD or TIA_COD diagnosis on or before today
- No resolution exclusion
- No age restrictions
- Lifelong condition register for secondary prevention

Includes all patients meeting clinical criteria (active, deceased, deducted).
This table provides one row per person for analytical use.
*/

WITH stroke_tia_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        NULL AS earliest_resolved_date,
        NULL AS latest_resolved_date,

        -- Resolution dates (stroke/TIA are permanent conditions, no resolved codes)
        0 AS total_resolution_codes,
        MIN(
            CASE
                WHEN
                    (is_stroke_diagnosis_code OR is_tia_diagnosis_code)
                    THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,

        -- Episode counts
        MAX(
            CASE
                WHEN
                    (is_stroke_diagnosis_code OR is_tia_diagnosis_code)
                    THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,
        COUNT(
            CASE
                WHEN (is_stroke_diagnosis_code OR is_tia_diagnosis_code) THEN 1
            END
        ) AS total_stroke_tia_episodes,  -- No resolution codes for stroke/TIA

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE
                WHEN
                    (is_stroke_diagnosis_code OR is_tia_diagnosis_code)
                    THEN concept_code
            END
        )
            AS stroke_tia_diagnosis_codes,
        ARRAY_CONSTRUCT() AS stroke_tia_resolution_codes,  -- No resolution codes
        ARRAY_AGG(
            DISTINCT CASE
                WHEN
                    (is_stroke_diagnosis_code OR is_tia_diagnosis_code)
                    THEN concept_display
            END
        )
            AS stroke_tia_diagnosis_displays

    FROM {{ ref('int_stroke_tia_diagnoses_all') }}
    WHERE CAST(clinical_effective_date AS DATE) <= CURRENT_DATE()
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        std.*,

        -- QOF register logic: Include if has diagnosis (stroke/TIA are permanent conditions)
        COALESCE(earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN 'Active stroke/TIA - permanent condition'
            ELSE 'No stroke/TIA diagnosis'
        END AS stroke_tia_status,


    FROM stroke_tia_diagnoses AS std
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.stroke_tia_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.earliest_resolved_date,
    ri.latest_resolved_date,
    ri.total_stroke_tia_episodes,
    ri.total_resolution_codes,
    ri.stroke_tia_diagnosis_codes,
    ri.stroke_tia_resolution_codes,
    ri.stroke_tia_diagnosis_displays

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
