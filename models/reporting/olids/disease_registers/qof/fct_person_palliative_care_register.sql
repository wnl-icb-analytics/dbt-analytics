-- Pair: macros/qof_registers/calculate_palliative_care_register.sql.
-- Clinical evidence is bounded by today; its PIT pair is strict as-of.

{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Palliative Care register fact table - one row per person.
Applies QOF palliative care register inclusion criteria.

Clinical Purpose:
- QOF palliative care register for end-of-life care quality measures
- End-of-life care coordination and monitoring
- Palliative care pathway assessment
- Appropriate care targeting and quality of life monitoring

QOF Register Criteria (Complex Pattern):
- Palliative care code (PALCARE_COD) on/after 1 April 2008
- No "no longer indicated" code (PALCARENI_COD) on a later calendar date than latest care
- No age restrictions for palliative care register
- Important for end-of-life care quality measures

Includes all patients meeting clinical criteria (active, deceased, deducted).
This table provides one row per person for analytical use.
*/

WITH palliative_care_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates (on or after QOF start date)
        MIN(CASE
            WHEN
                is_palliative_care_code
                AND clinical_effective_date::DATE >= DATE '2008-04-01'
                THEN clinical_effective_date
        END) AS earliest_diagnosis_date,
        MAX(CASE
            WHEN
                is_palliative_care_code
                AND clinical_effective_date::DATE >= DATE '2008-04-01'
                THEN clinical_effective_date
        END) AS latest_diagnosis_date,

        -- Exclusion dates ("no longer indicated")
        MIN(CASE
            WHEN is_palliative_care_not_indicated_code
                THEN clinical_effective_date
        END) AS earliest_no_longer_indicated_date,
        MAX(CASE
            WHEN is_palliative_care_not_indicated_code
                THEN clinical_effective_date
        END) AS latest_no_longer_indicated_date,

        -- Episode counts
        COUNT(CASE
            WHEN
                is_palliative_care_code
                AND clinical_effective_date::DATE >= DATE '2008-04-01'
                THEN 1
        END) AS total_palliative_care_episodes,
        COUNT(CASE
            WHEN is_palliative_care_not_indicated_code
                THEN 1
        END) AS total_no_longer_indicated_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE WHEN is_palliative_care_code THEN concept_code END
        )
            AS palliative_care_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_palliative_care_code THEN concept_display END
        )
            AS palliative_care_displays,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_palliative_care_not_indicated_code THEN concept_code
            END
        )
            AS no_longer_indicated_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_palliative_care_not_indicated_code THEN concept_display
            END
        )
            AS no_longer_indicated_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT observation_id::VARCHAR) AS all_observation_ids

    FROM {{ ref('int_palliative_care_diagnoses_all') }}
    WHERE CAST(clinical_effective_date AS DATE) <= CURRENT_DATE()
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        pc.*,

        -- Care on or after 1 April 2008, with no "no longer indicated" code on a later date
        COALESCE(
            latest_diagnosis_date IS NOT NULL
            AND (
                latest_no_longer_indicated_date IS NULL
                OR latest_no_longer_indicated_date::DATE <= latest_diagnosis_date::DATE
            ), FALSE
        ) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN
                latest_diagnosis_date IS NOT NULL
                AND (
                    latest_no_longer_indicated_date IS NULL
                    OR latest_no_longer_indicated_date::DATE <= latest_diagnosis_date::DATE
                )
                THEN 'Active palliative care'
            WHEN
                latest_diagnosis_date IS NOT NULL
                AND latest_no_longer_indicated_date::DATE > latest_diagnosis_date::DATE
                THEN 'Palliative care - no longer indicated'
            WHEN
                earliest_diagnosis_date IS NOT NULL
                AND latest_diagnosis_date IS NULL
                THEN 'Palliative care before QOF date (pre-2008)'
            ELSE 'No palliative care diagnosis'
        END AS palliative_care_status,


    FROM palliative_care_diagnoses AS pc
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.palliative_care_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.earliest_no_longer_indicated_date,
    ri.latest_no_longer_indicated_date,
    ri.total_palliative_care_episodes,
    ri.total_no_longer_indicated_episodes,
    ri.palliative_care_codes,
    ri.palliative_care_displays,
    ri.no_longer_indicated_codes,
    ri.no_longer_indicated_displays,
    ri.all_observation_ids

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
