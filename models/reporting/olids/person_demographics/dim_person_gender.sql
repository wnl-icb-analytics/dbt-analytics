{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'gender'],
        cluster_by=['person_id'])
}}

-- Person Gender Dimension Table
-- Derives gender from gender concepts using dynamic concept lookups
-- Ensures one row per person by preferring a mapped patient with gender; falls back to current registration

WITH current_patient_per_person AS (
    -- Current registration per person (for fallback context)
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.sk_patient_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ipr.person_id
        ORDER BY ipr.registration_start_date DESC, ipr.episode_of_care_id DESC
    ) = 1
),

best_patient_with_gender AS (
    -- Choose a single best patient per person: prefer one with a gender_source_concept_id
    SELECT
        pp.person_id,
        p.id AS patient_id,
        p.gender_source_concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id
            ORDER BY CASE WHEN p.gender_source_concept_id IS NOT NULL THEN 1 ELSE 2 END,
                     p.id DESC
        ) AS rn
    FROM {{ ref('int_patient_person_unique') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
),

all_persons AS (
    SELECT person_id FROM {{ ref('dim_person') }}
)

SELECT
    person_id,
    gender
FROM (
    SELECT
        ap.person_id,
        COALESCE(
            -- "Finding related to biological sex" is the SNOMED parent grouper (429019009)
            -- mapped to source code 'U'/'Unknown'; non-informative, so fall through to source display
            NULLIF(p_best.gender_display, 'Finding related to biological sex'),
            NULLIF(p_curr.gender_display, 'Finding related to biological sex'),
            p_best.gender_source_display, p_curr.gender_source_display, 'Unknown'
        ) AS gender,
        ROW_NUMBER() OVER (
            PARTITION BY ap.person_id
            ORDER BY
                CASE WHEN p_best.gender_display IS NOT NULL THEN 1 
                     WHEN p_curr.gender_display IS NOT NULL THEN 2
                     WHEN p_best.gender_source_display IS NOT NULL THEN 3
                     WHEN p_curr.gender_source_display IS NOT NULL THEN 4
                     ELSE 5 END,
                p_best.gender_display,
                p_curr.gender_display,
                p_best.gender_source_display,
                p_curr.gender_source_display
        ) AS rn
    FROM all_persons AS ap
    LEFT JOIN best_patient_with_gender AS bpg
        ON ap.person_id = bpg.person_id AND bpg.rn = 1
    LEFT JOIN current_patient_per_person AS cpp
        ON ap.person_id = cpp.person_id
    -- Prefer gender from best mapped patient; fall back to current registration's patient
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p_best
        ON bpg.patient_id = p_best.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p_curr
        ON cpp.patient_id = p_curr.id
) ranked
WHERE rn = 1
