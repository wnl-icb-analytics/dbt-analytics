{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Familial Hypercholesterolaemia (FH) register fact table - one row per person.
Applies clinical FH register inclusion criteria.

Clinical Purpose:
- Clinical FH register for genetic cardiovascular risk management
- Clinical register (NOT part of QOF)
- Familial hypercholesterolaemia cascade screening
- High-intensity statin therapy monitoring
- Family screening pathway identification

Register Criteria:
- Any FH diagnosis code (FHYP_COD)
- Any age: QOF v51 and the NICE indicators that exclude FH use any FHYP_COD diagnosis
  (age limits for prevalence come from ltc_register_denominator_rules)
- No resolution codes (genetic condition)
- Important for cascade family screening programmes

Includes only active patients as per standard population requirements.
This table provides one row per person for analytical use.
*/

WITH fh_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END) AS total_fh_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END)
            AS fh_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        )
            AS fh_diagnosis_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT ID::VARCHAR) AS all_IDs

    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        fd.*,

        -- Age at first diagnosis, from the birth date
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN
                    FLOOR(DATEDIFF('month', age.birth_date_approx, earliest_diagnosis_date) / 12)
        END AS age_at_first_fh_diagnosis,

        -- Register logic: any FH diagnosis
        earliest_diagnosis_date IS NOT NULL AS is_on_register

    FROM fh_diagnoses AS fd
    INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
        ON fd.person_id = ap.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON fd.person_id = age.person_id
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.age_at_first_fh_diagnosis,
    ri.total_fh_episodes,
    ri.fh_diagnosis_codes,
    ri.fh_diagnosis_displays,
    ri.all_IDs

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
