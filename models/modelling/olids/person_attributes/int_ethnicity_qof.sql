{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'qof', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Intermediate Ethnicity QOF - QOF-specific ethnicity observations
-- Uses ETH2016*_COD cluster IDs from combined_codesets (QOF ethnicity codes) with LIKE pattern matching
-- Includes BAME classification for obesity register
-- Includes ALL persons regardless of active status

WITH qof_ethnicity_enriched AS (
    SELECT *
    FROM {{ ref('int_ethnicity_qof_all') }}
),

person_level_aggregation AS (
    -- Aggregate all ethnicity concept codes and displays into arrays per person
    SELECT
        person_id,
        array_agg(DISTINCT mapped_concept_code) AS all_ethnicity_concept_codes,
        array_agg(DISTINCT mapped_concept_display)
            AS all_ethnicity_concept_displays,
        max(clinical_effective_date) AS latest_ethnicity_date,
        max(CASE WHEN is_bame THEN clinical_effective_date END)
            AS latest_bame_date
    FROM qof_ethnicity_enriched
    GROUP BY person_id
)

-- Final selection with QOF ethnicity data
SELECT
    qee.person_id,
    qee.sk_patient_id,
    qee.id,
    qee.clinical_effective_date,
    qee.mapped_concept_code AS concept_code,
    qee.mapped_concept_display AS code_description,
    qee.cluster_id AS source_cluster_id,
    qee.is_bame,
    pla.latest_ethnicity_date,
    pla.latest_bame_date,
    pla.all_ethnicity_concept_codes,
    pla.all_ethnicity_concept_displays
FROM qof_ethnicity_enriched AS qee
LEFT JOIN person_level_aggregation AS pla
    ON qee.person_id = pla.person_id
-- Get one row per person (latest ethnicity record)
QUALIFY
    row_number()
        OVER (
            PARTITION BY qee.person_id ORDER BY qee.clinical_effective_date DESC, qee.id DESC
        )
    = 1
ORDER BY qee.person_id
