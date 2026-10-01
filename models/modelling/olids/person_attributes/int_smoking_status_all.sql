{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Smoking habit observations, including non-smokers whose past history is unknown.
Specific QOF clusters determine current, ex-smoker and never-smoked status.
SMOK_COD-only observations cannot establish never-smoking history.
*/

WITH base_observations AS (

    SELECT
        obs.id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS code_description,
        obs.cluster_id AS source_cluster_id,

        -- Flag different types of smoking codes
        CASE WHEN obs.cluster_id = 'LSMOK_COD' THEN TRUE ELSE FALSE END AS is_smoker_code,
        CASE WHEN obs.cluster_id = 'EXSMOK_COD' THEN TRUE ELSE FALSE END AS is_ex_smoker_code,
        CASE WHEN obs.cluster_id = 'NSMOK_COD' THEN TRUE ELSE FALSE END AS is_never_smoked_code

    FROM ({{ get_observations("'SMOK_COD', 'LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
    AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
    AND obs.age_at_event >= 11 -- Filter out parent smoking codes recorded on children's records (#595)
    -- Keep one observation when its concept belongs to several clusters.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY obs.id
        ORDER BY CASE obs.cluster_id
            WHEN 'LSMOK_COD' THEN 1
            WHEN 'EXSMOK_COD' THEN 2
            WHEN 'NSMOK_COD' THEN 3
            ELSE 4
        END
    ) = 1
)

SELECT
    person_id,
    ID,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,

    -- Core boolean flags (matching legacy pattern)
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,

    -- Enhanced analytics fields
    -- Derive smoking status based on the code type
    CASE
        WHEN is_smoker_code THEN 'Current Smoker'
        WHEN is_ex_smoker_code THEN 'Ex-Smoker'
        WHEN is_never_smoked_code THEN 'Never Smoked'
        WHEN concept_code = '405746006' THEN 'Non-Smoker (History Unknown)'
        ELSE 'Unknown'
    END AS smoking_status,

    -- Current smoker indicator  
    CASE
        WHEN is_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_current_smoker,

    -- Ex-smoker indicator
    CASE
        WHEN is_ex_smoker_code THEN TRUE
        ELSE FALSE
    END AS is_ex_smoker,

    -- Never smoker indicator
    CASE
        WHEN is_never_smoked_code THEN TRUE
        ELSE FALSE
    END AS is_never_smoker,

    -- Analytics-ready risk flags
    CASE
        WHEN is_smoker_code OR is_ex_smoker_code THEN TRUE
        ELSE FALSE
    END AS has_smoking_history

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC
