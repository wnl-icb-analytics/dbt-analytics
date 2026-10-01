{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
Smoking status observations at age 11 or older, dated no later than today.
405746006 records a current non-smoker without establishing past history.
*/

WITH status_codes AS (
    SELECT
        code AS mapped_concept_code,
        cluster_id AS source_cluster_id
    FROM {{ ref('stg_reference_combined_codesets') }}
    WHERE cluster_id IN ('LSMOK_COD', 'EXSMOK_COD', 'NSMOK_COD')
        OR (cluster_id = 'SMOK_COD' AND code = '405746006')
    -- Resolve membership before the observation join to avoid multiplying records.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY code
        ORDER BY CASE cluster_id
            WHEN 'LSMOK_COD' THEN 1
            WHEN 'EXSMOK_COD' THEN 2
            WHEN 'NSMOK_COD' THEN 3
            ELSE 4
        END
    ) = 1
),

base_observations AS (
    SELECT
        obs.id,
        obs.person_id,
        -- Match the shared get_observations date correction.
        CASE WHEN obs.clinical_effective_date > obs.date_recorded
            THEN obs.date_recorded
            ELSE COALESCE(obs.clinical_effective_date, '1900-01-01')
        END AS clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS code_description,
        codes.source_cluster_id,
        codes.source_cluster_id = 'LSMOK_COD' AS is_smoker_code,
        codes.source_cluster_id = 'EXSMOK_COD' AS is_ex_smoker_code,
        codes.source_cluster_id = 'NSMOK_COD' AS is_never_smoked_code
    FROM {{ ref('stg_olids_observation') }} AS obs
    INNER JOIN status_codes AS codes
        ON obs.mapped_concept_code = codes.mapped_concept_code
    WHERE obs.age_at_event >= 11 -- Exclude parent smoking codes on children's records (#595).
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
        ELSE 'Non-Smoker (History Unknown)'
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
WHERE clinical_effective_date <= CURRENT_DATE()
ORDER BY person_id, clinical_effective_date DESC
