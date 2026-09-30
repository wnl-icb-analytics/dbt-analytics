{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'qof', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- QOF ethnicity observations (ETH2016*_COD clusters from combined_codesets), one row
-- per observation and cluster, with the BAME classification used by the obesity register.
-- Includes ALL persons regardless of active status.

WITH mapped_observations AS (
    -- Get all observations with proper concept mapping from staging
    SELECT
        o.id AS ID,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        o.date_recorded,
        o.mapped_concept_id,
        o.mapped_concept_code,
        o.mapped_concept_display,
        ccs.cluster_id,
        ccs.cluster_description
    FROM {{ ref('stg_olids_observation') }} AS o
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON o.patient_id = p.id
    INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
        ON p.id = pp.patient_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} AS ccs
        ON o.mapped_concept_code = ccs.code
        AND ccs.cluster_id LIKE 'ETH2016%_COD'  -- Filter for ethnicity clusters
    WHERE o.clinical_effective_date IS NOT NULL
)

SELECT
    mo.*,
    -- BAME classification based on cluster IDs (same as legacy)
    coalesce(mo.cluster_id IN (
        'ETH2016MWBC_COD', -- White and Black Caribbean
        'ETH2016MWBA_COD', -- White and Black African
        'ETH2016MWA_COD',  -- White and Asian
        'ETH2016AI_COD',   -- Indian
        'ETH2016AP_COD',   -- Pakistani
        'ETH2016AB_COD',   -- Bangladeshi
        'ETH2016AC_COD',   -- Chinese
        'ETH2016AO_COD',   -- Any other Asian background
        'ETH2016BA_COD',   -- African
        'ETH2016BC_COD',   -- Caribbean
        'ETH2016BO_COD',   -- Any other Black or African or Caribbean background
        'ETH2016OA_COD'    -- Arab
    ), FALSE) AS is_bame
FROM mapped_observations AS mo
