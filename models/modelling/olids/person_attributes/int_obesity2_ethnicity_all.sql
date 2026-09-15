{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'qof'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/* Journal ethnicity inputs used by QOF OBES2. */

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id IN (
        'ETHALLMWBC_COD', 'ETHALLMWBA_COD', 'ETHALLMWA_COD',
        'ETHALLAI_COD', 'ETHALLAP_COD', 'ETHALLAB_COD',
        'ETHALLAC_COD', 'ETHALLAO_COD', 'ETHALLBA_COD',
        'ETHALLBC_COD', 'ETHALLBO_COD', 'ETHALLOA_COD'
    ) AS is_lower_bmi_threshold_ethnicity
FROM (
    {{ get_observations(
        "'ETHALLAB_COD', 'ETHALLAC_COD', 'ETHALLAI_COD', 'ETHALLAO_COD', 'ETHALLAP_COD', 'ETHALLBA_COD', 'ETHALLBC_COD', 'ETHALLBO_COD', 'ETHALLMO_COD', 'ETHALLMWA_COD', 'ETHALLMWBA_COD', 'ETHALLMWBC_COD', 'ETHALLOA_COD', 'ETHALLOO_COD', 'ETHALLWB_COD', 'ETHALLWGT_COD', 'ETHALLWI_COD', 'ETHALLWO_COD', 'ETHNICITYND_COD'",
        source='PCD',
        include_history=true
    ) }}
) AS obs
