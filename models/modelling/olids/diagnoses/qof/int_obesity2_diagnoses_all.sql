{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/* OBES2 diagnosis inputs. One row per observation and matched QOF cluster. */

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.cluster_id = 'ASCVD_COD' AS is_ascvd_code,
    obs.cluster_id = 'OBSLPAPNOEA_COD' AS is_obstructive_sleep_apnoea_code
FROM (
    {{ get_observations("'ASCVD_COD', 'OBSLPAPNOEA_COD'", source='PCD', include_history=true) }}
) AS obs
