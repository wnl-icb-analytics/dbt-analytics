{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/* OBES2 lipid test inputs. One row per observation and matched QOF cluster. */

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    TRY_CAST(obs.result_value AS FLOAT) AS lipid_value,
    obs.result_unit_display
FROM (
    {{ get_observations("'LDLCCHOL_COD', 'TRIGLYC_COD', 'HDLCCHOL_COD'", source='PCD', include_history=true) }}
) AS obs
WHERE TRY_CAST(obs.result_value AS FLOAT) IS NOT NULL
