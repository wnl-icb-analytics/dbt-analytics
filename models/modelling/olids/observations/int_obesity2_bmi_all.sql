{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/* OBES2 BMI inputs. One row per observation and matched QOF cluster. */

SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.date_recorded,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    TRY_CAST(obs.result_value AS FLOAT) AS bmi_value,
    obs.result_unit_display,
    obs.cluster_id = 'BMI35_COD' AS is_bmi_35_code
FROM (
    {{ get_observations("'BMI35_COD', 'BMIVAL_COD'", source='PCD', include_history=true) }}
) AS obs
