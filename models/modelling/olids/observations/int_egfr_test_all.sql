{{ config(materialized='view') }}

-- QOF v51 EGFR_DAT records a performed test without requiring a numeric result.
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    obs.result_value IS NOT NULL AS is_result_recorded
FROM ({{ get_observations("'EGFR_COD'", source='PCD') }}) AS obs
WHERE obs.clinical_effective_date::DATE <= CURRENT_DATE()
