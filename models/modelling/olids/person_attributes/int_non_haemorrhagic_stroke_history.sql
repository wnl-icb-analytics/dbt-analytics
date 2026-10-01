{{ config(materialized='table', cluster_by=['person_id']) }}

-- QOF v51 STIA007 requires recorded non-haemorrhagic stroke, rather than absence of haemorrhage.
SELECT
    obs.person_id,
    MIN(obs.clinical_effective_date_raw::DATE) AS earliest_recorded_date,
    BOOLOR_AGG(obs.clinical_effective_date_raw IS NULL) AS has_undated_record
FROM ({{ get_observations("'OSTR_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date_raw IS NULL
    OR obs.clinical_effective_date_raw::DATE <= CURRENT_DATE()
GROUP BY obs.person_id
