{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- TTR observations for all people; valued-record selection belongs to the AF profile.
SELECT
    obs.id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.clinical_effective_date_raw,
    obs.date_recorded,
    obs.result_value AS original_result_value,
    TRY_CAST(obs.result_value AS FLOAT) AS ttr_percentage
FROM ({{ get_observations("'TTR_COD'", source='PCD') }}) AS obs
WHERE obs.clinical_effective_date::DATE <= CURRENT_DATE()
