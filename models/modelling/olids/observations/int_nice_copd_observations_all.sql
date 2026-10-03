{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

WITH observations AS (
    SELECT * FROM ({{ get_observations("'FEV1_COD', 'FEV1_PCT_PRED_COD', 'PULREHAB_OFFERED_COD'", source='ECL_CACHE') }})
    UNION ALL
    SELECT * FROM ({{ get_observations("'COPD_COD', 'COPDINVITE_COD'", source='PCD') }})
    UNION ALL
    -- National referral evidence includes retired predecessors of the maintained codes.
    SELECT * FROM ({{ get_observations("'PULRHBOFF_COD'", source='PCD', include_history=true) }})
    UNION ALL
    SELECT * FROM ({{ get_observations("'ARDENS/SPO2_SATS_SATURATIONS'", source='OPENCODELISTS') }})
)
SELECT person_id, id AS observation_id, clinical_effective_date::DATE AS event_date,
    date_recorded::DATE AS date_recorded, cluster_id AS evidence_type,
    TRY_TO_DOUBLE(result_value::VARCHAR) AS result_value,
    result_unit_display,
    -- The broad COPD list also contains other stages; its maintained label identifies stage 4.
    cluster_id = 'COPD_COD'
        AND code_description = 'Very severe chronic obstructive pulmonary disease (disorder)'
        AS is_very_severe_copd
FROM observations
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
