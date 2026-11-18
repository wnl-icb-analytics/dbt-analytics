{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

WITH events AS (
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        cluster_id AS source_cluster_id, 'ECL_CACHE' AS source
    FROM ({{ get_observations("'WEIGHT_MGMT_REFERRAL_COD', 'WEIGHT_MGMT_ATTENDING_COD', 'WEIGHT_MGMT_ENDED_COD', 'WEIGHT_MGMT_OFFERED'", source='ECL_CACHE') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
    UNION ALL
    SELECT id, person_id, clinical_effective_date::DATE AS event_date,
        cluster_id AS source_cluster_id, 'PCD' AS source
    FROM ({{ get_observations("'WTMGREFDEC_COD'", source='PCD') }}) AS observation
    WHERE clinical_effective_date::DATE <= CURRENT_DATE()
)
SELECT id, person_id, event_date, source_cluster_id, source,
    source_cluster_id = 'WEIGHT_MGMT_REFERRAL_COD' AS is_referral,
    source_cluster_id = 'WEIGHT_MGMT_OFFERED' AS is_offer,
    source_cluster_id = 'WTMGREFDEC_COD' AS is_decline,
    source_cluster_id = 'WEIGHT_MGMT_ATTENDING_COD' AS is_attendance,
    source_cluster_id = 'WEIGHT_MGMT_ENDED_COD' AS is_end
FROM events
