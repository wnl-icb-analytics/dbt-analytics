{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

-- NICE flu administrations and vaccine orders for all people, including inactive and deceased people.
SELECT
    'OBSERVATION' AS source_kind,
    obs.id::VARCHAR AS source_event_id,
    obs.cluster_id,
    obs.person_id,
    obs.clinical_effective_date::DATE AS event_date,
    obs.cluster_id = 'LAIV_COD' AS is_laiv
FROM ({{ get_observations("'FLUVAX_COD', 'LAIV_COD'", 'UKHSA_FLU') }}) AS obs

UNION ALL

SELECT
    'MEDICATION_ORDER' AS source_kind,
    med.medication_order_id::VARCHAR AS source_event_id,
    med.cluster_id,
    med.person_id,
    med.order_date::DATE AS event_date,
    med.cluster_id = 'LAIVRX_COD' AS is_laiv
FROM ({{ get_medication_orders(cluster_id="'FLURX_COD', 'LAIVRX_COD'", source='UKHSA_FLU') }}) AS med
