{{ config(materialized='table', cluster_by=['person_id', 'evidence_date']) }}

-- Event evidence for the existing NICE COVID immunosuppression proxy, without campaign membership.
WITH current_campaign AS (
    {{ covid_autumn_config() }}
)

SELECT
    'OBSERVATION' AS source_kind,
    obs.id::VARCHAR AS source_event_id,
    obs.person_id,
    obs.clinical_effective_date AS evidence_date,
    obs.cluster_id AS evidence_type,
    obs.spec_version AS terminology_version
FROM ({{ get_observations("'IMMDX_COV_COD', 'IMM_ADM_COD', 'DXT_CHEMO_COD'", 'UKHSA_COVID', versioned=true) }}) AS obs
INNER JOIN current_campaign AS campaign
    ON obs.spec_version = campaign.terminology_version

UNION ALL

SELECT
    'MEDICATION_ORDER' AS source_kind,
    med.medication_order_id::VARCHAR AS source_event_id,
    med.person_id,
    med.order_date AS evidence_date,
    med.cluster_id AS evidence_type,
    med.spec_version AS terminology_version
FROM ({{ get_medication_orders(cluster_id='IMMRX_COD', source='UKHSA_COVID', versioned=true) }}) AS med
INNER JOIN current_campaign AS campaign
    ON med.spec_version = campaign.terminology_version
