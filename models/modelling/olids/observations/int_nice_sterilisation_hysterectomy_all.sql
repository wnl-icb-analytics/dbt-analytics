{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

SELECT id, person_id, clinical_effective_date::DATE AS event_date, cluster_id AS source_cluster_id
FROM ({{ get_observations("'NHSD_PRIMARY_CARE_DOMAIN_REFSETS/STERIL_COD', 'NHSD_PRIMARY_CARE_DOMAIN_REFSETS/HYST2_COD'", source='OPENCODELISTS') }}) AS observation
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
