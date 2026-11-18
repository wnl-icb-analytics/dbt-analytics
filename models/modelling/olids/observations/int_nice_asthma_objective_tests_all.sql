{{ config(materialized='table', cluster_by=['person_id', 'event_date']) }}

SELECT person_id, id AS observation_id, clinical_effective_date::DATE AS event_date,
    cluster_id AS test_type
FROM ({{ get_observations("'FENO_COD', 'ASTSPIR_COD', 'PEFRVAR_COD', 'SKINTEST_COD', 'IGE_COD', 'BRONCCHALENG_COD', 'EOS_COUNT', 'FBC_COD'") }})
WHERE clinical_effective_date::DATE <= CURRENT_DATE()
