{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

SELECT id, person_id, clinical_effective_date, cluster_id AS source_cluster_id
FROM ({{ get_observations("'DEPSCRN_COD', 'ANXSCRN_COD'", source="PCD") }})
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY cluster_id) = 1
