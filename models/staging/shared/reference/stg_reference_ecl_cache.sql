select
    cluster_id,
    code,
    display,
    system,
    last_refreshed,
    ecl_expression_hash
from {{ ref('raw_snomed_cluster_manager_ecl_cache') }}
