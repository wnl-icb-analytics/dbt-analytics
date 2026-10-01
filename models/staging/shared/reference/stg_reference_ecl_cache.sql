select
    cluster_id,
    code,
    display,
    system,
    last_refreshed,
    ecl_expression_hash
from {{ ref('raw_reference_ecl_cache') }}
