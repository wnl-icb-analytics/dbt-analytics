select
    valueset_id,
    report_id,
    valueset_index,
    valueset_hash,
    valueset_friendly_name,
    code_system,
    expansion_error,
    expanded_at
from {{ ref('raw_reference_ltc_lcs_valuesets') }}
