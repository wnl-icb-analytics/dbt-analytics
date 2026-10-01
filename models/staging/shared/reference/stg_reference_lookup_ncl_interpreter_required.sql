select
    id,
    interpreter_required,
    interpreter_required_flag
from {{ ref('raw_reference_lookup_ncl_interpreter_required') }}
