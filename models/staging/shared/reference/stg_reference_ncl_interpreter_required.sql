select
    interpreter_required
from {{ ref('raw_reference_lookup_ncl_interpreter_required') }}
