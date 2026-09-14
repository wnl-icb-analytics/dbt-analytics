select
    service_id,
    service_name,
    beg_effective_dt,
    end_effective_dt
from {{ ref('raw_dictionary_ers_service') }}
