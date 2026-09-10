select
    service_id,
    nullif(trim(service_name), '') as service_name,
    beg_effective_dt as effective_from_at,
    end_effective_dt as effective_to_at
from {{ ref('stg_dictionary_ers_service') }}
