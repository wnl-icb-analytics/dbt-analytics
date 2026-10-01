select
    hx_id,
    activated_date,
    enrolled_date,
    onboarded_date,
    discharged_date,
    file_date,
    source_file,
    loaded_at
from {{ ref('raw_myria_enrolled_patients') }}
