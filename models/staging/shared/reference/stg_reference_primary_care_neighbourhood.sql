select
    neighbourhood_code
    , neighbourhood_code_legacy
    , neighbourhood_name
    , neighbourhood_name_with_borough
    , sub_icb_code
    , health_borough_name
    , registered_borough_name
    , is_active
    , updated_by
    , updated_at
from {{ ref('raw_reference_primary_care_neighbourhood') }}
