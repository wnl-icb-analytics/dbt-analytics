select
    practice_code
    , practice_name
    , sub_icb_code
    , sub_icb_name
    , geographic_borough_name
    , pcn_name
    , neighbourhood_name
from {{ ref('raw_reference_primary_care_practice_all') }}
qualify row_number() over (
    partition by practice_code
    order by ods_last_updated desc nulls last, details_since desc nulls last
) = 1
