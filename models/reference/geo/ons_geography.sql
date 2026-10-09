select geography_code as code, geography_name as name,
    introduced_date, terminated_date
from {{ ref('stg_ukhfd_ons_codes_equivalents') }}
qualify row_number() over (
    partition by geography_code
    order by introduced_date desc nulls last, register_year desc, geography_name
) = 1
