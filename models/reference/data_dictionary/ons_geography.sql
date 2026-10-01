select geography_code as code, geography_name as name,
    date_of_introduction as introduced_date,
    date_of_termination as terminated_date
from {{ ref('stg_dictionary_dbo_onscodeequivalent') }}
qualify row_number() over (
    partition by geography_code
    order by import_date desc nulls last, created_date desc nulls last, geography_name
) = 1
