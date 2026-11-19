select
    sk_date,
    full_date,
    holiday,
    in_england_and_wales,
    in_northern_ireland,
    in_scotland
from {{ ref('raw_dictionary_dbo_datebankholiday') }}