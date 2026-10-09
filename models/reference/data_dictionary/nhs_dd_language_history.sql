select *
from (
    {{ select_data_dictionary_history('stg_ukhfd_other_iso_language_codes') }}
)

union all

select *
from (
    {{ select_data_dictionary_history('stg_ukhfd_data_dictionary_language_code') }}
)
