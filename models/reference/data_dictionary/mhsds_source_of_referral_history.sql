select *
from (
    {{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mhsds_source_of_referral') }}
)

union all

select *
from (
    {{ select_data_dictionary_history('stg_ukhfd_data_dictionary_mental_health_source_of_referral_legacy') }}
)
