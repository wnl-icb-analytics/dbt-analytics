{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_method_of_discharge') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_discharge_method_legacy') }}
