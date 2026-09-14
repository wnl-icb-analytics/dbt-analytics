{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_destination_of_discharge') }}

union all

{{ select_data_dictionary_history('stg_ukhfd_data_dictionary_discharge_destination_legacy') }}
