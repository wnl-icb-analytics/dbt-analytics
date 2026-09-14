select
    'child_protection_plan_status' as code_set_name,
    definitions.*
from (
    {{ select_data_dictionary_history(
        'stg_ukhfd_data_dictionary_child_protection_plan_indicator'
    ) }}
) as definitions

union all

select
    'looked_after_child_indicator' as code_set_name,
    definitions.*
from (
    {{ select_data_dictionary_history(
        'stg_ukhfd_data_dictionary_looked_after_child_indicator'
    ) }}
) as definitions
