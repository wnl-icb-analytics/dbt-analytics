with current_definitions as (
    select code, source_code_set_name
    from {{ ref('stg_ukhfd_data_dictionary_mhsds_source_of_referral') }}
    where is_latest_definition
),

legacy_only_codes as (
    select legacy.code
    from {{ ref('stg_ukhfd_data_dictionary_mental_health_source_of_referral_legacy') }} as legacy
    left join current_definitions as current_definition
        on legacy.code = current_definition.code
    where legacy.is_latest_definition
      and current_definition.code is null
),

selected_definitions as (
    select code, source_code_set_name
    from {{ ref('mhsds_source_of_referral') }}
)

select current_definition.code
from current_definitions as current_definition
left join selected_definitions as selected
    on current_definition.code = selected.code
where selected.source_code_set_name != current_definition.source_code_set_name
   or selected.code is null

union all

select legacy.code
from legacy_only_codes as legacy
left join selected_definitions as selected
    on legacy.code = selected.code
where selected.code is null
