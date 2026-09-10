with dictionary_codes as (
    select code::varchar as code, display as name
    from {{ ref('stg_dictionary_ers_action') }}
), recorded_labels as (
    -- Retain codes absent from the maintained dictionary using their latest supplied display.
    select
        a.action_cd::varchar as code,
        nullif(trim(a.action_desc), '') as name,
        max(a.action_id) as last_observed_action_id
    from {{ ref('stg_ers_ubrn_action') }} as a
    where a.action_cd is not null
        and nullif(trim(a.action_desc), '') is not null
        and not exists (select 1 from dictionary_codes d where d.code = a.action_cd::varchar)
    group by a.action_cd, nullif(trim(a.action_desc), '')
    qualify row_number() over (partition by code order by last_observed_action_id desc, name) = 1
)
select code, name, 'Dictionary.E-Referral' as name_source
from dictionary_codes
union all
select code, name, 'Latest supplied e-RS action display' as name_source
from recorded_labels
