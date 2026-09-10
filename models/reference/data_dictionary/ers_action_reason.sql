with dictionary_codes as (
    select code, min(name) as name
    from (
        select code::varchar as code, display as name from {{ ref('stg_dictionary_ers_e_referral_reason') }}
union all
        select code::varchar as code, display as name from {{ ref('stg_dictionary_ers_triageoutcometype') }}
union all
        select code::varchar as code, display as name from {{ ref('stg_dictionary_ers_adviceresponseoutcome') }}
union all
        select code::varchar as code, display as name from {{ ref('stg_dictionary_ers_adviceclosereason') }}
    )
    group by code
), recorded_labels as (
    -- Retain older codes absent from the maintained dictionary using their latest supplied display.
    select
        a.action_reason_cd::varchar as code,
        nullif(trim(a.action_reason_desc), '') as name,
        max(a.action_id) as last_observed_action_id
    from {{ ref('stg_ers_ubrn_action') }} as a
    where a.action_reason_cd is not null
        and nullif(trim(a.action_reason_desc), '') is not null
        and not exists (select 1 from dictionary_codes d where d.code = a.action_reason_cd::varchar)
    group by a.action_reason_cd, nullif(trim(a.action_reason_desc), '')
    qualify row_number() over (partition by code order by last_observed_action_id desc, name) = 1
)
select code, name, 'Dictionary.E-Referral' as name_source
from dictionary_codes
union all
select code, name, 'Latest supplied e-RS action-reason display' as name_source
from recorded_labels
