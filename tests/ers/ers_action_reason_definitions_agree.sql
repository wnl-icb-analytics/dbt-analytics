with supplied as (
    select code::varchar as code, display as name
    from {{ ref('stg_dictionary_ers_e_referral_reason') }}
union all
    select code::varchar as code, display as name
    from {{ ref('stg_dictionary_ers_triageoutcometype') }}
union all
    select code::varchar as code, display as name
    from {{ ref('stg_dictionary_ers_adviceresponseoutcome') }}
union all
    select code::varchar as code, display as name
    from {{ ref('stg_dictionary_ers_adviceclosereason') }}
)
select code
from supplied
group by code
having count(distinct name) > 1
