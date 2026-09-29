with definitions as (
    select
        upper(trim(unit_symbol)) as code
        , unit_symbol
        , unit_name
        , quantity_name
        , si_power
        , is_standard_unit
        , is_derived_unit
        , 'canonical_symbol' as match_type
        , 1 as match_priority
    from {{ ref('stg_dictionary_dbo_unit') }}

    union all

    select
        upper(trim(m.unit_label)) as code
        , u.unit_symbol
        , u.unit_name
        , u.quantity_name
        , u.si_power
        , u.is_standard_unit
        , u.is_derived_unit
        , 'known_alias' as match_type
        , 2 as match_priority
    from {{ ref('stg_dictionary_dbo_unit_mapping') }} as m
    inner join {{ ref('stg_dictionary_dbo_unit') }} as u
        on m.sk_unit_id = u.sk_unit_id
)

select * exclude match_priority
from definitions
qualify row_number() over (
    partition by code
    order by match_priority, is_standard_unit desc nulls last, unit_symbol
) = 1
