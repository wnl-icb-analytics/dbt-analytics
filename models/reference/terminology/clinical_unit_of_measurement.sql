with definitions as (
    select
        code
        , code as unit_symbol
        , description
        , quantity_name
        , 'ucum_code' as match_type
        , 'UKHFD UCUM' as definition_source
        , 1 as priority
    from {{ ref('ucum_unit') }}

    union all

    select synonym, code, description, quantity_name,
        'known_alias', 'UKHFD UCUM', 2
    from {{ ref('ucum_unit') }}
    where synonym is not null

    union all

    select unit_symbol, unit_symbol, unit_name, quantity_name,
        'dictionary_symbol', 'Dictionary.dbo.Unit', 3
    from {{ ref('stg_dictionary_dbo_unit') }}
    where unit_symbol is not null

    union all

    select m.unit_label, u.unit_symbol, u.unit_name, u.quantity_name,
        'known_alias', 'Dictionary.dbo.UnitMapping', 4
    from {{ ref('stg_dictionary_dbo_unit_mapping') }} as m
    inner join {{ ref('stg_dictionary_dbo_unit') }} as u
        on m.sk_unit_id = u.sk_unit_id
    where m.unit_label is not null
)

, preferred as (
    select *
    from definitions
    qualify priority = min(priority) over (partition by code)
)

select * exclude priority
from preferred
-- Ambiguous aliases remain unresolved; UCUM codes always take precedence.
qualify count(distinct unit_symbol) over (partition by code) = 1
    and row_number() over (partition by code order by description, definition_source) = 1
