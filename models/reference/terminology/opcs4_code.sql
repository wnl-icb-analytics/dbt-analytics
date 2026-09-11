with definitions as (
    select code, description, 'NHS TRUD' as definition_source,
        1 as priority, null::timestamp_ntz as updated_at, null::number as source_key
    from {{ ref('stg_reference_opcs4_code_term_latest') }}

    union all

    select replace(upper(trim(code)), '.', ''), description,
        'Dictionary.dbo.Procedure', 2, date_updated::timestamp_ntz, sk_procedure_code
    from {{ ref('stg_dictionary_dbo_procedure') }}
)
select code, description, definition_source
from definitions
where nullif(code, '') is not null
qualify row_number() over (
    partition by code
    order by iff(description is null, 1, 0), priority, updated_at desc nulls last, source_key desc nulls last
) = 1
