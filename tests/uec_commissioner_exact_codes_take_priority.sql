-- Exact five-character commissioner codes must take priority over the
-- three-character fallback used for legacy *00 values absent from the lookup.
with commissioner_codes as (
    select commissioner_code, commissioner_name
    from {{ ref('stg_dictionary_dbo_commissioner') }}
    qualify row_number() over (
        partition by commissioner_code
        order by coalesce(end_date, '9999-12-31'::date) desc, start_date desc
    ) = 1
)
select
    encounter.visit_occurrence_id
    , 'residence_commissioner' as field_name
from {{ ref('int_sus_uec_encounter') }} as encounter
inner join commissioner_codes as exact_code
    on encounter.residence_area_code_at_event = exact_code.commissioner_code
where encounter.residence_area_code_at_event in ('VPP00', 'TDH00')
  and encounter.residence_area_name_at_event is distinct from exact_code.commissioner_name

union all

select
    encounter.visit_occurrence_id
    , 'assigned_commissioner' as field_name
from {{ ref('int_sus_uec_encounter') }} as encounter
inner join commissioner_codes as exact_code
    on encounter.assigned_commissioner_code_at_event = exact_code.commissioner_code
where encounter.assigned_commissioner_code_at_event in ('VPP00', 'TDH00')
  and encounter.assigned_commissioner_name_at_event is distinct from exact_code.commissioner_name
