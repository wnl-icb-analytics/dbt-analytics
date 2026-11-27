-- The shared SUS commissioner lookup must prefer an exact five-character code
-- to the legacy three-character fallback.
select
    'int_sus_uec_encounter' as model_name
    , count(*) as mismatched_rows
from {{ ref('int_sus_uec_encounter') }} as encounter
inner join {{ ref('organisation') }} as exact_code
    on encounter.commissioner_code_at_event = exact_code.organisation_code
where encounter.commissioner_code_at_event in ('VPP00', 'TDH00')
  and encounter.commissioner_name_at_event is distinct from exact_code.organisation_name
having count(*) > 0

union all

select
    'int_sus_uec_encounter residence area' as model_name
    , count(*) as mismatched_rows
from {{ ref('int_sus_uec_encounter') }} as encounter
inner join {{ ref('organisation') }} as exact_code
    on encounter.residence_area_code_at_event = exact_code.organisation_code
where encounter.residence_area_code_at_event in ('VPP00', 'TDH00')
  and encounter.residence_area_name_at_event is distinct from exact_code.organisation_name
having count(*) > 0
