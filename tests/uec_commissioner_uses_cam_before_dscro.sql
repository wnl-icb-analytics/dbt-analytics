select encounter.visit_occurrence_id
from {{ ref('obt_encounter_uec') }} as encounter
inner join {{ ref('stg_sus_ecds_emergency_care') }} as source
    on encounter.visit_occurrence_id = source.primarykey_id
where encounter.commissioner_code_at_event is distinct from coalesce(
    source.commissioning_commissioner_assignment_commissioner
    , source.commissioning_service_agreement_commissioner
)
or encounter.commissioner_source_at_event is distinct from case
    when source.commissioning_commissioner_assignment_commissioner is not null then 'CAM'
    when source.commissioning_service_agreement_commissioner is not null then 'DSCRO'
end
