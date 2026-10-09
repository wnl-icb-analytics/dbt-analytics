select visit_occurrence_id
from {{ ref('obt_encounter_uec') }}
where commissioner_code_at_event is distinct from coalesce(
    cam_commissioner_code_at_event
    , dscro_commissioner_code_at_event
)
or commissioner_source_at_event is distinct from case
    when cam_commissioner_code_at_event is not null then 'CAM'
    when dscro_commissioner_code_at_event is not null then 'DSCRO'
end
