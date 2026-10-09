select visit_occurrence_id
from {{ ref('obt_encounter_uec') }}
where four_hour_breach_status != case
    when duration >= 240 then 'Y'
    when duration < 240 then 'N'
    else 'Unknown'
    end
