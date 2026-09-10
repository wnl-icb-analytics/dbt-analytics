-- Whole-table column coverage; no identifiers or patient records are returned.
with stats as (
    select object_construct(
        'appointment_id', object_construct('populated', count(appointment_id), 'blank', count_if(trim(appointment_id::varchar) = '')),
        'action_id', object_construct('populated', count(action_id), 'blank', count_if(trim(action_id::varchar) = ''))
    ) as fields
    from {{ ref('rel_ers_appointment_action') }}
)
select f.key as column_name, f.value as coverage
from stats, lateral flatten(input => fields) f
