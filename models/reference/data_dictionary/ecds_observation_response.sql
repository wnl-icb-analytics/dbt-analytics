-- ETOS code notes list each permitted categorical response on its own line as: Label = "value".
with note_line as (
    select
        m.snomed_code
        , trim(line.value, ' \r\t') as note_line
        , m.source_file_name
    from {{ ref('ecds_measurement_code') }} as m
        , lateral split_to_table(m.notes, '\n') as line
    where m.record_type = 'observation'
)

select
    snomed_code as observation_code
    , trim(split_part(note_line, '=', 2), ' "') as response_value
    , trim(split_part(note_line, '=', 1)) as response_name
    , source_file_name
from note_line
where regexp_like(note_line, '[^="]+= *"[^"]+"')
