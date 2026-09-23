-- Retain each historical code; the latest bulk import can contain several releases.
select
    case code_set_name
        when 'CODED OBSERVATION' then 'observation'
        when 'CODED ASSESSMENT TOOL TYPE' then 'scored_assessment'
    end as record_type
    , snomed_code
    , preferred_term
    , case
        when code_set_name = 'CODED OBSERVATION' then preferred_term
        when ecds_group1 = 'NEWS2' then 'National Early Warning Score 2'
        when ecds_group1 = 'PAIN' then 'Pain'
        when snomed_code = '1239211000000103' then 'Delirium'
        when snomed_code = '763264000' then 'Frailty'
    end as measurement_category
    , ecds_description
    , ecds_group1
    , notes
    , valid_from
    , source_file_name
    , source_file_at
from {{ ref('stg_ukhfd_ecds_code_sets') }}
where code_set_name in ('CODED OBSERVATION', 'CODED ASSESSMENT TOOL TYPE')
    and nullif(trim(snomed_code), '') is not null
qualify row_number() over (
    partition by code_set_name, snomed_code
    order by source_file_at desc nulls last, source_file_name desc,
        source_imported_at desc nulls last, source_record_id desc
) = 1
