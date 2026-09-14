with preferred_sources as (
    select column1 as code_set_name, column2 as source_code_set_name
    from values
        ('admission_source', 'Admission_Source')
        , ('planned_discharge_destination', 'Planned_Destination_Of_Discharge')
        , ('discharge_destination', 'Destination_Of_Discharge')
)

, preferred_definitions as (
    select
        history.code_set_name
        , history.code
        , history.source_code_set_name
    from {{ ref('mhsds_inpatient_code_lookup_history') }} as history
    inner join preferred_sources
        on history.code_set_name = preferred_sources.code_set_name
        and history.source_code_set_name
            = preferred_sources.source_code_set_name
    where history.is_latest_definition
)

select
    preferred.code_set_name
    , count(*) as failure_count
from preferred_definitions as preferred
left join {{ ref('mhsds_inpatient_code_lookup') }} as selected
    on preferred.code_set_name = selected.code_set_name
    and preferred.code = selected.code
where selected.code is null
    or selected.source_code_set_name is distinct from preferred.source_code_set_name
group by preferred.code_set_name
having count(*) > 0
