-- Aggregate coverage only. Reading adapters avoids the shared view's timeline sort.
with records as (
    {% for system in ['csds', 'mhsds', 'ecds'] %}
    select source_dataset, source_code, source_coding_system,
        mapped_code, mapped_code_name, mapped_coding_system
    from {{ ref('int_' ~ system ~ '_person_clinical_record') }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select
    r.source_dataset,
    count(*) as record_count,
    count_if(r.mapped_code is not null) as mapped_record_count,
    count_if(source_concept.snomed_code is not null) as recognised_source_snomed_count,
    count_if(source_concept.snomed_code is not null and not source_concept.is_active) as historical_source_snomed_count,
    count_if(source_concept.snomed_code is not null
        and r.mapped_code is distinct from source_concept.snomed_code) as recognised_source_not_retained_count,
    count_if(r.mapped_code is not null and mapped_concept.snomed_code is null) as unrecognised_target_count,
    count_if(r.mapped_code is not null
        and r.mapped_code_name is distinct from mapped_concept.preferred_term) as outdated_target_label_count
from records as r
left join {{ ref('snomed_concept') }} as source_concept
    on trim(r.source_code) = source_concept.snomed_code
    and r.source_coding_system like 'SNOMED CT%'
left join {{ ref('snomed_concept') }} as mapped_concept
    on r.mapped_code = mapped_concept.snomed_code
group by r.source_dataset
