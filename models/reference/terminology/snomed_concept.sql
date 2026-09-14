with current_terms as (
    select concept_id, description_type, term
    from {{ ref('stg_nhsd_snomed_sct_description') }}
    where active and description_type in ('P', 'F')
    qualify row_number() over (
        partition by concept_id, description_type
        order by effective_time desc nulls last, id desc
    ) = 1
)
select
    c.id::varchar as snomed_code,
    coalesce(p.term, f.term) as preferred_term,
    f.term as fully_specified_name,
    c.active as is_active,
    c.effective_time as effective_time,
    'Dictionary.NHSD_SnomedReportingModel'::varchar as definition_source
from {{ ref('stg_nhsd_snomed_sct_concept') }} as c
left join current_terms as p on c.id = p.concept_id and p.description_type = 'P'
left join current_terms as f on c.id = f.concept_id and f.description_type = 'F'
