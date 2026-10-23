{% test navigation_snomed_mapping(model) %}
    select r.clinical_record_id
    from {{ model }} as r
    left join {{ ref('snomed_concept') }} as source_concept
        on trim(r.source_code) = source_concept.snomed_code
        and r.source_coding_system like 'SNOMED CT%'
    left join {{ ref('snomed_concept') }} as mapped_concept
        on r.mapped_code = mapped_concept.snomed_code
    -- Historical deliveries can await new reference entries until full refresh.
    where (source_concept.snomed_code is not null
            {% if not flags.FULL_REFRESH %}
            and r.mapped_code is not null
            {% endif %}
            and r.mapped_code is distinct from source_concept.snomed_code)
        or (r.mapped_code is not null and mapped_concept.snomed_code is null)
        or (r.mapped_code is not null and r.mapped_coding_system is distinct from 'SNOMED CT')
{% endtest %}
