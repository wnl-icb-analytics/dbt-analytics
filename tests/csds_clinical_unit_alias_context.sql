-- Historical unit labels require the named observation context and cannot replace exact matches.
select f.source_record_id, 'unsupported_alias_context' as failure_reason
from {{ ref('fct_csds_clinical_record') }} as f
left join {{ ref('clinical_unit_of_measurement') }} as exact_unit
    on trim(f.unit_of_measurement_code) = exact_unit.code
left join {{ ref('csds_observation_unit_alias') }} as alias_unit
    on upper(trim(f.unit_of_measurement_code)) = alias_unit.source_unit_upper
    and case
        when f.clinical_code_system = 'SNOMED CT' and trim(f.coding_scheme_code) = '03'
            then trim(f.clinical_code)
        when f.clinical_code_system in ('Read v2', 'CTV3')
            then f.dictionary_snomed_code
    end = alias_unit.snomed_code
where f.unit_of_measurement_match_type = 'csds_etos_alias'
    and (f.source_table is distinct from 'CYP202'
        or f.clinical_record_type is distinct from 'observation'
        or exact_unit.code is not null or alias_unit.snomed_code is null
        or f.unit_of_measurement_description is distinct from alias_unit.description
        or f.unit_of_measurement_symbol is distinct from alias_unit.unit_symbol
        or f.unit_of_measurement_definition_source is distinct from alias_unit.definition_source)

union all

-- Both analyst interfaces must preserve the submitted unit and value and give the same unit label.
select f.source_record_id, 'activity_unit_disagreement'
from {{ ref('fct_csds_clinical_record') }} as f
inner join {{ ref('fct_csds_care_activity') }} as a
    on f.care_activity_source_record_id = a.source_record_id
    and f.submission_id = a.submission_id
where f.source_table = 'CYP202' and f.clinical_record_type = 'observation'
    and (f.unit_of_measurement_code is distinct from a.observation_unit_code
        or f.clinical_value is distinct from a.observation_value::varchar
        or f.unit_of_measurement_description is distinct from a.observation_unit_name
        or f.unit_of_measurement_symbol is distinct from a.observation_unit_symbol
        or f.unit_of_measurement_match_type is distinct from a.observation_unit_match_type
        or f.unit_of_measurement_definition_source is distinct from a.observation_unit_definition_source)
