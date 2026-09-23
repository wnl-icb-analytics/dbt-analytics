-- Aggregate-only reconciliation of each fact with the published clinical record.
-- Run after the facts, adapter and union have built from the same source refresh.
with expected as (
select
    'observation' as source_record_type,
    observation_id::varchar as source_record_id,
    sk_patient_id::varchar as sk_patient_id,
    'observation' as clinical_record_type,
    'fct_sus_uec_observation' as source_model_name,
    observation_code::varchar as source_code,
    'SNOMED CT' as source_coding_system,
    visit_occurrence_id::varchar as parent_record_id,
    'emergency_care_attendance' as parent_record_type,
    'obt_encounter_uec' as parent_model_name,
    'recorded_parent' as relationship_type,
    observed_at::timestamp_ntz as clinical_record_at,
    observed_at::date as clinical_record_date,
    iff(observed_at is null, 'unknown', 'timestamp') as clinical_time_precision,
    iff(observed_at is null, 'not_recorded', 'observed_at') as clinical_time_basis,
    observation_value::varchar as result_value,
    categorical_value_description::varchar as result_value_name,
    observation_value_numeric::number(38,9) as result_value_numeric,
    value_parse_status as result_value_parse_status,
    ucum_unit_code::varchar as result_unit_code,
    unit_description as result_unit_name,
    resolved_unit_symbol as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status,
    organisation_id::varchar as provider_organisation_code,
    organisation_name::varchar as provider_organisation_name,
    'ODS' as provider_code_authority
from {{ ref('fct_sus_uec_observation') }}
union all
select
    'scored_assessment' as source_record_type,
    assessment_id::varchar as source_record_id,
    sk_patient_id::varchar as sk_patient_id,
    'scored_assessment' as clinical_record_type,
    'fct_sus_uec_scored_assessment' as source_model_name,
    assessment_tool_code::varchar as source_code,
    'SNOMED CT' as source_coding_system,
    visit_occurrence_id::varchar as parent_record_id,
    'emergency_care_attendance' as parent_record_type,
    'obt_encounter_uec' as parent_model_name,
    'recorded_parent' as relationship_type,
    validated_at::timestamp_ntz as clinical_record_at,
    validated_at::date as clinical_record_date,
    iff(validated_at is null, 'unknown', 'timestamp') as clinical_time_precision,
    iff(validated_at is null, 'not_recorded', 'validated_at') as clinical_time_basis,
    person_score::varchar as result_value,
    null::varchar as result_value_name,
    person_score_numeric::number(38,9) as result_value_numeric,
    value_parse_status as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    assessment_description as assessment_tool_name,
    iff(value_parse_status = 'not_recorded', 'value_missing', 'not_validated') as assessment_response_status,
    organisation_id::varchar as provider_organisation_code,
    organisation_name::varchar as provider_organisation_name,
    'ODS' as provider_code_authority
from {{ ref('fct_sus_uec_scored_assessment') }}
), published as (
    select * from {{ ref('fct_person_clinical_record') }}
    where source_dataset = 'ECDS'
        and source_record_type in ('observation', 'scored_assessment')
)
select
    coalesce(e.source_record_type, r.source_record_type) as source_record_type,
    count(e.source_record_id) as expected_rows,
    count(r.source_record_id) as published_rows,
    count_if(e.source_record_id is not null and r.source_record_id is null) as missing_rows,
    count_if(e.source_record_id is null and r.source_record_id is not null) as unexpected_rows,
    count_if(e.source_record_id is not null and r.source_record_id is not null and (
        e.sk_patient_id is distinct from r.sk_patient_id
        or e.clinical_record_type is distinct from r.clinical_record_type
        or e.source_model_name is distinct from r.source_model_name
        or e.source_code is distinct from r.source_code
        or e.source_coding_system is distinct from r.source_coding_system
        or e.parent_record_id is distinct from r.parent_record_id
        or e.parent_record_type is distinct from r.parent_record_type
        or e.parent_model_name is distinct from r.parent_model_name
        or e.relationship_type is distinct from r.relationship_type
        or e.clinical_record_at is distinct from r.clinical_record_at
        or e.clinical_record_date is distinct from r.clinical_record_date
        or e.clinical_time_precision is distinct from r.clinical_time_precision
        or e.clinical_time_basis is distinct from r.clinical_time_basis
        or e.result_value is distinct from r.result_value
        or e.result_value_name is distinct from r.result_value_name
        or e.result_value_numeric is distinct from r.result_value_numeric
        or e.result_value_parse_status is distinct from r.result_value_parse_status
        or e.result_unit_code is distinct from r.result_unit_code
        or e.result_unit_name is distinct from r.result_unit_name
        or e.result_unit_symbol is distinct from r.result_unit_symbol
        or e.assessment_tool_name is distinct from r.assessment_tool_name
        or e.assessment_response_status is distinct from r.assessment_response_status
        or e.provider_organisation_code is distinct from r.provider_organisation_code
        or e.provider_organisation_name is distinct from r.provider_organisation_name
        or e.provider_code_authority is distinct from r.provider_code_authority
        or r.assessment_score_numeric is not null
        or r.is_assessment_response_non_score is not null
    )) as changed_payload_rows,
    count_if(e.source_record_id is not null and e.sk_patient_id is null) as expected_missing_patient_rows,
    count_if(r.source_record_id is not null and r.sk_patient_id is null) as published_missing_patient_rows
from expected as e
full outer join published as r
    on e.source_record_type = r.source_record_type and e.source_record_id = r.source_record_id
group by coalesce(e.source_record_type, r.source_record_type);

-- Compare these aggregates before and after an incremental replay.
select
    source_record_type,
    count(*) as record_count,
    hash_agg(*) as content_fingerprint
from {{ ref('int_ecds_person_clinical_record') }}
group by source_record_type
order by source_record_type
