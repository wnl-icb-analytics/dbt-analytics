{{ config(
    cluster_by=['sk_patient_id', 'clinical_record_at'],
    tags=['person_clinical_record']
) }}

-- One row per IAPT clinical item, selected from fct_iapt_clinical_record, the only inclusion union.
-- source_record_id drills down to the typed fact named in source_model_name; one care activity
-- can supply a procedure, finding and observation, told apart by source_record_type.
select
    {{ dbt_utils.generate_surrogate_key(["'IAPT'", 'c.source_record_id']) }} as clinical_record_id,
    c.sk_patient_id::varchar as sk_patient_id,
    c.person_id::varchar as source_person_id,
    case c.source_model_name
        when 'fct_iapt_assessment_score' then 'assessment_score'
        when 'fct_iapt_health_condition' then 'health_condition'
        else 'care_activity_' || c.clinical_record_type
    end::varchar as source_record_type,
    c.originating_source_record_id::varchar as source_record_id,
    c.clinical_record_type::varchar as clinical_record_type,
    c.clinical_date::date as clinical_record_date,
    -- Midnight on date-only rows is a sort anchor from the source fact, not an observed time.
    c.clinical_at::timestamp_ntz as clinical_record_at,
    iff(c.clinical_date is null, 'unknown', coalesce(c.clinical_time_precision, 'unknown'))::varchar
        as clinical_time_precision,
    coalesce(c.clinical_time_basis, 'not_recorded')::varchar as clinical_time_basis,
    c.is_source_date_inconsistent,
    c.clinical_code::varchar as source_code,
    c.clinical_description::varchar as source_code_name,
    c.clinical_value::varchar as result_value,
    c.clinical_value_description::varchar as result_value_name,
    c.clinical_value_numeric::number(38,9) as result_value_numeric,
    c.clinical_value_parse_status::varchar as result_value_parse_status,
    c.assessment_tool_name::varchar as assessment_tool_name,
    c.assessment_score_numeric::number(38,9) as assessment_score_numeric,
    c.assessment_response_status::varchar as assessment_response_status,
    c.is_assessment_response_non_score,
    c.unit_of_measurement_code::varchar as result_unit_code,
    c.unit_of_measurement_description::varchar as result_unit_name,
    c.unit_of_measurement_symbol::varchar as result_unit_symbol,
    c.provider_organisation_code::varchar as provider_organisation_code,
    c.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    c.reporting_period_end_date::date as source_submission_period,
    -- Warehouse load time, so a late-loaded submission is not missed by a later receipt watermark.
    c.source_loaded_at::timestamp_ntz as source_received_at,
    'IAPT'::varchar as source_dataset,
    c.source_model_name::varchar as source_model_name,
    c.coding_system::varchar as source_coding_system,
    -- Only an atomic code the fact supports and the NHS Digital reference holds; ICD-10 is never reverse-mapped.
    snomed.snomed_code::varchar as mapped_code,
    snomed.preferred_term::varchar as mapped_code_name,
    iff(snomed.snomed_code is not null, 'SNOMED CT', null)::varchar as mapped_coding_system,
    c.clinical_qualifier_code::varchar as qualifier_code,
    c.clinical_qualifier_description::varchar as qualifier_name,
    c.parent_record_id::varchar as parent_record_id,
    c.parent_record_type::varchar as parent_record_type,
    c.parent_model_name::varchar as parent_model_name,
    iff(c.parent_record_id is not null, 'recorded_parent', null)::varchar as relationship_type
from {{ ref('fct_iapt_clinical_record') }} as c
left join {{ ref('snomed_concept') }} as snomed
    on trim(c.snomed_code) = snomed.snomed_code
