{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(clinical_record_at, clinical_record_date::timestamp_ntz)'], tags=['person_clinical_record', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('clinical_record', 'fct_csds_clinical_record', 'source_record_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

select
    {{ dbt_utils.generate_surrogate_key(["'CSDS'", 's.source_record_id']) }} as clinical_record_id,
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'clinical_record'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    s.clinical_record_type::varchar as clinical_record_type,
    s.clinical_date::date as clinical_record_date,
    iff(s.clinical_time_precision = 'timestamp', s.clinical_at, null)::timestamp_ntz as clinical_record_at,
    iff(s.clinical_date is null, 'unknown', coalesce(s.clinical_time_precision, 'unknown'))::varchar as clinical_time_precision,
    s.clinical_time_basis::varchar as clinical_time_basis,
    s.clinical_code::varchar as source_code,
    s.clinical_description::varchar as source_code_name,
    s.clinical_value::varchar as result_value,
    s.clinical_value_description::varchar as result_value_name,
    s.clinical_value_numeric::number(38,9) as result_value_numeric,
    s.clinical_value_parse_status::varchar as result_value_parse_status,
    s.unit_of_measurement_code::varchar as result_unit_code,
    s.unit_of_measurement_description::varchar as result_unit_name,
    s.unit_of_measurement_symbol::varchar as result_unit_symbol,
    s.provider_organisation_code::varchar as provider_organisation_code,
    s.provider_organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.reporting_period_end_date::date as source_submission_period,
    s.source_file_received_at::timestamp_ntz as source_received_at,
    'CSDS'::varchar as source_dataset,
    'fct_csds_clinical_record'::varchar as source_model_name,
    s.clinical_code_system::varchar as source_coding_system,
    coalesce(source_snomed.snomed_code, mapped_snomed.snomed_code) as mapped_code,
    case
        when source_snomed.snomed_code is not null then source_snomed.preferred_term
        else mapped_snomed.preferred_term
    end::varchar as mapped_code_name,
    iff(mapped_code is not null, 'SNOMED CT', null)::varchar as mapped_coding_system,
    case
        when s.is_care_activity_linked and s.is_care_activity_person_consistent is distinct from false then s.care_activity_source_record_id::varchar
        else s.referral_id::varchar
    end as parent_record_id,
    case
        when s.is_care_activity_linked and s.is_care_activity_person_consistent is distinct from false then 'care_activity'
        when s.referral_id is not null then 'referral'
    end as parent_record_type,
    case parent_record_type
        when 'care_activity' then 'fct_csds_care_activity'
        when 'referral' then 'fct_csds_referral'
    end as parent_model_name,
    iff(parent_record_id is not null, 'recorded_parent', null)::varchar as relationship_type
from {{ ref('fct_csds_clinical_record') }} as s
left join {{ ref('snomed_concept') }} as source_snomed
    on trim(s.clinical_code) = source_snomed.snomed_code
    and s.clinical_code_system = 'SNOMED CT'
left join {{ ref('snomed_concept') }} as mapped_snomed
    on s.dictionary_snomed_code::varchar = mapped_snomed.snomed_code
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'clinical_record') }}
