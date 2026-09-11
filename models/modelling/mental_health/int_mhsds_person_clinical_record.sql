{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(clinical_record_at, clinical_record_date::timestamp_ntz)'], tags=['person_clinical_record', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('clinical_record', 'fct_mhsds_clinical_record', 'source_record_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

select
    {{ dbt_utils.generate_surrogate_key(["'MHSDS'", 's.source_record_id']) }} as clinical_record_id,
    s.sk_patient_id::varchar as sk_patient_id,
    s.person_id::varchar as source_person_id,
    'clinical_record'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    s.clinical_record_type::varchar as clinical_record_type,
    s.clinical_date::date as clinical_record_date,
    iff(s.clinical_time_precision = 'timestamp', s.clinical_at, null)::timestamp_ntz as clinical_record_at,
    -- Stored diagnosis and assessment timestamps do not establish submitted precision.
    case
        when s.clinical_date is null then 'unknown'
        when s.clinical_time_precision = 'stored_timestamp_precision_unknown' then 'unknown'
        else coalesce(s.clinical_time_precision, 'unknown')
    end::varchar as clinical_time_precision,
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
    'MHSDS'::varchar as source_dataset,
    'fct_mhsds_clinical_record'::varchar as source_model_name,
    s.coding_scheme_description::varchar as source_coding_system,
    s.standardised_snomed_code::varchar as mapped_code,
    s.standardised_snomed_description::varchar as mapped_code_name,
    iff(s.standardised_snomed_code is not null, 'SNOMED CT', null)::varchar as mapped_coding_system,
    case
        when s.is_care_activity_linked and s.is_care_activity_person_consistent is distinct from false and s.is_care_activity_referral_consistent is distinct from false and s.is_care_activity_contact_consistent is distinct from false then s.care_activity_source_record_id::varchar
        else s.referral_source_record_id::varchar
    end as parent_record_id,
    case
        when s.is_care_activity_linked and s.is_care_activity_person_consistent is distinct from false and s.is_care_activity_referral_consistent is distinct from false and s.is_care_activity_contact_consistent is distinct from false then 'care_activity'
        when s.referral_source_record_id is not null then 'referral'
    end as parent_record_type,
    case parent_record_type
        when 'care_activity' then 'fct_mhsds_care_activity'
        when 'referral' then 'fct_mhsds_referral'
    end as parent_model_name,
    iff(parent_record_id is not null, 'recorded_parent', null)::varchar as relationship_type
from {{ ref('fct_mhsds_clinical_record') }} as s
where true
{{ navigation_delivery_filter('s.source_file_received_at', 'clinical_record') }}
