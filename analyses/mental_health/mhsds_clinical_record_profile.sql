select
    clinical_record_type
    , count(*) as row_count
    , count(distinct clinical_record_id) as distinct_clinical_records
    , count(distinct source_record_id) as distinct_drill_down_keys
    , count(distinct originating_source_record_id) as distinct_originating_source_keys
    , count_if(source_record_id is distinct from clinical_record_id) as drill_down_key_mismatches
    , count_if(sk_patient_id is null) as missing_patient_keys
    , count_if(clinical_code is null) as missing_codes
    , count_if(clinical_description is null) as missing_descriptions
    , count_if(coding_scheme_description is null) as missing_scheme_labels
    , count_if(provider_organisation_name is null) as missing_provider_labels
    , count_if(unit_of_measurement_code is not null) as recorded_units
    , count_if(unit_of_measurement_code is not null and unit_of_measurement_description is null)
        as unmatched_unit_labels
    , count_if(trim(clinical_code) = '') as blank_codes
    , count_if(trim(coding_scheme_code) = '') as blank_schemes
    , count_if(source_table = 'MHS601' and local_patient_id is null) as missing_local_patient_links
    , count_if(coding_scheme_kind = 'fixed_snomed' and coding_scheme_code is not null)
        as fixed_schemes_presented_as_submitted
    , count_if(clinical_label_status = 'mapped_to_snomed') as mapped_labels
    , count_if(clinical_value_parse_status = 'not_numeric_or_out_of_range') as unparsed_values
    , count_if(clinical_value_parse_status = 'numeric_rounded') as rounded_values
    , count_if(clinical_value_numeric <> trunc(clinical_value_numeric)) as fractional_values
    , count_if(clinical_date is null) as missing_clinical_dates
    , count_if(is_source_date_inconsistent) as inconsistent_source_dates
    , count_if(has_source_date_sentinel) as source_date_sentinels
    , count_if(is_clinical_date_after_reporting_period) as dates_after_period
    , count_if(is_assessment_before_reporting_period) as assessments_before_period
    , count_if(has_person_identifier_changed) as changed_person_identifiers
    , count_if(not is_care_activity_linked) as missing_activity_parents
    , count_if(not is_care_activity_person_consistent) as activity_person_mismatches
    , count_if(not is_care_activity_referral_consistent) as activity_referral_mismatches
    , count_if(not is_care_activity_contact_consistent) as activity_contact_mismatches
    , sum(accepted_source_record_count) as represented_source_rows
from {{ ref('fct_mhsds_clinical_record') }}
group by clinical_record_type
