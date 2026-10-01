-- Aggregate-only profile. Do not add person, provider or record identifiers.
with spell as (
    select * from {{ ref('fct_mhsds_hospital_provider_spell') }}
)

, ward_stay as (
    select * from {{ ref('fct_mhsds_ward_stay') }}
)

, ward_details as (
    select * from {{ ref('stg_mhsds_mhs903warddetails') }}
)

, code_lookup_history as (
    select * from {{ ref('mhsds_inpatient_code_lookup_history') }}
)

, code_lookup as (
    select * from {{ ref('mhsds_inpatient_code_lookup') }}
)

, summary as (
    select
        'population' as profile_section
        , 'hospital_provider_spell' as model_name
        , 'row_count' as metric
        , null::varchar as dimension_value
        , count(*)::number as numerator
        , count(*)::number as denominator
    from spell

    union all

    select 'population', 'hospital_provider_spell', 'no_recorded_discharge', null
        , count_if(not has_recorded_discharge), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell', 'invalid_recorded_date_order', null
        , count_if(has_invalid_recorded_date_order), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell', 'invalid_recorded_time_order', null
        , count_if(has_invalid_recorded_time_order), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell'
        , 'decision_to_admit_after_admission_date', null
        , count_if(has_decision_to_admit_after_admission), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell'
        , 'decision_to_admit_after_admission_time', null
        , count_if(has_decision_to_admit_after_admission_time)
        , count_if(decided_to_admit_time_precision = 'timestamp'
            and admission_time_precision = 'timestamp')
    from spell

    union all

    select 'quality', 'hospital_provider_spell', 'decision_time_without_date', null
        , count_if(decided_to_admit_time is not null
            and decided_to_admit_date is null), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell', 'discharge_time_without_date', null
        , count_if(discharge_time is not null and discharge_date is null), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell', 'same_day_midnight_discharge', null
        , count_if(admission_date = discharge_date
            and admission_time > '00:00:00'::time
            and discharge_time = '00:00:00'::time), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell'
        , 'estimated_discharge_before_admission', null
        , count_if(has_estimated_discharge_before_admission), count(*)
    from spell

    union all

    select 'quality', 'hospital_provider_spell'
        , 'planned_discharge_before_admission', null
        , count_if(has_planned_discharge_before_admission), count(*)
    from spell

    union all

    select 'relationship', 'hospital_provider_spell', 'referral_unmatched', null
        , count_if(not has_matching_referral), count(*)
    from spell

    union all

    select 'population', 'ward_stay', 'row_count', null
        , count(*), count(*)
    from ward_stay

    union all

    select 'population', 'ward_stay', 'no_recorded_end', null
        , count_if(not has_recorded_end), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'invalid_recorded_date_order', null
        , count_if(has_invalid_recorded_date_order), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'invalid_recorded_time_order', null
        , count_if(has_invalid_recorded_time_order), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'end_time_without_date', null
        , count_if(ward_stay_end_time is not null
            and ward_stay_end_date is null), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'same_day_midnight_end', null
        , count_if(ward_stay_start_date = ward_stay_end_date
            and ward_stay_start_time > '00:00:00'::time
            and ward_stay_end_time = '00:00:00'::time), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'trial_leave_end_before_ward_start', null
        , count_if(has_trial_leave_end_before_ward_start), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'trial_leave_end_after_ward_end', null
        , count_if(has_trial_leave_end_after_ward_end), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'trial_leave_end_year_2100_or_later', null
        , count_if(trial_leave_end_date >= '2100-01-01'::date), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'outside_recorded_spell_dates', null
        , count_if(is_outside_recorded_spell_dates)
        , count_if(is_outside_recorded_spell_dates is not null)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'recorded_spell_date_check_unavailable', null
        , count_if(is_outside_recorded_spell_dates is null), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'ward_start_before_spell_admission', null
        , count_if(has_ward_start_before_spell_admission), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'ward_end_after_spell_discharge', null
        , count_if(has_ward_end_after_spell_discharge), count(*)
    from ward_stay

    union all

    select 'quality', 'ward_stay', 'open_ward_stay_in_discharged_spell', null
        , count_if(has_open_ward_stay_in_discharged_spell), count(*)
    from ward_stay

    union all

    select 'relationship', 'ward_stay', 'parent_spell_unmatched', null
        , count_if(not has_matching_hospital_provider_spell), count(*)
    from ward_stay

    union all

    select 'relationship', 'ward_stay', 'referral_unmatched', null
        , count_if(not has_matching_referral), count(*)
    from ward_stay

    union all

    select 'relationship', 'ward_stay', 'site_code_unavailable', null
        , count_if(ward_site_code is null), count(*)
    from ward_stay

    union all

    select 'relationship', 'ward_stay', 'site_name_unmatched', null
        , count_if(ward_site_code is not null and ward_site_name is null)
        , count_if(ward_site_code is not null)
    from ward_stay
)

, label_coverage as (
    select 'hospital_provider_spell' as model_name, 'admission_source' as metric
        , count_if(admission_source_description is not null) as numerator
        , count_if(admission_source_code is not null) as denominator
    from spell
    union all
    select 'hospital_provider_spell', 'admission_method'
        , count_if(admission_method_description is not null)
        , count_if(admission_method_code is not null)
    from spell
    union all
    select 'hospital_provider_spell', 'planned_discharge_destination'
        , count_if(planned_discharge_destination_description is not null)
        , count_if(planned_discharge_destination_code is not null)
    from spell
    union all
    select 'hospital_provider_spell', 'discharge_method'
        , count_if(discharge_method_description is not null)
        , count_if(discharge_method_code is not null)
    from spell
    union all
    select 'hospital_provider_spell', 'discharge_destination'
        , count_if(discharge_destination_description is not null)
        , count_if(discharge_destination_code is not null)
    from spell
    union all
    select 'ward_stay', 'admitted_patient_classification'
        , count_if(admitted_patient_classification_description is not null)
        , count_if(admitted_patient_classification_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'ward_setting'
        , count_if(ward_setting_description is not null)
        , count_if(ward_setting_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'ward_intended_age_group'
        , count_if(ward_intended_age_group_description is not null)
        , count_if(ward_intended_age_group_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'ward_intended_sex'
        , count_if(ward_intended_sex_description is not null)
        , count_if(ward_intended_sex_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'ward_clinical_care_intensity'
        , count_if(ward_clinical_care_intensity_description is not null)
        , count_if(ward_clinical_care_intensity_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'ward_security_level'
        , count_if(ward_security_level_description is not null)
        , count_if(ward_security_level_code is not null)
    from ward_stay
    union all
    select 'ward_stay', 'locked_ward_indicator'
        , count_if(locked_ward_indicator_description is not null)
        , count_if(locked_ward_indicator_code is not null)
    from ward_stay
)

, ward_context as (
    select
        'relationship' as profile_section
        , 'ward_stay' as model_name
        , 'reported_ward_context_link_status' as metric
        , reported_ward_context_link_status as dimension_value
        , count(*) as numerator
        , sum(count(*)) over () as denominator
    from ward_stay
    group by reported_ward_context_link_status
)

, ward_sources as (
    select
        'relationship' as profile_section
        , 'ward_stay' as model_name
        , 'ward_site_source' as metric
        , ward_site_source as dimension_value
        , count(*) as numerator
        , sum(count(*)) over () as denominator
    from ward_stay
    group by ward_site_source

    union all

    select
        'relationship'
        , 'ward_stay'
        , 'ward_attribute_source'
        , ward_attribute_source
        , count(*)
        , sum(count(*)) over ()
    from ward_stay
    group by ward_attribute_source
)

, ward_details_provider_submission as (
    select
        org_id_prov
        , uniq_submission_id
        , count(*) as context_rows
    from ward_details
    group by org_id_prov, uniq_submission_id
)

, ward_details_provider_ward as (
    select
        org_id_prov
        , ward_code
        , count(*) as context_rows
    from ward_details
    group by org_id_prov, ward_code
)

, unmatched_ward_context as (
    select
        coalesce(provider_submission.context_rows, 0)
            as provider_submission_context_rows
        , coalesce(provider_ward.context_rows, 0)
            as provider_ward_context_rows
    from ward_stay
    left join ward_details_provider_submission as provider_submission
        on ward_stay.provider_organisation_code = provider_submission.org_id_prov
        and ward_stay.uniq_submission_id = provider_submission.uniq_submission_id
    left join ward_details_provider_ward as provider_ward
        on ward_stay.provider_organisation_code = provider_ward.org_id_prov
        and ward_stay.ward_code = provider_ward.ward_code
    where ward_stay.reported_ward_context_link_status = 'not_matched'
)

, ward_context_gaps as (
    select
        'relationship' as profile_section
        , 'ward_stay' as model_name
        , 'mhs903_no_provider_submission_context' as metric
        , null::varchar as dimension_value
        , count_if(provider_submission_context_rows = 0) as numerator
        , count(*) as denominator
    from unmatched_ward_context

    union all

    select 'relationship', 'ward_stay', 'mhs903_ward_never_seen_for_provider'
        , null
        , count_if(provider_submission_context_rows > 0
            and provider_ward_context_rows = 0)
        , count(*)
    from unmatched_ward_context

    union all

    select 'relationship', 'ward_stay', 'mhs903_ward_missing_from_submission'
        , null
        , count_if(provider_submission_context_rows > 0
            and provider_ward_context_rows > 0)
        , count(*)
    from unmatched_ward_context
)

, latest_code_history_keys as (
    select
        code_set_name
        , code
        , boolor_agg(not is_currently_valid) as has_retired_latest_definition
    from code_lookup_history
    where is_latest_definition
    group by code_set_name, code
)

, code_lookup_coverage as (
    select
        'lookup_integrity' as profile_section
        , 'mhsds_inpatient_code_lookup' as model_name
        , 'latest_history_code_coverage' as metric
        , null::varchar as dimension_value
        , count_if(code_lookup.code is not null) as numerator
        , count(*) as denominator
    from latest_code_history_keys
    left join code_lookup
        on latest_code_history_keys.code_set_name = code_lookup.code_set_name
        and latest_code_history_keys.code = code_lookup.code

    union all

    select 'lookup_integrity', 'mhsds_inpatient_code_lookup'
        , 'codes_with_retired_latest_definition', null
        , count_if(has_retired_latest_definition), count(*)
    from latest_code_history_keys
)

, version_population as (
    select
        'version_population' as profile_section
        , 'hospital_provider_spell' as model_name
        , 'row_count' as metric
        , mhsds_specification_version::varchar as dimension_value
        , count(*) as numerator
        , sum(count(*)) over () as denominator
    from spell
    group by mhsds_specification_version

    union all

    select 'version_population', 'ward_stay', 'row_count'
        , mhsds_specification_version::varchar
        , count(*), sum(count(*)) over ()
    from ward_stay
    group by mhsds_specification_version
)

, version_6_label_gaps as (
    select 'hospital_provider_spell' as model_name, 'admission_source' as metric
        , count_if(admission_source_code is not null
            and admission_source_description is null) as numerator
        , count_if(admission_source_code is not null) as denominator
    from spell where mhsds_specification_version >= 6
    union all
    select 'hospital_provider_spell', 'admission_method'
        , count_if(admission_method_code is not null
            and admission_method_description is null)
        , count_if(admission_method_code is not null)
    from spell where mhsds_specification_version >= 6
    union all
    select 'hospital_provider_spell', 'discharge_method'
        , count_if(discharge_method_code is not null
            and discharge_method_description is null)
        , count_if(discharge_method_code is not null)
    from spell where mhsds_specification_version >= 6
    union all
    select 'hospital_provider_spell', 'planned_discharge_destination'
        , count_if(planned_discharge_destination_code is not null
            and planned_discharge_destination_description is null)
        , count_if(planned_discharge_destination_code is not null)
    from spell where mhsds_specification_version >= 6
    union all
    select 'hospital_provider_spell', 'discharge_destination'
        , count_if(discharge_destination_code is not null
            and discharge_destination_description is null)
        , count_if(discharge_destination_code is not null)
    from spell where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'admitted_patient_classification'
        , count_if(admitted_patient_classification_code is not null
            and admitted_patient_classification_description is null)
        , count_if(admitted_patient_classification_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'ward_setting'
        , count_if(ward_setting_code is not null
            and ward_setting_description is null)
        , count_if(ward_setting_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'ward_intended_age_group'
        , count_if(ward_intended_age_group_code is not null
            and ward_intended_age_group_description is null)
        , count_if(ward_intended_age_group_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'ward_intended_sex'
        , count_if(ward_intended_sex_code is not null
            and ward_intended_sex_description is null)
        , count_if(ward_intended_sex_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'ward_clinical_care_intensity'
        , count_if(ward_clinical_care_intensity_code is not null
            and ward_clinical_care_intensity_description is null)
        , count_if(ward_clinical_care_intensity_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'ward_security_level'
        , count_if(ward_security_level_code is not null
            and ward_security_level_description is null)
        , count_if(ward_security_level_code is not null)
    from ward_stay where mhsds_specification_version >= 6
    union all
    select 'ward_stay', 'locked_ward_indicator'
        , count_if(locked_ward_indicator_code is not null
            and locked_ward_indicator_description is null)
        , count_if(locked_ward_indicator_code is not null)
    from ward_stay where mhsds_specification_version >= 6
)

, capacity as (
    select 'ward_capacity' as profile_section, 'mhs903_ward_details' as model_name
        , 'available_bed_days_populated' as metric, null::varchar as dimension_value
        , count_if(avail_bed_days is not null) as numerator, count(*) as denominator
    from ward_details
    union all
    select 'ward_capacity', 'mhs903_ward_details', 'closed_bed_days_populated', null
        , count_if(closed_bed_days is not null), count(*)
    from ward_details
)

, combined as (
    select * from summary
    union all
    select 'label_coverage', model_name, metric, null, numerator, denominator
    from label_coverage
    union all
    select * from ward_context
    union all
    select * from ward_sources
    union all
    select * from ward_context_gaps
    union all
    select * from code_lookup_coverage
    union all
    select * from version_population
    union all
    select 'v6_label_gap', model_name, metric, null, numerator, denominator
    from version_6_label_gaps
    union all
    select * from capacity
)

select
    profile_section
    , model_name
    , metric
    , dimension_value
    , numerator
    , denominator
    , round(100 * numerator / nullif(denominator, 0), 3) as rate_pct
from combined
order by profile_section, model_name, metric, dimension_value
