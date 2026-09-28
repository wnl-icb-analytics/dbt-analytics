with ward_stay_versions as (
    select
        w.*
        , try_to_decimal(w.dat_set_ver::varchar, 4, 1)
            as mhsds_specification_version
    from {{ ref('stg_mhsds_mhs502wardstay') }} as w
)

, ward_stays as (
    select
        w.*
        , case
            when w.mhsds_specification_version >= 6
                then ward_details.site_id_of_ward
            when w.mhsds_specification_version < 6 then w.site_id_of_treat
        end as ward_site_code
        , case
            when w.mhsds_specification_version is null
                then 'unknown_specification_version'
            when w.mhsds_specification_version >= 6
                and ward_details.mhs903_uniq_id is not null
                then 'mhs903_retained_submission'
            when w.mhsds_specification_version >= 6 then 'unavailable'
            when w.site_id_of_treat is not null then 'mhs502_ward_stay'
            else 'unavailable'
        end as ward_site_source
        , case
            when w.mhsds_specification_version is null
                then 'unknown_specification_version'
            when w.mhsds_specification_version < 6 then 'not_applicable'
            when ward_details.mhs903_uniq_id is not null then 'matched'
            else 'not_matched'
        end as reported_ward_context_link_status
        , case
            when w.mhsds_specification_version is null
                then 'unknown_specification_version'
            when w.mhsds_specification_version >= 6
                and ward_details.mhs903_uniq_id is not null
                then 'mhs903_retained_submission'
            when w.mhsds_specification_version >= 6 then 'unavailable'
            else 'mhs502_ward_stay'
        end as ward_attribute_source
        , case
            when w.mhsds_specification_version >= 6 then ward_details.ward_type
            when w.mhsds_specification_version < 6 then w.ward_type
        end as ward_setting_code
        , case
            when w.mhsds_specification_version >= 6 then ward_details.ward_age
            when w.mhsds_specification_version < 6 then w.ward_age
        end as ward_intended_age_group_code
        , case
            when w.mhsds_specification_version >= 6
                then ward_details.ward_intended_sex
            when w.mhsds_specification_version < 6 then w.ward_sex_type_code
        end as ward_intended_sex_code
        , case
            when w.mhsds_specification_version >= 6
                then ward_details.ward_intended_clin_care_mh
            when w.mhsds_specification_version < 6
                then w.intend_clin_care_inten_code_mh
        end as ward_clinical_care_intensity_code
        , case
            when w.mhsds_specification_version >= 6
                then ward_details.ward_sec_level
            when w.mhsds_specification_version < 6 then w.ward_sec_level
        end as ward_security_level_code
        , case lower(trim(
            case
                when w.mhsds_specification_version >= 6
                    then ward_details.locked_ward_ind::varchar
                when w.mhsds_specification_version < 6
                    then w.locked_ward_ind::varchar
            end
        ))
            when 'true' then 'Y'
            when 'false' then 'N'
            when '1' then 'Y'
            when '0' then 'N'
            when 'y' then 'Y'
            when 'n' then 'N'
            else upper(trim(
                case
                    when w.mhsds_specification_version >= 6
                        then ward_details.locked_ward_ind::varchar
                    when w.mhsds_specification_version < 6
                        then w.locked_ward_ind::varchar
                end
            ))
        end as locked_ward_indicator_code
    from ward_stay_versions as w
    left join {{ ref('stg_mhsds_mhs903warddetails') }} as ward_details
        on w.uniq_submission_id = ward_details.uniq_submission_id
        and w.uniq_ward_code = ward_details.uniq_ward_code
)

select
    w.uniq_ward_stay_id as source_record_id
    , 'MHSDS' as source_dataset
    , w.uniq_ward_stay_id
    , w.mhs502_uniq_id
    , w.ward_stay_id
    , w.uniq_hosp_prov_spell_id as recorded_hospital_provider_spell_id
    , spell.source_record_id as hospital_provider_spell_source_record_id
    , spell.source_record_id is not null as has_matching_hospital_provider_spell
    , w.uniq_serv_req_id as referral_source_record_id
    , referral.source_record_id is not null as has_matching_referral
    , w.person_id
    , bridge.sk_patient_id
    , w.start_date_ward_stay as ward_stay_start_date
    , w.start_time_ward_stay as ward_stay_start_time
    , case
        when w.start_date_ward_stay is null then null
        when w.start_time_ward_stay is null
            then w.start_date_ward_stay::timestamp_ntz
        else timestamp_ntz_from_parts(
            w.start_date_ward_stay, w.start_time_ward_stay
        )
    end as ward_stay_started_at
    , case
        when w.start_date_ward_stay is null then null
        when w.start_time_ward_stay is null then 'date'
        else 'timestamp'
    end as ward_stay_start_time_precision
    , w.end_date_ward_stay as ward_stay_end_date
    , w.end_time_ward_stay as ward_stay_end_time
    , case
        when w.end_date_ward_stay is null then null
        when w.end_time_ward_stay is null
            then w.end_date_ward_stay::timestamp_ntz
        else timestamp_ntz_from_parts(
            w.end_date_ward_stay, w.end_time_ward_stay
        )
    end as ward_stay_ended_at
    , case
        when w.end_date_ward_stay is null then null
        when w.end_time_ward_stay is null then 'date'
        else 'timestamp'
    end as ward_stay_end_time_precision
    , w.end_date_mh_trial_leave as trial_leave_end_date
    , coalesce(
        w.end_date_mh_trial_leave < w.start_date_ward_stay, false
    ) as has_trial_leave_end_before_ward_start
    , coalesce(
        w.end_date_mh_trial_leave > w.end_date_ward_stay, false
    ) as has_trial_leave_end_after_ward_end
    , w.end_date_ward_stay is not null as has_recorded_end
    , iff(
        w.end_date_ward_stay is null, 'no_recorded_end', 'ended'
    ) as source_ward_stay_status
    , datediff(
        day, w.start_date_ward_stay, w.end_date_ward_stay
    ) as recorded_length_of_stay_days
    , coalesce(
        w.end_date_ward_stay < w.start_date_ward_stay, false
    ) as has_invalid_recorded_date_order
    , coalesce(
        timestamp_ntz_from_parts(
            w.end_date_ward_stay, w.end_time_ward_stay
        ) < timestamp_ntz_from_parts(
            w.start_date_ward_stay, w.start_time_ward_stay
        )
        , false
    ) as has_invalid_recorded_time_order
    , coalesce(
        w.start_date_ward_stay < spell.admission_date, false
    ) as has_ward_start_before_spell_admission
    , coalesce(
        w.end_date_ward_stay > spell.discharge_date, false
    ) as has_ward_end_after_spell_discharge
    , w.end_date_ward_stay is null
        and spell.discharge_date is not null
        as has_open_ward_stay_in_discharged_spell
    , w.start_date_ward_stay < spell.admission_date
        or w.end_date_ward_stay > spell.discharge_date
        as is_outside_recorded_spell_dates
    , w.mh_admitted_patient_class
        as admitted_patient_classification_code
    , admitted_class.description as admitted_patient_classification_description
    , w.hospital_bed_type_mh as legacy_hospital_bed_type_code
    , w.hospital_bed_type_name as source_derived_hospital_bed_type_name
    , w.specialised_mh_service_code
        as specialised_mental_health_service_category_code
    , w.specialised_mh_service_name
        as source_derived_specialised_mental_health_service_category_name
    , w.ward_code
    , w.uniq_ward_code
    , w.ward_site_code
    , site.organisation_name as ward_site_name
    , w.ward_site_source
    , w.reported_ward_context_link_status
    , w.ward_attribute_source
    , w.ward_setting_code
    , ward_setting.description as ward_setting_description
    , w.ward_intended_age_group_code
    , ward_age.description as ward_intended_age_group_description
    , w.ward_intended_sex_code
    , ward_sex.description as ward_intended_sex_description
    , w.ward_clinical_care_intensity_code
    , care_intensity.description as ward_clinical_care_intensity_description
    , w.ward_security_level_code
    , security_level.description as ward_security_level_description
    , w.locked_ward_indicator_code
    , locked_ward.description as locked_ward_indicator_description
    , w.ward_loc_distance_home as distance_from_home_km
    , w.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , w.dmic_ccg_code as source_derived_icb_commissioner_code
    , commissioner.organisation_name as source_derived_icb_commissioner_name
    , coalesce(commissioner.is_wnl_commissioner, false) as is_wnl_commissioner
    , w.record_start_date
    , w.record_end_date
    , w.uniq_submission_id
    , w.uniq_month_id
    , w.reporting_period_start_date
    , w.reporting_period_end_date
    , w.mhsds_specification_version
    , w.dmic_dataset as source_pipeline_dataset
    , w.effective_from as source_file_received_at
    , w.dmic_date_added as source_loaded_at
from ward_stays as w
left join {{ ref('fct_mhsds_hospital_provider_spell') }} as spell
    on w.uniq_hosp_prov_spell_id = spell.source_record_id
left join {{ ref('fct_mhsds_referral') }} as referral
    on w.uniq_serv_req_id = referral.source_record_id
left join {{ ref('stg_mhsds_bridging') }} as bridge
    on w.person_id = bridge.person_id
left join {{ ref('mhsds_inpatient_code_lookup') }} as admitted_class
    on upper(trim(w.mh_admitted_patient_class)) = admitted_class.code
    and admitted_class.code_set_name
        = 'mental_health_admitted_patient_classification'
left join {{ ref('mhsds_inpatient_code_lookup') }} as ward_setting
    on upper(trim(w.ward_setting_code)) = ward_setting.code
    and ward_setting.code_set_name = 'ward_setting'
left join {{ ref('mhsds_inpatient_code_lookup') }} as ward_age
    on upper(trim(w.ward_intended_age_group_code)) = ward_age.code
    and ward_age.code_set_name = 'ward_intended_age_group'
left join {{ ref('mhsds_inpatient_code_lookup') }} as ward_sex
    on upper(trim(w.ward_intended_sex_code)) = ward_sex.code
    and ward_sex.code_set_name = 'ward_intended_sex'
left join {{ ref('mhsds_inpatient_code_lookup') }} as care_intensity
    on upper(trim(w.ward_clinical_care_intensity_code)) = care_intensity.code
    and care_intensity.code_set_name = 'ward_clinical_care_intensity'
left join {{ ref('mhsds_inpatient_code_lookup') }} as security_level
    on upper(trim(w.ward_security_level_code)) = security_level.code
    and security_level.code_set_name = 'ward_security_level'
left join {{ ref('mhsds_inpatient_code_lookup') }} as locked_ward
    on upper(trim(w.locked_ward_indicator_code)) = locked_ward.code
    and locked_ward.code_set_name = 'locked_ward_indicator'
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(w.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as site
    on upper(w.ward_site_code) = upper(site.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as commissioner
    on upper(w.dmic_ccg_code) = upper(commissioner.organisation_code)
