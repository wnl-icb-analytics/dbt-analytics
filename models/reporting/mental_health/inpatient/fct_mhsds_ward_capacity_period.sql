with stay_totals as (
    select
        uniq_submission_id
        , uniq_ward_code
        , count(*) as n_ward_stay_records
        , count_if(has_invalid_stay_dates) as n_stays_with_invalid_dates
        , count_if(has_overlapping_person_stay_in_submission) as n_stays_with_overlap
        , sum(recorded_ward_stay_midnights) as recorded_ward_stay_midnights
    from {{ ref('int_mhsds_ward_stay_period') }}
    group by uniq_submission_id, uniq_ward_code
)
select
    {{ dbt_utils.generate_surrogate_key(['w.uniq_submission_id','w.uniq_ward_code']) }} as ward_capacity_period_id
    , w.uniq_ward_code as ward_id
    , w.ward_code as ward_local_id
    , w.site_id_of_ward as ward_site_code
    , site.organisation_name as ward_site_name
    , w.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , w.reporting_period_start_date::date as reporting_period_start_date
    , w.reporting_period_end_date::date as reporting_period_end_date
    , datediff(day, reporting_period_start_date, reporting_period_end_date) + 1 as days_in_reporting_period
    , w.avail_bed_days as available_bed_days
    , w.closed_bed_days as temporarily_closed_bed_days
    , w.avail_bed_days is not null as has_reported_available_capacity
    , w.avail_bed_days / nullif(days_in_reporting_period, 0) as average_available_beds
    , w.closed_bed_days / nullif(days_in_reporting_period, 0) as average_temporarily_closed_beds
    , w.avail_bed_days + w.closed_bed_days as reported_bed_base_days
    , reported_bed_base_days / nullif(days_in_reporting_period, 0) as average_reported_bed_base
    , coalesce(s.n_ward_stay_records, 0) as n_ward_stay_records
    , s.n_ward_stay_records is not null as has_matching_ward_stay_evidence
    , coalesce(s.n_stays_with_invalid_dates, 0) as n_stays_with_invalid_dates
    , coalesce(s.n_stays_with_overlap, 0) as n_stays_with_overlap
    , coalesce(s.recorded_ward_stay_midnights, 0) as recorded_ward_stay_midnights
    , iff(w.avail_bed_days > 0 and s.n_ward_stay_records > 0
        and s.n_stays_with_invalid_dates = 0 and s.n_stays_with_overlap = 0,
        s.recorded_ward_stay_midnights / w.avail_bed_days, null)
        as recorded_stay_midnights_per_available_bed_day
    , w.ward_type as ward_setting_code
    , setting.description as ward_setting_description
    , w.ward_age as ward_intended_age_group_code
    , age.description as ward_intended_age_group_description
    , w.ward_intended_sex as ward_intended_sex_code
    , sex.description as ward_intended_sex_description
    , w.ward_sec_level as ward_security_level_code
    , security.description as ward_security_level_description
    , w.ward_intended_clin_care_mh as ward_clinical_care_intensity_code
    , intensity.description as ward_clinical_care_intensity_description
    , case when w.locked_ward_ind then 'Y' when not w.locked_ward_ind then 'N' end as locked_ward_indicator_code
    , locked_ward.description as locked_ward_indicator_description
    , w.uniq_submission_id as submission_id
    , w.mhs903_uniq_id as source_row_id
from {{ ref('stg_mhsds_mhs903warddetails') }} as w
left join stay_totals as s on w.uniq_submission_id = s.uniq_submission_id and w.uniq_ward_code = s.uniq_ward_code
left join {{ ref('int_mhsds_organisation') }} as provider on upper(w.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as site on upper(w.site_id_of_ward) = upper(site.organisation_code)
left join {{ ref('mhsds_inpatient_code_lookup') }} as setting
    on w.ward_type = setting.code and setting.code_set_name = 'ward_setting'
left join {{ ref('mhsds_inpatient_code_lookup') }} as age
    on w.ward_age = age.code and age.code_set_name = 'ward_intended_age_group'
left join {{ ref('mhsds_inpatient_code_lookup') }} as sex
    on w.ward_intended_sex = sex.code and sex.code_set_name = 'ward_intended_sex'
left join {{ ref('mhsds_inpatient_code_lookup') }} as security
    on w.ward_sec_level = security.code and security.code_set_name = 'ward_security_level'
left join {{ ref('mhsds_inpatient_code_lookup') }} as intensity
    on w.ward_intended_clin_care_mh = intensity.code and intensity.code_set_name = 'ward_clinical_care_intensity'
left join {{ ref('mhsds_inpatient_code_lookup') }} as locked_ward
    on locked_ward_indicator_code = locked_ward.code and locked_ward.code_set_name = 'locked_ward_indicator'
