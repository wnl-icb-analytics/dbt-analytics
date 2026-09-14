{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

with latest_ward_stay as (
    {{
        select_latest_mhsds_record(
            mhsds_table = ref('raw_mhsds_mhs502wardstay'),
            partition_cols = ['uniq_ward_stay_id'],
            tie_breaker_cols = ['mhs502_uniq_id']
        )
    }}
)

select
    ward_stay.uniq_ward_stay_id
    , ward_stay.mhs502_uniq_id
    , ward_stay.ward_stay_id
    , ward_stay.ward_code
    , ward_stay.uniq_ward_code
    , ward_stay.uniq_hosp_prov_spell_id
    , ward_stay.uniq_hosp_prov_spell_num
    , ward_stay.hosp_prov_spell_id
    , ward_stay.hosp_prov_spell_num
    , ward_stay.uniq_serv_req_id
    , ward_stay.person_id
    , ward_stay.org_id_prov
    , iff(
        ward_stay.start_date_ward_stay::date < '1901-01-01'::date
        , null
        , ward_stay.start_date_ward_stay::date
    ) as start_date_ward_stay
    , ward_stay.start_time_ward_stay::time as start_time_ward_stay
    , iff(
        ward_stay.end_date_ward_stay::date < '1901-01-01'::date
        , null
        , ward_stay.end_date_ward_stay::date
    ) as end_date_ward_stay
    , ward_stay.end_time_ward_stay::time as end_time_ward_stay
    , iff(
        ward_stay.end_date_mh_trial_leave::date < '1901-01-01'::date
        , null
        , ward_stay.end_date_mh_trial_leave::date
    ) as end_date_mh_trial_leave
    , ward_stay.mh_admitted_patient_class
    , ward_stay.hospital_bed_type_mh
    , ward_stay.hospital_bed_type_name
    , ward_stay.specialised_mh_service_code
    , ward_stay.specialised_mh_service_name
    , ward_stay.site_id_of_treat
    , ward_stay.ward_type
    , ward_stay.ward_age
    , ward_stay.ward_intended_sex
    , ward_stay.ward_sex_type_code
    , ward_stay.ward_intended_clin_care_mh
    , ward_stay.intend_clin_care_inten_code_mh
    , ward_stay.ward_sec_level
    , ward_stay.locked_ward_ind
    , ward_stay.ward_loc_distance_home::number(18, 3)
        as ward_loc_distance_home
    , ward_stay.dmic_ccg_code
    , ward_stay.record_start_date::date as record_start_date
    , ward_stay.record_end_date::date as record_end_date
    , ward_stay.uniq_submission_id
    , ward_stay.uniq_month_id
    , ward_stay.reporting_period_start_date::date as reporting_period_start_date
    , ward_stay.reporting_period_end_date::date as reporting_period_end_date
    , submission.dat_set_ver
    , ward_stay.dmic_dataset
    , ward_stay.effective_from
    , ward_stay.dmic_date_added
from latest_ward_stay as ward_stay
left join {{ ref('stg_mhsds_activesubmission') }} as submission
    on ward_stay.uniq_submission_id = submission.uniq_submission_id
