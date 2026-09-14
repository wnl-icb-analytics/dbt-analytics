{{ config(materialized='table', tags=['mhsds']) }}

with active_referrals as (
    select
        r.uniq_serv_req_id
        , r.mhs101_uniq_id
        , r.service_request_id
        , r.person_id
        , r.org_id_prov
        , r.org_id_comm
        , r.org_id_referring_org
        , r.org_id_referring
        , r.nhs_serv_agree_line_id
        , r.nhs_serv_agree_line_num
        , r.specialised_mh_service_code
        -- 1899/1900 values are source missing-date sentinels from Excel epochs.
        , iff(
            r.referral_request_received_date::date < '1901-01-01'::date
            , null
            , r.referral_request_received_date::date
        ) as referral_request_received_date
        , r.referral_request_received_time::time as referral_request_received_time
        , iff(
            r.decision_to_treat_date::date < '1901-01-01'::date
            , null
            , r.decision_to_treat_date::date
        ) as decision_to_treat_date
        , r.decision_to_treat_time::time as decision_to_treat_time
        , r.disch_plan_creation_date::date as disch_plan_creation_date
        , r.disch_plan_creation_time::time as disch_plan_creation_time
        , r.disch_plan_last_updated_date::date as disch_plan_last_updated_date
        , r.disch_plan_last_updated_time::time as disch_plan_last_updated_time
        , r.refer_rejection_date::date as refer_rejection_date
        , r.refer_rejection_time::time as refer_rejection_time
        , r.refer_reject_reason
        , r.serv_disch_date::date as serv_disch_date
        , r.serv_disch_time::time as serv_disch_time
        , r.refer_clos_reason
        , iff(
            r.disch_letter_iss_date::date < '1901-01-01'::date
            , null
            , r.disch_letter_iss_date::date
        ) as disch_letter_iss_date
        , r.source_of_referral_mh
        , r.referring_care_professional_type
        , r.referring_care_professional_staff_group
        , r.clin_resp_priority_type
        , r.prim_reason_referral_mh
        , r.reason_oat
        , r.care_prof_team_local_id
        , r.uniq_care_prof_team_local_id
        , r.serv_team_type
        , r.serv_team_int_age_group
        , r.service_type_name
        , r.age_serv_refer_rec_date
        , r.age_serv_refer_disch_date
        , r.record_start_date::date as record_start_date
        , r.record_end_date::date as record_end_date
        , r.dm_icb_commissioner
        , r.dm_sub_icb_commissioner
        , r.dm_commissioner_derivation_reason
        , r.uniq_submission_id
        , r.uniq_month_id
        , r.reporting_period_start_date::date as reporting_period_start_date
        , r.reporting_period_end_date::date as reporting_period_end_date
        , r.dmic_dataset
        , r.effective_from
        , r.dmic_date_added
    from {{ ref('raw_mhsds_mhs101referral') }} as r
    inner join {{ ref('stg_mhsds_activesubmission') }} as a
        on r.uniq_submission_id = a.uniq_submission_id
    qualify row_number() over (
        partition by r.uniq_serv_req_id
        order by
            r.reporting_period_end_date desc
            , r.effective_from desc
            , r.uniq_submission_id desc
            , r.mhs101_uniq_id desc
    ) = 1
)

select
    uniq_serv_req_id
    , mhs101_uniq_id
    , service_request_id
    , person_id
    , org_id_prov
    , org_id_comm
    , org_id_referring_org
    , org_id_referring
    , nhs_serv_agree_line_id
    , nhs_serv_agree_line_num
    , specialised_mh_service_code
    , referral_request_received_date
    , referral_request_received_time
    , decision_to_treat_date
    , decision_to_treat_time
    , disch_plan_creation_date
    , disch_plan_creation_time
    , disch_plan_last_updated_date
    , disch_plan_last_updated_time
    , refer_rejection_date
    , refer_rejection_time
    , refer_reject_reason
    , serv_disch_date
    , serv_disch_time
    , refer_clos_reason
    , disch_letter_iss_date
    , source_of_referral_mh
    , referring_care_professional_type
    , referring_care_professional_staff_group
    , clin_resp_priority_type
    , prim_reason_referral_mh
    , reason_oat
    , care_prof_team_local_id
    , uniq_care_prof_team_local_id
    , serv_team_type
    , serv_team_int_age_group
    , service_type_name
    , age_serv_refer_rec_date
    , age_serv_refer_disch_date
    , record_start_date
    , record_end_date
    , dm_icb_commissioner
    , dm_sub_icb_commissioner
    , dm_commissioner_derivation_reason
    , uniq_submission_id
    , uniq_month_id
    , reporting_period_start_date
    , reporting_period_end_date
    , dmic_dataset
    , effective_from
    , dmic_date_added
from active_referrals
