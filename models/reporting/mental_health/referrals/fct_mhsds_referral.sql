select
    r.uniq_serv_req_id as source_record_id
    , 'MHSDS' as source_dataset
    , r.uniq_serv_req_id
    , r.mhs101_uniq_id
    , r.service_request_id
    , r.person_id
    , b.sk_patient_id
    , r.referral_request_received_date as referral_received_date
    , r.referral_request_received_time as referral_received_time
    , case
        when r.referral_request_received_date is null then null
        when r.referral_request_received_time is null
            then r.referral_request_received_date::timestamp_ntz
        else timestamp_ntz_from_parts(
                r.referral_request_received_date
                , r.referral_request_received_time
            )
    end as referral_received_at
    , case
        when r.referral_request_received_date is null then null
        when r.referral_request_received_time is null then 'date'
        else 'timestamp'
    end as referral_received_time_precision
    , r.decision_to_treat_date
    , r.decision_to_treat_time
    , case
        when r.decision_to_treat_date is null then null
        when r.decision_to_treat_time is null
            then r.decision_to_treat_date::timestamp_ntz
        else timestamp_ntz_from_parts(r.decision_to_treat_date, r.decision_to_treat_time)
    end as decision_to_treat_at
    , case
        when r.decision_to_treat_date is null then null
        when r.decision_to_treat_time is null then 'date'
        else 'timestamp'
    end as decision_to_treat_time_precision
    , r.disch_plan_creation_date as discharge_plan_created_date
    , r.disch_plan_creation_time as discharge_plan_created_time
    , case
        when r.disch_plan_creation_date is null then null
        when r.disch_plan_creation_time is null
            then r.disch_plan_creation_date::timestamp_ntz
        else timestamp_ntz_from_parts(
                r.disch_plan_creation_date
                , r.disch_plan_creation_time
            )
    end as discharge_plan_created_at
    , case
        when r.disch_plan_creation_date is null then null
        when r.disch_plan_creation_time is null then 'date'
        else 'timestamp'
    end as discharge_plan_created_time_precision
    , r.disch_plan_last_updated_date as discharge_plan_last_updated_date
    , r.disch_plan_last_updated_time as discharge_plan_last_updated_time
    , case
        when r.disch_plan_last_updated_date is null then null
        when r.disch_plan_last_updated_time is null
            then r.disch_plan_last_updated_date::timestamp_ntz
        else timestamp_ntz_from_parts(
                r.disch_plan_last_updated_date
                , r.disch_plan_last_updated_time
            )
    end as discharge_plan_last_updated_at
    , case
        when r.disch_plan_last_updated_date is null then null
        when r.disch_plan_last_updated_time is null then 'date'
        else 'timestamp'
    end as discharge_plan_last_updated_time_precision
    , r.refer_rejection_date as referral_rejection_date
    , r.refer_rejection_time as referral_rejection_time
    , case
        when r.refer_rejection_date is null then null
        when r.refer_rejection_time is null
            then r.refer_rejection_date::timestamp_ntz
        else timestamp_ntz_from_parts(r.refer_rejection_date, r.refer_rejection_time)
    end as referral_rejected_at
    , case
        when r.refer_rejection_date is null then null
        when r.refer_rejection_time is null then 'date'
        else 'timestamp'
    end as referral_rejected_time_precision
    , r.serv_disch_date as referral_discharge_date
    , r.serv_disch_time as referral_discharge_time
    , case
        when r.serv_disch_date is null then null
        when r.serv_disch_time is null then r.serv_disch_date::timestamp_ntz
        else timestamp_ntz_from_parts(r.serv_disch_date, r.serv_disch_time)
    end as referral_discharged_at
    , case
        when r.serv_disch_date is null then null
        when r.serv_disch_time is null then 'date'
        else 'timestamp'
    end as referral_discharged_time_precision
    , case
        when r.refer_rejection_date is not null then 'rejected'
        when r.serv_disch_date is not null then 'closed'
        else 'open'
    end as referral_status
    , r.source_of_referral_mh as source_of_referral_code
    , sr.description as source_of_referral_description
    , r.clin_resp_priority_type as clinical_response_priority_code
    , clinical_priority.description as clinical_response_priority_description
    , r.prim_reason_referral_mh as primary_reason_for_referral_code
    , referral_reason.description as primary_reason_for_referral_description
    , r.referring_care_professional_type as referring_care_professional_type_code
    , referrer_type.description as referring_care_professional_type_description
    , r.referring_care_professional_staff_group as referring_care_professional_staff_group_code
    , referrer_staff_group.description as referring_care_professional_staff_group_description
    , r.refer_reject_reason as rejection_reason_code
    , rejection_reason.description as rejection_reason_description
    , r.refer_clos_reason as closure_reason_code
    , closure_reason.description as closure_reason_description
    , r.reason_oat as out_of_area_treatment_reason_code
    , out_of_area_reason.description as out_of_area_treatment_reason_description
    , r.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , r.org_id_comm as commissioner_organisation_code
    , source_commissioner.organisation_name as commissioner_organisation_name
    , r.dm_icb_commissioner as source_derived_icb_commissioner_code
    , coalesce(
        r.dm_icb_commissioner
        , iff(source_commissioner.is_integrated_care_board, r.org_id_comm, null)
    ) as derived_icb_commissioner_code
    , coalesce(
        derived_icb.organisation_name
        , iff(
            source_commissioner.is_integrated_care_board
            , source_commissioner.organisation_name
            , null
        )
    ) as derived_icb_commissioner_name
    , case
        when r.dm_icb_commissioner is not null then 'source_derived'
        when source_commissioner.is_integrated_care_board then 'submitted_icb'
    end as icb_commissioner_derivation_method
    , coalesce(derived_icb.is_wnl_commissioner, false)
        or coalesce(source_commissioner.is_wnl_commissioner, false)
        as is_wnl_commissioner
    , r.dm_sub_icb_commissioner as derived_sub_icb_commissioner_code
    , derived_sub_icb.organisation_name as derived_sub_icb_commissioner_name
    , r.org_id_referring_org as referring_organisation_code
    , referring_organisation.organisation_name as referring_organisation_name
    , r.org_id_referring as referring_organisation_or_person_code
    , r.nhs_serv_agree_line_id as nhs_service_agreement_line_id
    , r.nhs_serv_agree_line_num as nhs_service_agreement_line_number
    , r.specialised_mh_service_code as specialised_mental_health_service_category_code
    , r.care_prof_team_local_id as primary_service_or_team_local_id
    , r.uniq_care_prof_team_local_id as primary_service_or_team_id
    , coalesce(r.serv_team_type, td.serv_team_type_mh) as primary_service_or_team_type_code
    , tt.description as primary_service_or_team_type_description
    , coalesce(r.service_type_name, td.service_type_name) as source_service_or_team_type_name
    , coalesce(r.serv_team_int_age_group, td.serv_team_int_age_group)
        as primary_service_or_team_intended_age_group_code
    , intended_age_group.description
        as primary_service_or_team_intended_age_group_description
    , r.age_serv_refer_rec_date as age_at_referral
    , r.age_serv_refer_disch_date as age_at_discharge
    , r.disch_letter_iss_date as discharge_letter_issued_date
    , r.record_start_date
    , r.record_end_date
    , r.uniq_submission_id
    , r.uniq_month_id
    , r.reporting_period_start_date
    , r.reporting_period_end_date
    , r.dmic_dataset as mhsds_version
    , r.effective_from as source_file_received_at
    , r.dmic_date_added as source_loaded_at
from {{ ref('stg_mhsds_referral') }} as r
left join {{ ref('stg_mhsds_bridging') }} as b
    on r.person_id = b.person_id
left join {{ ref('mhsds_source_of_referral') }} as sr
    on r.source_of_referral_mh = sr.code
left join {{ ref('stg_mhsds_service_or_team_details') }} as td
    on r.care_prof_team_local_id = td.care_prof_team_local_id
    and r.uniq_submission_id = td.uniq_submission_id
left join {{ ref('mhsds_service_or_team_type') }} as tt
    on coalesce(r.serv_team_type, td.serv_team_type_mh) = tt.code
left join {{ ref('mhsds_service_or_team_intended_age_group') }} as intended_age_group
    on upper(trim(coalesce(r.serv_team_int_age_group, td.serv_team_int_age_group)))
        = intended_age_group.code
left join {{ ref('mhsds_referral_code_lookup') }} as clinical_priority
    on upper(trim(r.clin_resp_priority_type)) = clinical_priority.code
    and clinical_priority.code_set_name = 'clinical_response_priority'
left join {{ ref('mhsds_referral_code_lookup') }} as referral_reason
    on upper(trim(r.prim_reason_referral_mh)) = referral_reason.code
    and referral_reason.code_set_name = 'primary_reason_for_referral'
left join {{ ref('mhsds_referral_code_lookup') }} as referrer_type
    on upper(trim(r.referring_care_professional_type)) = referrer_type.code
    and referrer_type.code_set_name = 'referring_care_professional_type'
left join {{ ref('mhsds_referral_code_lookup') }} as referrer_staff_group
    on upper(trim(r.referring_care_professional_staff_group)) = referrer_staff_group.code
    and referrer_staff_group.code_set_name = 'referring_care_professional_staff_group'
left join {{ ref('mhsds_referral_code_lookup') }} as rejection_reason
    on upper(trim(r.refer_reject_reason)) = rejection_reason.code
    and rejection_reason.code_set_name = 'rejection_reason'
left join {{ ref('mhsds_referral_code_lookup') }} as closure_reason
    on upper(trim(r.refer_clos_reason)) = closure_reason.code
    and closure_reason.code_set_name = 'closure_reason'
left join {{ ref('mhsds_referral_code_lookup') }} as out_of_area_reason
    on upper(trim(r.reason_oat)) = out_of_area_reason.code
    and out_of_area_reason.code_set_name = 'out_of_area_referral_reason'
left join {{ ref('int_mhsds_organisation') }} as source_commissioner
    on upper(r.org_id_comm) = upper(source_commissioner.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(r.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as referring_organisation
    on upper(r.org_id_referring_org) = upper(referring_organisation.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_icb
    on upper(r.dm_icb_commissioner) = upper(derived_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as derived_sub_icb
    on upper(r.dm_sub_icb_commissioner) = upper(derived_sub_icb.organisation_code)
