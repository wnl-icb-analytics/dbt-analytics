with versions as (
    select
        r.*
        , b.sk_patient_id
    from {{ ref('stg_iapt_referral_history') }} as r
    left join {{ ref('stg_iapt_bridging') }} as b
        on r.person_id = b.person_id
)

, latest as (
    select
        *
        , min(reporting_period_end_date) over (partition by referral_id) as first_reported_period_end_date
        , max(reporting_period_end_date) over (partition by referral_id) as last_reported_period_end_date
        , count(*) over (partition by referral_id) as reported_period_count
        -- Versions can name different people; each record keeps its own identity.
        , min(person_id) over (partition by referral_id)
            <> max(person_id) over (partition by referral_id) as has_person_identifier_changed
        , min(sk_patient_id) over (partition by referral_id)
            <> max(sk_patient_id) over (partition by referral_id) as has_patient_key_changed
    from versions
    qualify row_number() over (
        partition by referral_id
        order by
            unique_month_id desc nulls last
            , source_file_received_at desc nulls last
            , try_to_number(submission_id) desc nulls last
            , try_to_number(source_row_id) desc nulls last
            , submission_id desc
            , source_row_id desc
    ) = 1
)

, outcomes as (
    select
        *
        , try_to_number(phq9_first_score) as phq9_first_score_value
        , try_to_number(phq9_last_score) as phq9_last_score_value
        , try_to_number(gad_first_score) as gad7_first_score_value
        , try_to_number(gad_last_score) as gad7_last_score_value
        -- NHS England sets True when discharged with two or more treatment contacts, else null. FALSE needs a
        -- visibly failed criterion; a null flag whose criteria look met stays unknown.
        , case
            when completed_treatment_flag then true
            when serv_disch_date is null then false
            when treatment_care_contact_count < 2 then false
        end as is_completed_treatment
    from latest
)

-- The mental health list unions current and legacy definitions, so a code can appear twice.
, mental_health_source_of_referral as (
    select
        code
        , description
    from {{ ref('mhsds_source_of_referral') }}
    qualify row_number() over (
        partition by code
        order by is_currently_valid desc, definition_updated_at desc
    ) = 1
)

, provider_periods as (
    select
        provider_organisation_code
        , max(reporting_period_end_date) as latest_provider_period_end_date
    from {{ ref('stg_iapt_activesubmission') }}
    group by provider_organisation_code
)

select
    o.referral_id as source_record_id
    , 'IAPT' as source_dataset
    , o.referral_id
    , o.service_request_id as local_referral_id
    , o.pathway_id
    , o.source_row_id
    , o.person_id
    , o.sk_patient_id
    , o.has_person_identifier_changed
    , o.has_patient_key_changed
    , o.referral_request_received_date as referral_received_date
    , o.serv_disch_date as service_discharge_date
    , iff(o.serv_disch_date is null, 'open', 'discharged') as referral_status
    , o.age_referral_request_received_date as age_at_referral
    , o.age_service_discharge_date as age_at_discharge
    , coalesce(o.source_of_referral_iapt, o.source_of_referral_mh) as source_of_referral_code
    , iff(o.source_of_referral_iapt is not null, source_iapt.description, source_mh.description)
        as source_of_referral_name
    , case
        when o.source_of_referral_iapt is not null then 'iapt'
        when o.source_of_referral_mh is not null then 'mental_health'
    end as source_of_referral_code_set
    , o.end_code as discharge_reason_code
    , coalesce(discharge.description, discharge_legacy.description) as discharge_reason_name
    , case
        when discharge.code is not null then 'discharge_reason'
        when discharge_legacy.code is not null then 'discharge_reason_legacy'
    end as discharge_reason_code_set
    , discharge.category as discharge_reason_category
    , o.prev_diag_cond_ind as previous_diagnosed_condition_code
    , previous_condition.description as previous_diagnosed_condition_name
    , o.onset_date as symptom_onset_month
    , iff(
        regexp_like(o.onset_date, '[0-9]{4}-[0-9]{2}')
        , try_to_date(o.onset_date || '-01', 'YYYY-MM-DD')
        , null
    ) as symptom_onset_month_start_date
    , o.assessment_first_date as first_assessment_date
    , o.therapy_session_first_date as first_treatment_date
    , o.therapy_session_second_date as second_treatment_date
    , o.therapy_session_last_date as last_treatment_date
    , o.care_contact_count as attended_contact_count
    , o.treatment_care_contact_count as treatment_contact_count
    , o.is_completed_treatment
    , o.adsm as anxiety_disorder_specific_measure
    , o.phq9_first_score_value as phq9_first_score
    , o.phq9_last_score_value as phq9_last_score
    , o.gad7_first_score_value as gad7_first_score
    , o.gad7_last_score_value as gad7_last_score
    , o.adsm_first_score
    , o.adsm_last_score
    -- Caseness flags need completed treatment and first scores; neither can be shown without both scores.
    , case
        when o.is_completed_treatment is distinct from true then null
        when o.caseness_flag then 'at_caseness'
        when o.not_caseness_flag then 'not_at_caseness'
        when o.phq9_first_score_value is null or o.adsm_first_score is null then 'not_assessable'
    end as caseness_at_start_status
    -- Source flag as supplied: recovery thresholds differ by data set version, so no negative is inferred.
    , o.recovery_flag as is_recovered
    -- Reliable change needs first and last PHQ-9 and anxiety scores; with all four one flag should be set.
    , case
        when o.is_completed_treatment is distinct from true then null
        when o.reliable_improvement_flag then 'reliable_improvement'
        when o.reliable_deterioration_flag then 'reliable_deterioration'
        when o.no_change_flag then 'no_reliable_change'
        when o.phq9_first_score_value is null or o.phq9_last_score_value is null
            or o.adsm_first_score is null or o.adsm_last_score is null then 'not_assessable'
    end as reliable_change_status
    , o.presenting_complaint_higher_category
    , o.presenting_complaint_lower_category
    , o.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , o.org_id_comm as submitted_commissioner_code
    , commissioner.organisation_name as submitted_commissioner_name
    , o.dm_icb_commissioner as source_icb_commissioner_code
    , icb.organisation_name as source_icb_commissioner_name
    , o.dm_sub_icb_commissioner as source_sub_icb_commissioner_code
    , sub_icb.organisation_name as source_sub_icb_commissioner_name
    , o.dm_commissioner_derivation_reason as source_commissioner_derivation_reason
    , coalesce(wnl_icb.commissioner_code, wnl_sub_icb.commissioner_code, wnl_submitted.commissioner_code) is not null
        as is_wnl_commissioner
    , o.first_reported_period_end_date
    , o.last_reported_period_end_date
    , o.reported_period_count
    , o.reporting_period_end_date = pp.latest_provider_period_end_date as is_in_latest_provider_period
    , o.submission_id
    , o.unique_month_id
    , o.reporting_period_start_date
    , o.reporting_period_end_date
    , o.source_file_received_at
    , o.source_loaded_at
    , o.file_type
    , o.dataset_version
from outcomes as o
left join provider_periods as pp
    on o.provider_organisation_code = pp.provider_organisation_code
left join {{ ref('iapt_code_lookup') }} as source_iapt
    on source_iapt.code_set_name = 'source_of_referral'
    and upper(o.source_of_referral_iapt) = source_iapt.code
left join mental_health_source_of_referral as source_mh
    on upper(o.source_of_referral_mh) = source_mh.code
left join {{ ref('iapt_code_lookup') }} as discharge
    on discharge.code_set_name = 'discharge_reason'
    and upper(o.end_code) = discharge.code
-- Retired care spell end codes (valid to March 2020) label only codes absent from the current list.
left join {{ ref('iapt_code_lookup') }} as discharge_legacy
    on discharge.code is null
    and discharge_legacy.code_set_name = 'discharge_reason_legacy'
    and upper(o.end_code) = discharge_legacy.code
left join {{ ref('iapt_code_lookup') }} as previous_condition
    on previous_condition.code_set_name = 'previous_diagnosed_condition_indicator'
    and upper(o.prev_diag_cond_ind) = previous_condition.code
left join {{ ref('organisation') }} as provider
    on o.provider_organisation_code = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on o.org_id_comm = commissioner.organisation_code
left join {{ ref('organisation') }} as icb
    on o.dm_icb_commissioner = icb.organisation_code
left join {{ ref('organisation') }} as sub_icb
    on o.dm_sub_icb_commissioner = sub_icb.organisation_code
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_icb
    on o.dm_icb_commissioner = upper(wnl_icb.commissioner_code)
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_sub_icb
    on o.dm_sub_icb_commissioner = upper(wnl_sub_icb.commissioner_code)
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_submitted
    on o.org_id_comm = upper(wnl_submitted.commissioner_code)
