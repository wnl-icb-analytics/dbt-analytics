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
        , count(distinct reporting_period_end_date) over (partition by referral_id) as reported_period_count
        -- Versions can name different people; each record keeps its own identity.
        , min(person_id) over (partition by referral_id)
            is distinct from max(person_id) over (partition by referral_id) as has_person_identifier_changed
        , min(sk_patient_id) over (partition by referral_id)
            is distinct from max(sk_patient_id) over (partition by referral_id) as has_patient_key_changed
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
        l.*
        -- Scale 9 matches fct_iapt_assessment_score, so a fractional score is never rounded.
        , try_to_decimal(phq9_first_score, 38, 9) as phq9_first_score_value
        , try_to_decimal(phq9_last_score, 38, 9) as phq9_last_score_value
        , try_to_decimal(gad_first_score, 38, 9) as gad7_first_score_value
        , try_to_decimal(gad_last_score, 38, 9) as gad7_last_score_value
        , adsm_first_score::number(38, 9) as adsm_first_score_value
        , adsm_last_score::number(38, 9) as adsm_last_score_value
        -- The warehouse copies the one submitted item into both source columns, so the data set version decides
        -- the list: v2.0 SourceOfReferralMH, renamed SourceOfReferralIAPT in v2.1 (DARS v2.1.7 PC_FIELDS A109).
        , iff(dataset_version = '2.0', source_of_referral_mh, source_of_referral_iapt) as source_of_referral_code
        , try_to_decimal(wasas_home_management_first_score, 38, 9) as wsas_home_management_first_score_value
        , try_to_decimal(wasas_private_leisure_activities_first_score, 38, 9) as wsas_private_leisure_activities_first_score_value
        , try_to_decimal(wasas_relationships_first_score, 38, 9) as wsas_relationships_first_score_value
        , try_to_decimal(wasas_social_leisure_activities_first_score, 38, 9) as wsas_social_leisure_activities_first_score_value
        , try_to_decimal(wasas_work_first_score, 38, 9) as wsas_work_first_score_value
        , try_to_decimal(wasas_home_management_last_score, 38, 9) as wsas_home_management_last_score_value
        , try_to_decimal(wasas_private_leisure_activities_last_score, 38, 9) as wsas_private_leisure_activities_last_score_value
        , try_to_decimal(wasas_relationships_last_score, 38, 9) as wsas_relationships_last_score_value
        , try_to_decimal(wasas_social_leisure_activities_last_score, 38, 9) as wsas_social_leisure_activities_last_score_value
        , try_to_decimal(wasas_work_last_score, 38, 9) as wsas_work_last_score_value
        -- UKHFD employment status codes are two-character; pad so a submitted 1 matches 01.
        , iff(regexp_like(employment_status_first, '[0-9]'), lpad(employment_status_first, 2, '0'),
            upper(employment_status_first)) as employment_status_first_normalised
        , iff(regexp_like(employment_status_last, '[0-9]'), lpad(employment_status_last, 2, '0'),
            upper(employment_status_last)) as employment_status_last_normalised
        -- NHS England sets True when discharged with two or more treatment contacts, else null. FALSE needs a
        -- visibly failed criterion; a null flag whose criteria look met stays unknown.
        , case
            when completed_treatment_flag then true
            when serv_disch_date is null then false
            when treatment_care_contact_count < 2 then false
        end as is_completed_treatment
    from latest as l
)

-- Groups are UKHFD categories. v2.0 codes use the retired mental health list v2.0 submitted against; the shared
-- lookup prefers the MHSDS list, which has no category for H2. group_key folds case, spacing and punctuation, so
-- "Self referral" and "Self-Referral" share a key and take the v2.1 spelling as group_name. A category with no v2.1
-- counterpart keeps its own name.
, categorised_codes as (
    select
        code_set_name
        , code
        , trim(regexp_replace(lower(category), '[^a-z0-9]+', '_'), '_') as group_key
        , regexp_replace(category, '^[[:space:]]+|[[:space:]]+$') as category
    from {{ ref('iapt_code_lookup') }}
    where code_set_name in ('source_of_referral', 'discharge_reason', 'discharge_reason_legacy')
        and category is not null
    union all
    select
        'source_of_referral_mental_health' as code_set_name
        , code
        , trim(regexp_replace(lower(category), '[^a-z0-9]+', '_'), '_') as group_key
        , regexp_replace(category, '^[[:space:]]+|[[:space:]]+$') as category
    from {{ ref('mhsds_source_of_referral_history') }}
    where is_latest_definition
        and source_code_set_name = 'Source_Of_Referral_For_Mental_Health'
        and category is not null
)

, iapt_referral_source_groups as (
    select
        group_key
        , min(category) as group_name
    from categorised_codes
    where code_set_name = 'source_of_referral'
    group by group_key
)

, code_groups as (
    select
        c.code_set_name
        , c.code
        , c.group_key
        , coalesce(v.group_name, c.category) as group_name
    from categorised_codes as c
    left join iapt_referral_source_groups as v
        on c.code_set_name = 'source_of_referral_mental_health'
        and c.group_key = v.group_key
)

select
    o.referral_id as source_record_id
    , 'IAPT' as source_dataset
    , o.referral_id
    , o.service_request_id as local_referral_id
    , o.person_id
    , o.sk_patient_id
    , o.has_person_identifier_changed
    , o.has_patient_key_changed
    , o.referral_request_received_date as referral_received_date
    , o.serv_disch_date as service_discharge_date
    -- Recorded discharge only, as in fct_mhsds_referral; fct_iapt_referral_summary infers the as-of status.
    , iff(o.serv_disch_date is not null, 'closed', 'open') as referral_status
    , o.age_referral_request_received_date as age_at_referral
    , {{ age_band_nhs('o.age_referral_request_received_date') }} as age_band_nhs_at_referral
    , o.age_service_discharge_date as age_at_discharge
    , o.source_of_referral_code
    , coalesce(source_mh.description, source_iapt.description) as source_of_referral_name
    , case
        when o.dataset_version = '2.0' and o.source_of_referral_mh is not null then 'mental_health'
        when o.dataset_version = '2.1' and o.source_of_referral_iapt is not null then 'iapt'
    end as source_of_referral_code_set
    , source_group.group_key as source_of_referral_group
    , source_group.group_name as source_of_referral_group_name
    , o.end_code as discharge_reason_code
    , coalesce(discharge.description, discharge_legacy.description) as discharge_reason_name
    , case
        when discharge.code is not null then 'discharge_reason'
        when discharge_legacy.code is not null then 'discharge_reason_legacy'
    end as discharge_reason_code_set
    , discharge_group.group_key as discharge_reason_group
    , discharge_group.group_name as discharge_reason_group_name
    , o.prev_diag_cond_ind as previous_diagnosed_condition_code
    , previous_condition.description as previous_diagnosed_condition_name
    , o.onset_date as symptom_onset_month
    , iff(
        regexp_like(o.onset_date, '[0-9]{4}-[0-9]{2}')
        , try_to_date(o.onset_date || '-01', 'YYYY-MM-DD')
        , null
    ) as symptom_onset_month_start_date
    , o.assessment_first_date as first_assessment_date
    , o.assessment_last_date as last_assessment_date
    , o.therapy_session_first_date as first_treatment_date
    , o.therapy_session_second_date as second_treatment_date
    , o.therapy_session_last_date as last_treatment_date
    , o.care_contact_count as nhse_care_contact_count
    , o.treatment_care_contact_count as nhse_treatment_contact_count
    , o.is_completed_treatment
    , o.adsm as anxiety_disorder_specific_measure
    , adsm_scale.assessment_tool_name as anxiety_disorder_specific_measure_name
    , o.phq9_first_score_value as phq9_first_score
    , o.phq9_last_score_value as phq9_last_score
    , o.gad7_first_score_value as gad7_first_score
    , o.gad7_last_score_value as gad7_last_score
    , o.adsm_first_score_value as adsm_first_score
    , o.adsm_last_score_value as adsm_last_score
    -- Caseness flags need completed treatment and first scores; neither can be shown without both scores.
    , case
        when o.is_completed_treatment is distinct from true then null
        when o.caseness_flag then 'at_caseness'
        when o.not_caseness_flag then 'not_at_caseness'
        when o.phq9_first_score_value is null or o.adsm_first_score_value is null then 'not_assessable'
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
            or o.adsm_first_score_value is null or o.adsm_last_score_value is null then 'not_assessable'
    end as reliable_change_status
    -- NHS England therapy and course derivations, supplied values (ETOS v2.1.22 IDS101).
    , o.therapy_type_first as first_therapy_type_code
    , first_therapy_concept.preferred_term as first_therapy_type_name
    , coalesce(
        first_therapy.therapy_type_category
        , iff(startswith(o.therapy_type_first, 'IET'), 'Internet Enabled Therapy', null)
    ) as first_therapy_type_category
    , o.therapy_type_last as last_therapy_type_code
    , last_therapy_concept.preferred_term as last_therapy_type_name
    , coalesce(
        last_therapy.therapy_type_category
        , iff(startswith(o.therapy_type_last, 'IET'), 'Internet Enabled Therapy', null)
    ) as last_therapy_type_category
    , o.high_intensity_therapy_first_date as first_high_intensity_therapy_date
    , o.low_intensity_therapy_first_date as first_low_intensity_therapy_date
    , o.integrated_contact_first_date as first_integrated_contact_date
    , o.integrated_treatment_first_date as first_integrated_treatment_date
    , o.internet_enabled_therapy_count as nhse_internet_enabled_therapy_count
    , o.wsas_home_management_first_score_value as wsas_home_management_first_score
    , o.wsas_private_leisure_activities_first_score_value as wsas_private_leisure_activities_first_score
    , o.wsas_relationships_first_score_value as wsas_relationships_first_score
    , o.wsas_social_leisure_activities_first_score_value as wsas_social_leisure_activities_first_score
    , o.wsas_work_first_score_value as wsas_work_first_score
    , o.wsas_home_management_last_score_value as wsas_home_management_last_score
    , o.wsas_private_leisure_activities_last_score_value as wsas_private_leisure_activities_last_score
    , o.wsas_relationships_last_score_value as wsas_relationships_last_score
    , o.wsas_social_leisure_activities_last_score_value as wsas_social_leisure_activities_last_score
    , o.wsas_work_last_score_value as wsas_work_last_score
    , o.employment_status_first as employment_status_first_code
    , employment_first.description as employment_status_first_name
    , o.employment_status_last as employment_status_last_code
    , employment_last.description as employment_status_last_name
    , o.sickpay_indicator_first as statutory_sick_pay_first_code
    , sick_pay_first.description as statutory_sick_pay_first_name
    -- I004080 codes: Y receiving, N not receiving; U unknown and Z not stated stay null.
    , case upper(o.sickpay_indicator_first) when 'Y' then true when 'N' then false end
        as is_receiving_statutory_sick_pay_first
    , o.sickpay_indicator_last as statutory_sick_pay_last_code
    , sick_pay_last.description as statutory_sick_pay_last_name
    , case upper(o.sickpay_indicator_last) when 'Y' then true when 'N' then false end
        as is_receiving_statutory_sick_pay_last
    , o.psychotropic_indicator_first as psychotropic_medication_first_code
    , psychotropic_first.description as psychotropic_medication_first_name
    , o.psychotropic_indicator_last as psychotropic_medication_last_code
    , psychotropic_last.description as psychotropic_medication_last_name
    , o.presenting_complaint_higher_category
    , o.presenting_complaint_lower_category
    , o.use_pathway_flag as is_nhse_use_pathway
    , o.use_quarter_referral_flag as is_nhse_use_quarter
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
    , o.pathway_id
    , o.submission_id
    , o.source_row_id
    , o.unique_month_id
    , o.reporting_period_start_date
    , o.reporting_period_end_date
    , o.source_file_received_at
    , o.source_loaded_at
    , o.file_type
    , o.dataset_version
from outcomes as o
left join {{ ref('iapt_code_lookup') }} as source_iapt
    on o.dataset_version = '2.1'
    and source_iapt.code_set_name = 'source_of_referral'
    and upper(o.source_of_referral_iapt) = source_iapt.code
left join {{ ref('mhsds_source_of_referral') }} as source_mh
    on o.dataset_version = '2.0'
    and upper(o.source_of_referral_mh) = source_mh.code
left join {{ ref('iapt_code_lookup') }} as discharge
    on discharge.code_set_name = 'discharge_reason'
    and upper(o.end_code) = discharge.code
-- Retired care spell end codes (valid to March 2020) label only codes absent from the current list.
left join {{ ref('iapt_code_lookup') }} as discharge_legacy
    on discharge.code is null
    and discharge_legacy.code_set_name = 'discharge_reason_legacy'
    and upper(o.end_code) = discharge_legacy.code
left join code_groups as source_group
    on source_group.code_set_name
        = iff(o.dataset_version = '2.0', 'source_of_referral_mental_health', 'source_of_referral')
    and upper(o.source_of_referral_code) = source_group.code
left join code_groups as discharge_group
    on discharge_group.code_set_name = iff(discharge.code is not null, 'discharge_reason', 'discharge_reason_legacy')
    and upper(o.end_code) = discharge_group.code
left join {{ ref('iapt_code_group') }} as adsm_group
    on adsm_group.code_set_name = 'anxiety_disorder_specific_measure'
    and o.adsm = adsm_group.code
left join {{ ref('iapt_assessment_scale') }} as adsm_scale
    on adsm_group.group_code = adsm_scale.concept_code
    and adsm_scale.is_latest_definition
left join {{ ref('iapt_therapy_type_definitions') }} as first_therapy
    on o.therapy_type_first = first_therapy.snomed_code
left join {{ ref('iapt_therapy_type_definitions') }} as last_therapy
    on o.therapy_type_last = last_therapy.snomed_code
left join {{ ref('snomed_concept') }} as first_therapy_concept
    on o.therapy_type_first = first_therapy_concept.snomed_code
left join {{ ref('snomed_concept') }} as last_therapy_concept
    on o.therapy_type_last = last_therapy_concept.snomed_code
left join {{ ref('mhsds_domain_code_lookup') }} as employment_first
    on employment_first.code_set_name = 'employment_status'
    and o.employment_status_first_normalised = employment_first.code
left join {{ ref('mhsds_domain_code_lookup') }} as employment_last
    on employment_last.code_set_name = 'employment_status'
    and o.employment_status_last_normalised = employment_last.code
left join {{ ref('iapt_code_lookup') }} as psychotropic_first
    on psychotropic_first.code_set_name = 'psychotropic_medication_usage'
    and upper(o.psychotropic_indicator_first) = psychotropic_first.code
left join {{ ref('iapt_code_lookup') }} as psychotropic_last
    on psychotropic_last.code_set_name = 'psychotropic_medication_usage'
    and upper(o.psychotropic_indicator_last) = psychotropic_last.code
left join {{ ref('iapt_code_lookup') }} as sick_pay_first
    on sick_pay_first.code_set_name = 'statutory_sick_pay_indicator'
    and upper(o.sickpay_indicator_first) = sick_pay_first.code
left join {{ ref('iapt_code_lookup') }} as sick_pay_last
    on sick_pay_last.code_set_name = 'statutory_sick_pay_indicator'
    and upper(o.sickpay_indicator_last) = sick_pay_last.code
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
