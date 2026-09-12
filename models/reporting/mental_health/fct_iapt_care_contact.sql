with contacts as (
    select
        *
        , nullif(ltrim(attend_or_dna_code, '0'), '') as attendance_code_normalised
        , count(*) over (partition by referral_id, care_contact_id) as reported_period_count
    from {{ ref('stg_iapt_care_contact_history') }}
    qualify row_number() over (
        partition by referral_id, care_contact_id
        order by
            unique_month_id desc nulls last
            , source_file_received_at desc nulls last
            , try_to_number(submission_id) desc nulls last
            , try_to_number(source_row_id) desc nulls last
            , submission_id desc
            , source_row_id desc
    ) = 1
)

, activity_counts as (
    select
        submission_id
        , referral_id
        , care_contact_id
        , count(*) as care_activity_count
    from {{ ref('stg_iapt_care_activity_history') }}
    group by submission_id, referral_id, care_contact_id
)

, classified as (
    select
        c.*
        -- Recorded outcome only; unplanned contacts may carry no attendance code.
        , case
            when c.attendance_code_normalised in ('5', '6') then true
            when c.attendance_code_normalised in ('2', '3', '4', '7') then false
        end as is_attended
        -- NHS England contact-count condition: code 5 or 6 OR not a planned appointment, as declared.
        , coalesce(c.attendance_code_normalised in ('5', '6'), false)
            or coalesce(upper(c.planned_care_cont_indicator) = 'N', false) as is_nhse_attended_or_unplanned
        -- The source only warns when attended contacts share a referral, date and time.
        , case
            when c.care_cont_time is null
                or c.attendance_code_normalised is null
                or c.attendance_code_normalised not in ('5', '6') then null
            else count_if(c.attendance_code_normalised in ('5', '6')) over (
                partition by c.referral_id, c.care_cont_date, c.care_cont_time
            ) > 1
        end as is_attended_slot_duplicate
    from contacts as c
)

select
    {{ dbt_utils.generate_surrogate_key(['c.referral_id', 'c.care_contact_id']) }} as source_record_id
    , 'IAPT' as source_dataset
    , c.referral_id
    , c.care_contact_id
    , c.local_care_contact_id
    , c.pathway_id
    , c.source_row_id
    , c.person_id
    , b.sk_patient_id
    , c.age_care_contact_date as age_at_contact
    , c.care_cont_date as care_contact_date
    , c.care_cont_time as care_contact_time
    , iff(
        c.care_cont_date is null
        , null
        , timestamp_ntz_from_parts(c.care_cont_date, coalesce(c.care_cont_time, '00:00:00'::time))
    ) as care_contact_at
    , case
        when c.care_cont_date is null then 'unknown'
        when c.care_cont_time is null then 'date'
        else 'timestamp'
    end as care_contact_time_precision
    , c.app_type as appointment_type_code
    , appointment_type.description as appointment_type_name
    , iff(c.app_type is null, null, lpad(c.app_type, 2, '0') in ('02', '03', '05')) as is_treatment_appointment_type
    , c.attend_or_dna_code as attendance_code
    , attendance.description as attendance_name
    , c.is_attended
    , c.is_nhse_attended_or_unplanned
    , c.planned_care_cont_indicator as planned_care_contact_code
    , planned.description as planned_care_contact_name
    , c.cancellation as short_notice_cancellation_code
    , cancellation.description as short_notice_cancellation_name
    , c.clin_cont_dur_of_care_cont as clinical_contact_duration_minutes
    , c.cons_mechanism as consultation_mechanism_code
    , coalesce(mechanism.description, mechanism_as_medium.description) as consultation_mechanism_name
    , case
        when mechanism.code is not null then 'consultation_mechanism'
        when mechanism_as_medium.code is not null then 'consultation_medium_used'
    end as consultation_mechanism_code_set
    , c.cons_medium_used as consultation_medium_used_code
    , medium.description as consultation_medium_used_name
    , c.care_cont_patient_ther_mode as patient_therapy_mode_code
    , therapy_mode.description as patient_therapy_mode_name
    , try_to_number(c.num_group_ther_participants) as group_therapy_participant_count
    , try_to_number(c.num_group_ther_facilitators) as group_therapy_facilitator_count
    , c.psych_med as psychotropic_medication_code
    , psychotropic.description as psychotropic_medication_name
    , c.iaptltc_service_ind as integrated_ltc_service_code
    , integrated_ltc.description as integrated_ltc_service_name
    , c.act_loc_type_code as activity_location_type_code
    , location.description as activity_location_type_name
    , c.site_id_of_treat as site_code
    , site.organisation_name as site_name
    , c.language_code_treat as treatment_language_code
    , treatment_language.description as treatment_language_name
    , c.interpreter_present_ind as interpreter_present_code
    , interpreter.description as interpreter_present_name
    , coalesce(ac.care_activity_count, 0) as care_activity_count
    , c.is_attended_slot_duplicate
    , sr.referral_id is not null as is_submitted_referral_linked
    , case
        when sr.referral_id is null or c.person_id is null or sr.person_id is null then null
        else c.person_id = sr.person_id
    end as is_submitted_referral_person_consistent
    , r.source_record_id is not null as is_referral_linked
    , case
        when r.source_record_id is null or c.person_id is null or r.person_id is null then null
        else c.person_id = r.person_id
    end as is_referral_person_consistent
    , c.provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , c.org_id_comm as submitted_commissioner_code
    , commissioner.organisation_name as submitted_commissioner_name
    , c.dm_icb_commissioner as source_icb_commissioner_code
    , icb.organisation_name as source_icb_commissioner_name
    , c.dm_sub_icb_commissioner as source_sub_icb_commissioner_code
    , sub_icb.organisation_name as source_sub_icb_commissioner_name
    , c.dm_commissioner_derivation_reason as source_commissioner_derivation_reason
    , coalesce(wnl_icb.commissioner_code, wnl_sub_icb.commissioner_code, wnl_submitted.commissioner_code) is not null
        as is_wnl_commissioner
    , c.reported_period_count
    , c.submission_id
    , c.unique_month_id
    , c.reporting_period_start_date
    , c.reporting_period_end_date
    , c.source_file_received_at
    , c.source_loaded_at
    , c.file_type
    , c.dataset_version
from classified as c
left join {{ ref('stg_iapt_bridging') }} as b
    on c.person_id = b.person_id
left join activity_counts as ac
    on c.submission_id = ac.submission_id
    and c.referral_id = ac.referral_id
    and c.care_contact_id = ac.care_contact_id
left join {{ ref('stg_iapt_referral_history') }} as sr
    on c.submission_id = sr.submission_id
    and c.referral_id = sr.referral_id
left join {{ ref('fct_iapt_referral') }} as r
    on c.referral_id = r.referral_id
left join {{ ref('attendance_status') }} as attendance
    on c.attendance_code_normalised = attendance.code
left join {{ ref('iapt_code_lookup') }} as appointment_type
    on appointment_type.code_set_name = 'appointment_type'
    and upper(c.app_type) = appointment_type.code
left join {{ ref('iapt_code_lookup') }} as cancellation
    on cancellation.code_set_name = 'short_notice_cancellation_indicator'
    and upper(c.cancellation) = cancellation.code
left join {{ ref('iapt_code_lookup') }} as psychotropic
    on psychotropic.code_set_name = 'psychotropic_medication_usage'
    and upper(c.psych_med) = psychotropic.code
left join {{ ref('iapt_code_lookup') }} as integrated_ltc
    on integrated_ltc.code_set_name = 'integrated_ltc_service_indicator'
    and upper(c.iaptltc_service_ind) = integrated_ltc.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as planned
    on planned.code_set_name = 'planned_care_contact_indicator'
    and upper(c.planned_care_cont_indicator) = planned.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as medium
    on medium.code_set_name = 'consultation_medium_used'
    and upper(c.cons_medium_used) = medium.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as therapy_mode
    on therapy_mode.code_set_name = 'patient_therapy_mode'
    and upper(c.care_cont_patient_ther_mode) = therapy_mode.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as interpreter
    on interpreter.code_set_name = 'interpreter_present_indicator'
    and upper(c.interpreter_present_ind) = interpreter.code
left join {{ ref('consultation_mechanism') }} as mechanism
    on upper(c.cons_mechanism) = mechanism.code
-- The warehouse can hold a v2.0 consultation medium code in the mechanism field. Use the medium list only when
-- the mechanism list has no match and both fields carry the same code.
left join {{ ref('mhsds_care_contact_code_lookup') }} as mechanism_as_medium
    on mechanism.code is null
    and upper(c.cons_mechanism) = upper(c.cons_medium_used)
    and mechanism_as_medium.code_set_name = 'consultation_medium_used'
    and upper(c.cons_mechanism) = mechanism_as_medium.code
left join {{ ref('activity_location_type') }} as location
    on upper(c.act_loc_type_code) = location.code
left join {{ ref('language') }} as treatment_language
    on upper(c.language_code_treat) = treatment_language.code
left join {{ ref('organisation') }} as site
    on c.site_id_of_treat = site.organisation_code
left join {{ ref('organisation') }} as provider
    on c.provider_organisation_code = provider.organisation_code
left join {{ ref('organisation') }} as commissioner
    on c.org_id_comm = commissioner.organisation_code
left join {{ ref('organisation') }} as icb
    on c.dm_icb_commissioner = icb.organisation_code
left join {{ ref('organisation') }} as sub_icb
    on c.dm_sub_icb_commissioner = sub_icb.organisation_code
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_icb
    on c.dm_icb_commissioner = upper(wnl_icb.commissioner_code)
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_sub_icb
    on c.dm_sub_icb_commissioner = upper(wnl_sub_icb.commissioner_code)
left join {{ ref('wnl_commissioner_icb_lookup') }} as wnl_submitted
    on c.org_id_comm = upper(wnl_submitted.commissioner_code)
