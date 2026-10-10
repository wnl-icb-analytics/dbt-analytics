-- IDS201 rejects a contact dated outside its file's reporting month (ETOS v2.1.22 IDS201 row 6), so an
-- identifier reused in another month is another contact. The month keeps a Primary file and the Refresh
-- that replaces it on one key.
with contacts as (
    select
        *
        , nullif(ltrim(attend_or_dna_code, '0'), '') as attendance_code_normalised
        -- UKHFD appointment_type codes are two-character; pad so a submitted 2 matches 02.
        , iff(app_type is null, null, lpad(upper(app_type), 2, '0')) as appointment_type_code_normalised
        -- v2.1 replaced consultation medium used with consultation mechanism under the same item, I201070.
        -- Take the field the file's data set version names; the warehouse holds the same value in both.
        , iff(
            dataset_version = '2.0'
            , coalesce(cons_medium_used, cons_mechanism)
            , coalesce(cons_mechanism, cons_medium_used)
        ) as consultation_code
    from {{ ref('stg_iapt_care_contact_history') }}
    qualify row_number() over (
        partition by referral_id, care_contact_id, unique_month_id
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
        -- NHS England treatment counts exclude contacts recording employment support (I101D29).
        , count_if(code_proc_and_proc_status = '1098051000000103') as employment_support_activity_count
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
    {{ dbt_utils.generate_surrogate_key(['c.referral_id', 'c.care_contact_id', 'c.unique_month_id']) }}
        as source_record_id
    , 'IAPT' as source_dataset
    , c.referral_id
    , c.care_contact_id
    , c.local_care_contact_id
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
    , iff(
        c.appointment_type_code_normalised is null
        , null
        , c.appointment_type_code_normalised in ('02', '03', '05')
    ) as is_treatment_appointment_type
    -- ETOS clause [3]: assessment, or assessment and treatment.
    , iff(
        c.appointment_type_code_normalised is null
        , null
        , c.appointment_type_code_normalised in ('01', '03')
    ) as is_assessment_appointment_type
    , c.attend_or_dna_code as attendance_code
    , attendance.description as attendance_name
    , c.is_attended
    , c.is_nhse_attended_or_unplanned
    -- I101D29 contact rule, judged against the same submission's referral; unknown without that referral.
    , iff(
        sr.referral_id is null
        , null
        , c.is_nhse_attended_or_unplanned
            and coalesce(c.appointment_type_code_normalised in ('02', '03', '05'), false)
            and coalesce(ac.employment_support_activity_count, 0) = 0
            and coalesce(
                c.care_cont_date between sr.referral_request_received_date
                    and coalesce(sr.serv_disch_date, c.reporting_period_end_date)
                , false
            )
    ) as is_nhse_treatment_contact
    , c.planned_care_cont_indicator as planned_care_contact_code
    , planned.description as planned_care_contact_name
    , c.cancellation as short_notice_cancellation_code
    , cancellation.description as short_notice_cancellation_name
    , c.clin_cont_dur_of_care_cont as clinical_contact_duration_minutes
    , c.consultation_code as consultation_mechanism_code
    -- Each version is labelled only from its own list: v2.0 consultation medium used, v2.1 consultation mechanism.
    -- A code valid only in the other version keeps no label and groups as unknown, as NHS England's M1009 does.
    , iff(
        consultation_group.code is null
        , null
        , iff(c.dataset_version = '2.0', medium.description, mechanism.description)
    ) as consultation_mechanism_name
    , iff(consultation_group.code is null, null, consultation_group.code_set_name) as consultation_mechanism_code_set
    , case
        when c.consultation_code is null then 'code_missing'
        when consultation_group.code is not null then 'labelled'
        when other_version_consultation_group.code is not null then 'other_version_code'
        else 'code_unmatched'
    end as consultation_mechanism_label_status
    , coalesce(consultation_group.group_code, 'unknown') as consultation_mechanism_group
    , coalesce(consultation_group.group_name, 'Unknown') as consultation_mechanism_group_name
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
    , c.pathway_id
    , c.submission_id
    , c.source_row_id
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
left join {{ ref('iapt_code_group') }} as consultation_group
    on consultation_group.code_set_name
        = iff(c.dataset_version = '2.0', 'consultation_medium_used', 'consultation_mechanism')
    and upper(c.consultation_code) = consultation_group.code
left join {{ ref('iapt_code_group') }} as other_version_consultation_group
    on other_version_consultation_group.code_set_name
        = iff(c.dataset_version = '2.0', 'consultation_mechanism', 'consultation_medium_used')
    and upper(c.consultation_code) = other_version_consultation_group.code
left join {{ ref('stg_iapt_referral_history') }} as sr
    on c.submission_id = sr.submission_id
    and c.referral_id = sr.referral_id
left join {{ ref('fct_iapt_referral') }} as r
    on c.referral_id = r.referral_id
left join {{ ref('attendance_status') }} as attendance
    on c.attendance_code_normalised = attendance.code
left join {{ ref('iapt_code_lookup') }} as appointment_type
    on appointment_type.code_set_name = 'appointment_type'
    and c.appointment_type_code_normalised = appointment_type.code
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
    and upper(c.consultation_code) = medium.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as therapy_mode
    on therapy_mode.code_set_name = 'patient_therapy_mode'
    and upper(c.care_cont_patient_ther_mode) = therapy_mode.code
left join {{ ref('mhsds_care_contact_code_lookup') }} as interpreter
    on interpreter.code_set_name = 'interpreter_present_indicator'
    and upper(c.interpreter_present_ind) = interpreter.code
left join {{ ref('consultation_mechanism') }} as mechanism
    on upper(c.consultation_code) = mechanism.code
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
