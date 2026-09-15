{#
    NHS Talking Therapies (IAPT data set) aggregate profile.
    Returns one row per section, entity and measure with a count; no identifiers or code values.
    Compile with `dbt compile -s iapt_data_profile` and run the compiled SQL in Snowflake.
#}

with active as (
    select submission_id, provider_organisation_code, reporting_period_end_date
    from {{ ref('stg_iapt_activesubmission') }}
)

, submissions as (
    select
        'stg_iapt_activesubmission' as entity
        , count(*) as accepted_submissions
        , count(distinct provider_organisation_code, reporting_period_end_date) as provider_periods
        , count_if(provider_organisation_code is null or reporting_period_end_date is null) as missing_header
    from active
)

-- Incremental histories: every retained submission should still be accepted.
, history_submissions as (
    select h.entity
        , count(*) as history_rows
        , count(distinct h.submission_id) as retained_submissions
        , count(distinct iff(a.submission_id is null, h.submission_id, null)) as retained_withdrawn_submissions
    from (
        select 'stg_iapt_referral_history' as entity, submission_id from {{ ref('stg_iapt_referral_history') }}
        union all
        select 'stg_iapt_onward_referral_history', submission_id from {{ ref('stg_iapt_onward_referral_history') }}
        union all
        select 'stg_iapt_care_contact_history', submission_id from {{ ref('stg_iapt_care_contact_history') }}
        union all
        select 'stg_iapt_care_activity_history', submission_id from {{ ref('stg_iapt_care_activity_history') }}
        union all
        select 'stg_iapt_referral_assessment_history', submission_id
        from {{ ref('stg_iapt_referral_assessment_history') }}
        union all
        select 'stg_iapt_activity_assessment_history', submission_id
        from {{ ref('stg_iapt_activity_assessment_history') }}
        union all
        select 'stg_iapt_previous_diagnosis_history', submission_id
        from {{ ref('stg_iapt_previous_diagnosis_history') }}
        union all
        select 'stg_iapt_long_term_condition_history', submission_id
        from {{ ref('stg_iapt_long_term_condition_history') }}
        union all
        select 'stg_iapt_presenting_complaint_history', submission_id
        from {{ ref('stg_iapt_presenting_complaint_history') }}
    ) as h
    left join active as a
        on h.submission_id = a.submission_id
    group by h.entity
)

, referral_history as (
    select submission_id, referral_id, person_id
    from {{ ref('stg_iapt_referral_history') }}
)

, contact_history as (
    select submission_id, unique_month_id, referral_id, care_contact_id, person_id
    from {{ ref('stg_iapt_care_contact_history') }}
)

, grain as (
    select 'referral' as entity, count(*) as accepted_rows, count(distinct referral_id) as distinct_keys
        , count_if(person_id is null) as missing_person_id
    from referral_history
    union all
    select 'care_contact', count(*), count(distinct unique_month_id, referral_id, care_contact_id), count_if(person_id is null)
    from contact_history
    union all
    select 'care_activity', count(*), count(distinct unique_month_id, referral_id, care_contact_id, care_activity_id)
        , count_if(person_id is null)
    from {{ ref('stg_iapt_care_activity_history') }}
)

-- The source rejects repeated onward referral natural keys, yet accepted submissions contain some.
, onward_grain as (
    select
        'onward_referral' as entity
        , count(*) as accepted_rows
        , count(distinct source_row_id) as distinct_source_rows
        , count(distinct onward_referral_id) as distinct_natural_keys
        , count(*) - count(distinct submission_id, onward_referral_id) as same_submission_duplicate_copies
    from {{ ref('stg_iapt_onward_referral_history') }}
)

-- Child to parent within the same accepted submission; staging grain tests keep the parents unique.
, linkage as (
    select
        'care_contact_to_referral' as entity
        , count(*) as child_rows
        , count_if(p.referral_id is null) as parent_missing
        , count_if(p.referral_id is not null and c.person_id is distinct from p.person_id) as person_mismatch
        , 0 as referral_mismatch
    from contact_history as c
    left join referral_history as p
        on c.submission_id = p.submission_id
        and c.referral_id = p.referral_id
    union all
    select
        'care_activity_to_care_contact'
        , count(*)
        , count_if(p.care_contact_id is null)
        , count_if(p.care_contact_id is not null and a.person_id is distinct from p.person_id)
        , count_if(p.care_contact_id is not null and a.referral_id is distinct from p.referral_id)
    from {{ ref('stg_iapt_care_activity_history') }} as a
    left join contact_history as p
        on a.submission_id = p.submission_id
        and a.care_contact_id = p.care_contact_id
    union all
    select
        'onward_referral_to_referral'
        , count(*)
        , count_if(p.referral_id is null)
        , count_if(p.referral_id is not null and o.person_id is distinct from p.person_id)
        , 0
    from {{ ref('stg_iapt_onward_referral_history') }} as o
    left join referral_history as p
        on o.submission_id = p.submission_id
        and o.referral_id = p.referral_id
)

, referral_fact as (
    select
        'fct_iapt_referral' as entity
        , count(*) as fact_rows
        , count(distinct source_record_id) as distinct_keys
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(has_person_identifier_changed) as person_identifier_changed
        , count_if(has_patient_key_changed) as patient_key_changed
        , count_if(referral_received_date is null) as missing_received_date
        , count_if(service_discharge_date is not null) as discharged
        , count_if(referral_status = 'open') as open
        , count_if(referral_end_date_source = 'last_submission') as inferred_closed
        , count_if(discharge_reason_code is not null and discharge_reason_name is null) as unlabelled_discharge_reason
        , count_if(discharge_reason_code_set = 'discharge_reason_legacy') as legacy_labelled_discharge_reason
        , count_if(is_completed_treatment) as completed_treatment
        , count_if(not is_completed_treatment) as not_completed_treatment
        , count_if(provider_organisation_name is null) as missing_provider_name
    from {{ ref('fct_iapt_referral') }}
)

, contact_fact as (
    select
        'fct_iapt_care_contact' as entity
        , count(*) as fact_rows
        , count(distinct source_record_id) as distinct_keys
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(care_contact_date is null) as missing_date
        , count_if(care_contact_time_precision = 'timestamp') as timestamp_precision
        , count_if(care_contact_time_precision = 'date') as date_precision
        , count_if(is_attended) as attended
        , count_if(is_nhse_attended_or_unplanned) as nhse_attended_or_unplanned
        , count_if(appointment_type_code is not null and appointment_type_name is null) as unlabelled_appointment_type
        , count_if(attendance_code is not null and attendance_name is null) as unlabelled_attendance
        , count_if(consultation_mechanism_code is not null and consultation_mechanism_name is null)
            as unlabelled_consultation_mechanism
        , count_if(consultation_mechanism_code_set = 'consultation_medium_used')
            as medium_labelled_consultation_mechanism
        , count_if(activity_location_type_code is not null and activity_location_type_name is null)
            as unlabelled_activity_location
        , count_if(site_code is not null and site_name is null) as unlabelled_site
        , count_if(not is_referral_linked) as referral_not_linked
        , count_if(not is_referral_person_consistent) as referral_person_mismatch
        , count_if(not is_submitted_referral_linked) as submitted_referral_not_linked
        , count_if(not is_submitted_referral_person_consistent) as submitted_referral_person_mismatch
    from {{ ref('fct_iapt_care_contact') }}
)

, onward_fact as (
    select
        'fct_iapt_onward_referral' as entity
        , count(*) as fact_rows
        , count(distinct source_record_id) as distinct_keys
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(onward_referral_date is null) as missing_date
        , count_if(onward_referral_time is null) as missing_time
        , count_if(onward_referral_reason_code is not null and onward_referral_reason_name is null)
            as unlabelled_reason
        , count_if(receiving_organisation_code is not null and receiving_organisation_name is null)
            as unlabelled_receiving_organisation
        , count_if(not is_referral_linked) as referral_not_linked
        , count_if(not is_referral_person_consistent) as referral_person_mismatch
    from {{ ref('fct_iapt_onward_referral') }}
)

, clinical_fact as (
    select 'fct_iapt_care_activity' as entity, count(*) as fact_rows
        , count(distinct source_record_id) as distinct_keys, count_if(sk_patient_id is null) as missing_patient_key
    from {{ ref('fct_iapt_care_activity') }}
    union all
    select 'fct_iapt_assessment_score', count(*), count(distinct source_record_id), count_if(sk_patient_id is null)
    from {{ ref('fct_iapt_assessment_score') }}
    union all
    select 'fct_iapt_health_condition', count(*), count(distinct source_record_id), count_if(sk_patient_id is null)
    from {{ ref('fct_iapt_health_condition') }}
)

, activity_clinical as (
    select
        coalesce(clinical_date_status, 'null') || ':' || coalesce(procedure_expression_type, 'no_procedure') as entity
        , count(*) as fact_rows
        , count_if(asserted_procedure_code is not null) as asserted_procedures
        , count_if(finding_label_status = 'invalid_code_supplied') as invalid_findings
        , count_if(is_finding_code_extended_by_nhsd) as nhsd_extended_findings
        , count_if(is_assessment_observable_in_observation) as assessment_in_observation
        , count_if(is_source_date_inconsistent) as source_date_inconsistent
    from {{ ref('fct_iapt_care_activity') }}
    group by clinical_date_status, procedure_expression_type
)

-- Scores are read against the latest published definition; data set version is not a definition key.
, assessment_status as (
    select
        assessment_source || ':' || assessment_response_status as entity
        , count(*) as fact_rows
        , count_if(assessment_score_numeric is not null) as usable_scores
        , count_if(is_assessment_response_non_score) as non_score_responses
        , count_if(historical_reference_version is not null) as historical_reference
        , count_if(has_excess_score_precision) as excess_precision
        , count_if(clinical_date is null) as undated
    from {{ ref('fct_iapt_assessment_score') }}
    group by assessment_source, assessment_response_status
)

-- DDS moved from a 17-102 total to a 1-6 mean in TOS 2.0.27 (ETOS v2.1.22 Change Control rows 117-119).
, dds_value_bands as (
    select
        case
            when score_numeric_parsed between 1 and 6 then 'mean_range'
            when score_numeric_parsed between 17 and 102 then 'total_range'
            else 'other'
        end || ':' || assessment_response_status as entity
        , count(*) as fact_rows
        , count_if(assessment_score_numeric is not null) as usable_scores
    from {{ ref('fct_iapt_assessment_score') }}
    where assessment_tool_code = '910931000000101'
    group by 1
)

, clinical_record as (
    select
        source_model_name || ':' || clinical_record_type || ':' || coalesce(parent_record_type, 'no_parent') as entity
        , count(*) as view_rows
        , count(*) - count(distinct source_record_id) as duplicate_ids
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(clinical_date is null) as undated
        , count_if(coding_system is null) as missing_coding_system
        , count_if(clinical_code is not null and clinical_description is null) as unlabelled_code
        , count_if(snomed_code is not null) as snomed_candidates
        , count_if(clinical_qualifier_code is not null) as with_qualifier
    from {{ ref('fct_iapt_clinical_record') }}
    group by source_model_name, clinical_record_type, parent_record_type
)

, event_feed as (
    select
        source_record_type || ':' || event_type as entity
        , count(*) as feed_rows
        , count(*) - count(distinct event_id) as duplicate_event_ids
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(event_time_precision = 'timestamp') as timestamp_precision
        , count_if(event_time_precision = 'date') as date_precision
        , count_if(event_time_precision = 'unknown') as unknown_precision
        , count_if(event_date is not null and event_at is null) as dated_without_sort_timestamp
        , count_if(outcome_code is not null) as with_outcome
        , count_if(event_code is not null) as with_event_code
        , count_if(parent_record_id is not null) as with_parent
        , count_if(source_record_type in ('care_contact', 'onward_referral') and parent_record_id is null)
            as parent_withheld_or_missing
    from {{ ref('int_iapt_healthcare_event') }}
    group by source_record_type, event_type
)

, clinical_feed as (
    select
        source_record_type || ':' || clinical_record_type as entity
        , count(*) as feed_rows
        , count(*) - count(distinct clinical_record_id) as duplicate_clinical_record_ids
        , count_if(sk_patient_id is null) as missing_patient_key
        , count_if(clinical_time_precision = 'timestamp') as timestamp_precision
        , count_if(clinical_time_precision = 'date') as date_precision
        , count_if(clinical_time_precision = 'unknown') as unknown_precision
        , count_if(clinical_record_date is not null and clinical_record_at is null) as dated_without_sort_timestamp
        , count_if(mapped_code is not null) as mapped_to_snomed
        , count_if(qualifier_code is not null) as with_qualifier
        , count_if(parent_record_id is null) as without_parent
    from {{ ref('int_iapt_person_clinical_record') }}
    group by source_record_type, clinical_record_type
)

-- Expected clinical items from the latest accepted activity versions, using the activity fact key.
, activity_components as (
    select
        'latest_care_activity' as entity
        , count(*) as latest_activities
        , count_if(code_proc_and_proc_status is not null) as with_procedure
        , count_if(code_find is not null) as with_finding
        , count_if(validated_finding_code = '-3') as nhsd_invalid_finding
        , count_if(code_obs is not null or obs_value is not null) as with_observation
    from (
        select code_proc_and_proc_status, code_find, validated_finding_code, code_obs, obs_value
        from {{ ref('stg_iapt_care_activity_history') }}
        qualify row_number() over (
            partition by referral_id, care_contact_id, unique_month_id, care_activity_id
            order by unique_month_id desc nulls last, source_file_received_at desc nulls last
                , try_to_number(submission_id) desc nulls last, try_to_number(source_row_id) desc nulls last
                , submission_id desc, source_row_id desc
        ) = 1
    )
)

-- Every accepted version is counted, so coverage reflects submitted history.
, submitted_codes as (
    select lower(code_set_name) as code_set_name, code
    from (
        select
            iff(dataset_version = '2.1', source_of_referral_iapt, null) as source_of_referral
            , end_code as discharge_reason
            , prev_diag_cond_ind as previous_diagnosed_condition_indicator
        from {{ ref('stg_iapt_referral_history') }}
    ) unpivot (code for code_set_name in (
        source_of_referral, discharge_reason, previous_diagnosed_condition_indicator
    ))
    union all
    select lower(code_set_name), code
    from (
        select
            app_type as appointment_type
            , psych_med as psychotropic_medication_usage
            , cancellation as short_notice_cancellation_indicator
            , iaptltc_service_ind as integrated_ltc_service_indicator
        from {{ ref('stg_iapt_care_contact_history') }}
    ) unpivot (code for code_set_name in (
        appointment_type, psychotropic_medication_usage, short_notice_cancellation_indicator,
        integrated_ltc_service_indicator
    ))
    union all
    select 'onward_referral_reason', onward_refer_reason
    from {{ ref('stg_iapt_onward_referral_history') }}
    where onward_refer_reason is not null
)

-- Current label, labelled historical list, or no published label.
, label_coverage as (
    select
        c.code_set_name as entity
        , count(*) as coded_rows
        , count_if(l.code is not null) as current_label_rows
        , count_if(legacy.code is not null) as legacy_label_rows
        , count_if(l.code is null and legacy.code is null) as unmatched_rows
        , count(distinct iff(l.code is null and legacy.code is null, c.code, null)) as distinct_unmatched_codes
    from submitted_codes as c
    left join {{ ref('iapt_code_lookup') }} as l
        on c.code_set_name = l.code_set_name
        and upper(trim(c.code)) = l.code
    -- Retired care spell end codes label a discharge reason only when the current list lacks it.
    left join {{ ref('iapt_code_lookup') }} as legacy
        on c.code_set_name = 'discharge_reason'
        and l.code is null
        and legacy.code_set_name = 'discharge_reason_legacy'
        and upper(trim(c.code)) = legacy.code
    group by c.code_set_name
    union all
    -- IAPT v2.0 submitted the mental health source of referral list, which can repeat a code.
    select
        'source_of_referral_mh_v2_0'
        , count(*)
        , count_if(l.code is not null)
        , 0
        , count_if(l.code is null)
        , count(distinct iff(l.code is null, r.source_of_referral_mh, null))
    from {{ ref('stg_iapt_referral_history') }} as r
    left join (select distinct code from {{ ref('mhsds_source_of_referral') }}) as l
        on upper(trim(r.source_of_referral_mh)) = l.code
    where r.dataset_version = '2.0' and r.source_of_referral_mh is not null
    union all
    -- Use the fact's version-aware consultation labels rather than repeat the vocabulary rule.
    select
        'consultation_mechanism'
        , count(*)
        , count_if(consultation_mechanism_code_set = 'consultation_mechanism')
        , count_if(consultation_mechanism_code_set = 'consultation_medium_used')
        , count_if(consultation_mechanism_name is null)
        , count(distinct iff(consultation_mechanism_name is null, consultation_mechanism_code, null))
    from {{ ref('fct_iapt_care_contact') }}
    where consultation_mechanism_code is not null
)

-- Broad provider/year completeness bands avoid publishing small provider cells.
, completeness_by_provider as (
    select 'referral_source' as field_name, year(referral_received_date) as record_year
        , provider_organisation_code, count(*) as records
        , count_if(source_of_referral_code is null) as missing_records
    from {{ ref('fct_iapt_referral') }}
    group by record_year, provider_organisation_code
    union all
    select 'discharge_reason', year(service_discharge_date), provider_organisation_code
        , count(*), count_if(discharge_reason_code is null)
    from {{ ref('fct_iapt_referral') }}
    where service_discharge_date is not null
    group by year(service_discharge_date), provider_organisation_code
    union all
    select 'consultation_mechanism', year(care_contact_date), provider_organisation_code
        , count(*), count_if(consultation_mechanism_code is null)
    from {{ ref('fct_iapt_care_contact') }}
    group by year(care_contact_date), provider_organisation_code
)

, completeness as (
    select field_name || '_' || coalesce(record_year::varchar, 'undated') as entity
        , sum(records) as records, sum(missing_records) as missing_records
        , count(*) as providers
        , count_if(missing_records >= 0.9 * records) as providers_at_least_90_percent_missing
        , sum(iff(missing_records >= 0.9 * records, records, 0)) as records_in_those_providers
    from completeness_by_provider
    group by field_name, record_year
)

, results as (
    select 'completeness' as section, object_construct_keep_null(*) as metrics from completeness
    union all
    select 'submissions' as section, object_construct_keep_null(*) as metrics from submissions
    union all
    select 'history_submissions' as section, object_construct_keep_null(*) as metrics from history_submissions
    union all
    select 'grain' as section, object_construct_keep_null(*) as metrics from grain
    union all
    select 'grain' as section, object_construct_keep_null(*) as metrics from onward_grain
    union all
    select 'linkage' as section, object_construct_keep_null(*) as metrics from linkage
    union all
    select 'fact' as section, object_construct_keep_null(*) as metrics from referral_fact
    union all
    select 'fact' as section, object_construct_keep_null(*) as metrics from contact_fact
    union all
    select 'fact' as section, object_construct_keep_null(*) as metrics from onward_fact
    union all
    select 'fact' as section, object_construct_keep_null(*) as metrics from clinical_fact
    union all
    select 'activity_clinical' as section, object_construct_keep_null(*) as metrics from activity_clinical
    union all
    select 'assessment_status' as section, object_construct_keep_null(*) as metrics from assessment_status
    union all
    select 'dds_value_bands' as section, object_construct_keep_null(*) as metrics from dds_value_bands
    union all
    select 'clinical_record' as section, object_construct_keep_null(*) as metrics from clinical_record
    union all
    select 'event_feed' as section, object_construct_keep_null(*) as metrics from event_feed
    union all
    select 'clinical_feed' as section, object_construct_keep_null(*) as metrics from clinical_feed
    union all
    select 'activity_components' as section, object_construct_keep_null(*) as metrics from activity_components
    union all
    select 'label_coverage' as section, object_construct_keep_null(*) as metrics from label_coverage
)

-- COUNT and COUNT_IF can have different numeric precision; VARIANT avoids UNPIVOT type conflicts.
select
    r.section
    , r.metrics:ENTITY::varchar as entity
    , lower(m.key) as measure
    , coalesce(m.value::number(38, 0), 0) as n
from results as r,
lateral flatten(input => r.metrics) as m
where m.key <> 'ENTITY'
order by section, entity, measure
