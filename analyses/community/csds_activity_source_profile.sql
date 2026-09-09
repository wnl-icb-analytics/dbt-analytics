-- Aggregate-only profile. Compile with dbt; execute in the approved dev target.
-- Counts describe the current accepted source snapshot, not fixed test thresholds.
with team_delivery_keys as (
    select unique_submission_id, unique_service_request_identifier,
        care_professional_team_local_identifier, count(*) as row_count,
        count(distinct hash(person_id, service_or_team_type_referred_to_community_care,
            referral_closure_date, referral_rejection_date, referral_closure_reason,
            referral_rejection_reason, effective_from, reporting_period_end_date,
            unique_care_professional_team_local_identifier)) as content_versions
    from {{ ref('stg_csds_service_type_history') }}
    group by 1, 2, 3
), activity_grain as (
    select unique_submission_id, unique_care_activity_identifier, count(*) as row_count
    from {{ ref('stg_csds_care_activity_history') }} group by 1, 2
), activity_source as (
    select object_construct(
        'activity_source_rows', coalesce(sum(row_count), 0),
        'activity_duplicate_occurrence_keys', count_if(row_count > 1)
    ) as metrics
    from activity_grain
), team_source as (
    select object_construct(
        'team_source_rows', coalesce(sum(row_count), 0),
        'team_duplicate_submission_keys', count_if(row_count > 1),
        'team_duplicate_keys_with_changed_content', count_if(row_count > 1 and content_versions > 1)
    ) as metrics
    from team_delivery_keys
), activity_output as (
    select object_construct(
        'activity_fact_rows', count(*),
        'activity_missing_contact', count_if(not is_submitted_contact_linked),
        'activity_conflicting_known_person', count_if(is_submitted_contact_person_consistent = false),
        'activity_unknown_person_consistency', count_if(is_submitted_contact_person_consistent is null),
        'activity_missing_latest_contact', count_if(not is_contact_linked),
        'activity_latest_contact_person_conflicts', count_if(is_contact_person_consistent = false),
        'activity_latest_contact_person_unknown', count_if(is_contact_person_consistent is null),
        'activity_latest_contact_occurrence_differs', count_if(is_contact_occurrence_consistent = false),
        'activity_negative_duration', count_if(clinical_contact_duration_minutes < 0),
        'activity_duration_over_one_day', count_if(clinical_contact_duration_minutes > 1440),
        'activity_negative_age', count_if(age_at_contact < 0),
        'activity_age_over_120', count_if(age_at_contact > 120),
        'activity_missing_provider_name', count_if(provider_organisation_name is null),
        'activity_unmatched_procedure', count_if(nullif(trim(procedure_code), '') is not null and procedure_name is null),
        'activity_unmatched_finding', count_if(nullif(trim(finding_code), '') is not null and finding_name is null),
        'activity_unmatched_observation', count_if(nullif(trim(observation_code), '') is not null and observation_name is null),
        'activity_historical_unit_alias', count_if(observation_unit_match_type = 'csds_etos_alias'),
        'activity_existing_unit_reference', count_if(observation_unit_name is not null and observation_unit_match_type <> 'csds_etos_alias'),
        'activity_unmatched_observation_unit', count_if(nullif(trim(observation_unit_code), '') is not null and observation_unit_name is null),
        'activity_supplied_contact_age', count(age_at_contact),
        'activity_unmatched_type', count_if(nullif(trim(activity_type_code), '') is not null and activity_type_name is null),
        'activity_unmatched_procedure_scheme', count_if(nullif(trim(procedure_scheme_code), '') is not null and procedure_scheme_name is null),
        'activity_unmatched_finding_scheme', count_if(nullif(trim(finding_scheme_code), '') is not null and finding_scheme_name is null),
        'activity_unmatched_observation_scheme', count_if(nullif(trim(observation_scheme_code), '') is not null and observation_scheme_name is null)
    ) as metrics
    from {{ ref('fct_csds_care_activity') }}
), team_output as (
    select object_construct(
        'team_latest_relationships', count(*),
        'team_unspecified_local_identifier', count_if(local_team_id is null),
        'team_missing_referral', count_if(not is_submitted_referral_linked),
        'team_conflicting_known_person', count_if(is_submitted_referral_person_consistent = false),
        'team_unknown_person_consistency', count_if(is_submitted_referral_person_consistent is null),
        'team_missing_latest_referral', count_if(not is_referral_linked),
        'team_latest_referral_person_conflicts', count_if(is_referral_person_consistent = false),
        'team_latest_referral_person_unknown', count_if(is_referral_person_consistent is null),
        'team_latest_referral_occurrence_differs', count_if(is_referral_occurrence_consistent = false),
        'team_missing_provider_name', count_if(provider_organisation_name is null),
        'team_closure_after_reporting_period', count_if(referral_closure_date > reporting_period_end_date),
        'team_rejection_after_reporting_period', count_if(referral_rejection_date > reporting_period_end_date),
        'team_unmatched_type', count_if(nullif(trim(service_type_code), '') is not null and service_type_name is null),
        'team_unmatched_closure_reason', count_if(nullif(trim(referral_closure_reason_code), '') is not null and referral_closure_reason_name is null),
        'team_unmatched_rejection_reason', count_if(nullif(trim(referral_rejection_reason_code), '') is not null and referral_rejection_reason_name is null)
    ) as metrics
    from {{ ref('fct_csds_referral_service') }}
), all_metrics as (
    select metrics from activity_source
    union all
    select metrics from team_source
    union all
    select metrics from activity_output
    union all
    select metrics from team_output
)
select metric.key::varchar as measure, metric.value::number as record_count
from all_metrics, lateral flatten(input => metrics) as metric
order by measure
