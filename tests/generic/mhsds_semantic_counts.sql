{% test mhsds_semantic_counts(model) %}

select 'people' as entity, s.person_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics people.person_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_person_summary') }}) as d
where s.person_count <> d.row_count

union all

select 'referrals' as entity, s.referral_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics referrals.referral_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_referral_summary') }}) as d
where s.referral_count <> d.row_count

union all

select 'contacts' as entity, s.contact_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics contacts.contact_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_care_contact') }}) as d
where s.contact_count <> d.row_count

union all

select 'spells' as entity, s.recorded_spell_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics spells.recorded_spell_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_hospital_provider_spell') }}) as d
where s.recorded_spell_count <> d.row_count

union all

select 'occupancy' as entity, s.occupancy_interval_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics occupancy.occupancy_interval_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_inpatient_occupancy') }}) as d
where s.occupancy_interval_count <> d.row_count

union all

select 'diagnoses' as entity, s.diagnosis_record_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics diagnoses.diagnosis_record_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_diagnosis') }}) as d
where s.diagnosis_record_count <> d.row_count

union all

select 'assessments' as entity, s.assessment_observation_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics assessments.assessment_observation_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_assessment_observation') }}) as d
where s.assessment_observation_count <> d.row_count

union all

select 'legal_status' as entity, s.legal_status_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics legal_status.legal_status_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_mental_health_act_period') }}) as d
where s.legal_status_period_count <> d.row_count

union all

select 'contacts_by_person_state' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(contact_count) as semantic_count
    from semantic_view(
        {{ model }}
        metrics contacts.contact_count
        dimensions people.is_currently_detained
    )
) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_care_contact') }}) as d
where s.semantic_count <> d.row_count

union all

select 'diagnoses_by_person_state' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(diagnosis_record_count) as semantic_count
    from semantic_view(
        {{ model }}
        metrics diagnoses.diagnosis_record_count
        dimensions people.is_currently_detained
    )
) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_diagnosis') }}) as d
where s.semantic_count <> d.row_count


union all

select 'demographics' as entity, s.person_provider_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics demographics.person_provider_period_count) as s
cross join (select count(*) as row_count from {{ ref('dim_mhsds_person_provider_period') }}) as d
where s.person_provider_period_count <> d.row_count

union all

select 'referral_periods' as entity, s.referral_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics referral_periods.referral_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_referral_period') }}) as d
where s.referral_period_count <> d.row_count

union all

select 'caseload_referrals' as entity, s.current_caseload_referral_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics caseload_referrals.current_caseload_referral_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_current_caseload_referral') }}) as d
where s.current_caseload_referral_count <> d.row_count

union all

select 'caseload_people' as entity, s.current_caseload_person_provider_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics caseload_people.current_caseload_person_provider_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_current_caseload_person') }}) as d
where s.current_caseload_person_provider_count <> d.row_count

union all

select 'rtt' as entity, s.rtt_evidence_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics rtt.rtt_evidence_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_referral_to_treatment_period') }}) as d
where s.rtt_evidence_count <> d.row_count

union all

select 'indirect_activity' as entity, s.indirect_activity_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics indirect_activity.indirect_activity_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_indirect_activity') }}) as d
where s.indirect_activity_count <> d.row_count

union all

select 'group_sessions' as entity, s.group_session_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics group_sessions.group_session_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_group_session') }}) as d
where s.group_session_count <> d.row_count

union all

select 'group_therapy' as entity, s.group_therapy_contact_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics group_therapy.group_therapy_contact_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_group_therapy_contact') }}) as d
where s.group_therapy_contact_count <> d.row_count

union all

select 'drop_ins' as entity, s.drop_in_contact_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics drop_ins.drop_in_contact_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_drop_in_contact') }}) as d
where s.drop_in_contact_count <> d.row_count

union all

select 'accommodation' as entity, s.accommodation_evidence_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics accommodation.accommodation_evidence_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_accommodation_observation') }}) as d
where s.accommodation_evidence_count <> d.row_count

union all

select 'employment' as entity, s.employment_evidence_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics employment.employment_evidence_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_employment_observation') }}) as d
where s.employment_evidence_count <> d.row_count

union all

select 'disability' as entity, s.disability_evidence_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics disability.disability_evidence_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_disability_observation') }}) as d
where s.disability_evidence_count <> d.row_count

union all

select 'social_circumstances' as entity, s.social_circumstance_evidence_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics social_circumstances.social_circumstance_evidence_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_social_circumstance_observation') }}) as d
where s.social_circumstance_evidence_count <> d.row_count

union all

select 'care_plans' as entity, s.care_plan_snapshot_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics care_plans.care_plan_snapshot_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_care_plan_period') }}) as d
where s.care_plan_snapshot_count <> d.row_count

union all

select 'care_plan_agreements' as entity, s.care_plan_agreement_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics care_plan_agreements.care_plan_agreement_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_care_plan_agreement') }}) as d
where s.care_plan_agreement_count <> d.row_count

union all

select 'complaints' as entity, s.presenting_complaint_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics complaints.presenting_complaint_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_presenting_complaint') }}) as d
where s.presenting_complaint_count <> d.row_count

union all

select 'assessment_instances' as entity, s.assessment_instance_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics assessment_instances.assessment_instance_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_assessment_instance') }}) as d
where s.assessment_instance_count <> d.row_count

union all

select 'score_changes' as entity, s.score_change_pair_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics score_changes.score_change_pair_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_assessment_score_change') }}) as d
where s.score_change_pair_count <> d.row_count

union all

select 'ctos' as entity, s.community_treatment_order_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics ctos.community_treatment_order_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_community_treatment_order') }}) as d
where s.community_treatment_order_count <> d.row_count

union all

select 'recalls' as entity, s.cto_recall_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics recalls.cto_recall_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_community_treatment_order_recall') }}) as d
where s.cto_recall_count <> d.row_count

union all

select 'restrictive_incidents' as entity, s.restrictive_incident_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics restrictive_incidents.restrictive_incident_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_restrictive_intervention_incident') }}) as d
where s.restrictive_incident_count <> d.row_count

union all

select 'restrictive_types' as entity, s.restrictive_type_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics restrictive_types.restrictive_type_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_restrictive_intervention_type') }}) as d
where s.restrictive_type_count <> d.row_count

union all

select 'home_leave' as entity, s.home_leave_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics home_leave.home_leave_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_home_leave') }}) as d
where s.home_leave_period_count <> d.row_count

union all

select 'leave' as entity, s.leave_of_absence_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics leave.leave_of_absence_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_leave_of_absence') }}) as d
where s.leave_of_absence_period_count <> d.row_count

union all

select 'awol' as entity, s.absence_without_leave_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics awol.absence_without_leave_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_absence_without_leave') }}) as d
where s.absence_without_leave_count <> d.row_count

union all

select 'spell_commissioners' as entity, s.spell_commissioner_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics spell_commissioners.spell_commissioner_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_spell_commissioner_period') }}) as d
where s.spell_commissioner_period_count <> d.row_count

union all

select 'readiness' as entity, s.discharge_readiness_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics readiness.discharge_readiness_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_discharge_readiness_period') }}) as d
where s.discharge_readiness_period_count <> d.row_count

union all

select 'ward_capacity' as entity, s.ward_capacity_period_count as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics ward_capacity.ward_capacity_period_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_ward_capacity_period') }}) as d
where s.ward_capacity_period_count <> d.row_count

union all
select 'latest_provider_caseload', s.latest_provider_open_referral_count, d.row_count
from semantic_view({{ model }} metrics latest_provider_caseload.latest_provider_open_referral_count) as s
cross join (select count(*) as row_count from {{ ref('fct_mhsds_latest_provider_caseload_referral') }}) as d
where s.latest_provider_open_referral_count <> d.row_count

{% for alias, metric, domain_model in [
    ('referral_periods', 'referral_period_count', 'fct_mhsds_referral_period'),
    ('caseload_referrals', 'current_caseload_referral_count', 'fct_mhsds_current_caseload_referral'),
    ('caseload_people', 'current_caseload_person_provider_count', 'fct_mhsds_current_caseload_person'),
    ('indirect_activity', 'indirect_activity_count', 'fct_mhsds_indirect_activity'),
    ('care_plans', 'care_plan_snapshot_count', 'fct_mhsds_care_plan_period')
] %}
union all
select '{{ alias }}_period_demographics', s.row_count, d.row_count
from (
    select sum({{ metric }}) as row_count
    from semantic_view({{ model }}
        metrics {{ alias }}.{{ metric }}
        dimensions demographics.demographics_ethnicity)
) as s
cross join (select count(*) as row_count from {{ ref(domain_model) }}) as d
where s.row_count <> d.row_count
{% endfor %}

{% endtest %}
