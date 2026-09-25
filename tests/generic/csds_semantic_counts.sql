{% test csds_semantic_counts(model) %}

{% set entities = [
    ('people', 'people.person_count', 'person_count', 'fct_csds_person_summary'),
    ('referrals', 'referrals.referral_count', 'referral_count', 'fct_csds_referral_summary'),
    ('contacts', 'contacts.contact_count', 'contact_count', 'fct_csds_care_contact'),
    ('activities', 'activities.care_activity_count', 'care_activity_count', 'fct_csds_care_activity'),
    ('clinical', 'clinical.clinical_record_count', 'clinical_record_count', 'fct_csds_clinical_record'),
    ('referral_periods', 'referral_periods.referral_period_count', 'referral_period_count', 'fct_csds_referral_period'),
    ('caseload_referrals', 'caseload_referrals.current_caseload_referral_count', 'current_caseload_referral_count', 'fct_csds_current_caseload_referral'),
    ('rtt', 'rtt.rtt_evidence_count', 'rtt_evidence_count', 'fct_csds_referral_to_treatment_period'),
    ('demographics', 'demographics.person_provider_period_count', 'person_provider_period_count', 'dim_csds_person_provider_period'),
] %}

{% for entity, metric, column, domain_model in entities %}
select '{{ entity }}' as entity, s.{{ column }} as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics {{ metric }}) as s
cross join (select count(*) as row_count from {{ ref(domain_model) }}) as d
where s.{{ column }} <> d.row_count
union all
{% endfor %}

-- Person dimensions must not drop or multiply facts.
select 'contacts_by_person_caseload' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(contact_count) as semantic_count
    from semantic_view({{ model }} metrics contacts.contact_count dimensions people.has_current_recorded_caseload)
) as s
cross join (select count(*) as row_count from {{ ref('fct_csds_care_contact') }}) as d
where s.semantic_count <> d.row_count

union all

-- Period demographics must not drop or multiply referral periods.
select 'referral_periods_by_demographics' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(referral_period_count) as semantic_count
    from semantic_view({{ model }} metrics referral_periods.referral_period_count dimensions demographics.demographics_gender)
) as s
cross join (select count(*) as row_count from {{ ref('fct_csds_referral_period') }}) as d
where s.semantic_count <> d.row_count

union all

-- Contact period demographics must not drop or multiply contacts.
select 'contacts_by_demographics' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(contact_count) as semantic_count
    from semantic_view({{ model }} metrics contacts.contact_count dimensions demographics.demographics_is_wnl_resident)
) as s
cross join (select count(*) as row_count from {{ ref('fct_csds_care_contact') }}) as d
where s.semantic_count <> d.row_count

{% endtest %}
