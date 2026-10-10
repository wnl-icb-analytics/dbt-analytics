{% test iapt_semantic_counts(model) %}

{% set entities = [
    ('referrals', 'referrals.referral_count', 'referral_count', 'fct_iapt_referral_summary'),
    ('contacts', 'contacts.contact_count', 'contact_count', 'fct_iapt_care_contact'),
    ('activities', 'activities.care_activity_count', 'care_activity_count', 'fct_iapt_care_activity'),
    ('assessments', 'assessments.assessment_item_count', 'assessment_item_count', 'fct_iapt_assessment_score'),
    ('conditions', 'conditions.condition_record_count', 'condition_record_count', 'fct_iapt_health_condition'),
    ('referral_periods', 'referral_periods.referral_period_count', 'referral_period_count', 'fct_iapt_referral_period'),
    ('submissions', 'submissions.accepted_submission_count', 'accepted_submission_count', 'dq_iapt_provider_submission'),
] %}

{% for entity, metric, column, domain_model in entities %}
select '{{ entity }}' as entity, s.{{ column }} as semantic_count, d.row_count as domain_count
from semantic_view({{ model }} metrics {{ metric }}) as s
cross join (select count(*) as row_count from {{ ref(domain_model) }}) as d
where s.{{ column }} <> d.row_count
union all
{% endfor %}

-- Referral dimensions must not drop or multiply child facts, including those whose referral is absent.
{% set children = [
    ('contacts_by_referral', 'contacts.contact_count', 'contact_count', 'fct_iapt_care_contact'),
    ('activities_by_referral', 'activities.care_activity_count', 'care_activity_count', 'fct_iapt_care_activity'),
    ('assessments_by_referral', 'assessments.assessment_item_count', 'assessment_item_count', 'fct_iapt_assessment_score'),
    ('conditions_by_referral', 'conditions.condition_record_count', 'condition_record_count', 'fct_iapt_health_condition'),
    ('referral_periods_by_referral', 'referral_periods.referral_period_count', 'referral_period_count', 'fct_iapt_referral_period'),
] %}

{% for entity, metric, column, domain_model in children %}
select '{{ entity }}' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum({{ column }}) as semantic_count
    from semantic_view({{ model }} metrics {{ metric }} dimensions referrals.referral_source_group)
) as s
cross join (select count(*) as row_count from {{ ref(domain_model) }}) as d
where s.semantic_count <> d.row_count
union all
{% endfor %}

-- Submission dimensions must not drop or multiply contacts or referral periods.
select 'contacts_by_submission' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(contact_count) as semantic_count
    from semantic_view({{ model }} metrics contacts.contact_count dimensions submissions.has_refresh_section_shortfall)
) as s
cross join (select count(*) as row_count from {{ ref('fct_iapt_care_contact') }}) as d
where s.semantic_count <> d.row_count

union all

select 'referral_periods_by_submission' as entity, s.semantic_count, d.row_count as domain_count
from (
    select sum(referral_period_count) as semantic_count
    from semantic_view(
        {{ model }} metrics referral_periods.referral_period_count dimensions submissions.has_refresh_section_shortfall
    )
) as s
cross join (select count(*) as row_count from {{ ref('fct_iapt_referral_period') }}) as d
where s.semantic_count <> d.row_count

{% endtest %}
