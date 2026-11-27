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

-- Compare attribution as well as totals, retaining absent or person-inconsistent parents in the null group.
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
    select referral_source_group, referral_age_band, referral_presenting_complaint, {{ column }} as semantic_count
    from semantic_view(
        {{ model }} metrics {{ metric }}
        dimensions referrals.referral_source_group, referrals.referral_age_band, referrals.referral_presenting_complaint
    )
) as s
full outer join (
    select
        r.source_of_referral_group_name
        , r.age_band_nhs_at_referral
        , r.presenting_complaint_higher_category
        , count(*) as row_count
    from {{ ref(domain_model) }} as c
    left join {{ ref('fct_iapt_referral_summary') }} as r
        on c.referral_id = r.referral_id
        and c.person_id = r.person_id
    group by r.source_of_referral_group_name, r.age_band_nhs_at_referral, r.presenting_complaint_higher_category
) as d
    on s.referral_source_group is not distinct from d.source_of_referral_group_name
    and s.referral_age_band is not distinct from d.age_band_nhs_at_referral
    and s.referral_presenting_complaint is not distinct from d.presenting_complaint_higher_category
where s.semantic_count is distinct from d.row_count
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
