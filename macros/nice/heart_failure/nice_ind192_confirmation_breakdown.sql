{% macro nice_ind192_confirmation_breakdown(reference='current') %}
{% set suffix = '_by_month' if reference == 'by_month' else '' %}
with confirmation as (
    select headline.person_id, headline.reporting_date, headline.diagnosis_date,
        headline.is_in_numerator as has_headline_confirmation,
        max(evidence.clinical_effective_date::date) as latest_completed_confirmation_date
    from {{ ref('fct_person_heart_failure_confirmation_ind192' ~ suffix) }} as headline
    left join {{ ref('int_heart_failure_confirmation_all') }} as evidence
        on headline.person_id = evidence.person_id
        and evidence.is_completed_confirmation
        and {{ nice_ind192_confirmation_window('evidence.clinical_effective_date::date',
            'headline.diagnosis_date', 'headline.reporting_date') }}
    group by headline.person_id, headline.reporting_date, headline.diagnosis_date,
        headline.is_in_numerator
)
select person_id, reporting_date, diagnosis_date, has_headline_confirmation,
    latest_completed_confirmation_date,
    latest_completed_confirmation_date is not null as has_completed_confirmation
from confirmation
{% endmacro %}
