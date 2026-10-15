-- Successors cannot contribute waits to a month before they first appeared in accepted submissions.
{% set waits = [
    ('assessment', 'first_assessment_date', 'days_referral_to_first_assessment'),
    ('first_treatment', 'first_treatment_date', 'days_referral_to_first_treatment'),
    ('second_treatment', 'second_treatment_date', 'days_first_to_second_treatment')
] %}

{% for stage, event_date, wait in waits %}
select
    '{{ stage }}_before_successor_submission' as rule_name
    , count(*) as failing_count
from {{ ref('fct_iapt_referral_summary') }}
where is_transfer_successor
    and {{ wait }} is not null
    and datediff(month, {{ event_date }}, first_reported_period_end_date) > 0
having count(*) > 0
{% if not loop.last %}union all{% endif %}
{% endfor %}
