-- Programme review activity in the inclusive trailing 12 months.
select
    person_id,
    max(event_date) as latest_completed_date,
    true as review_completed
from {{ ref('int_ltc_lcs_moc_events') }}
where event_code = 'M'
    and event_date >= dateadd(month, -12, current_date())
group by person_id
