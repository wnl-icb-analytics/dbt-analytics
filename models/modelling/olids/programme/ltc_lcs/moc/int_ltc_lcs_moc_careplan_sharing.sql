-- Programme careplan sharing activity in the inclusive trailing 12 months.
select
    person_id,
    max(event_date) as latest_completed_date,
    true as careplan_sharing_completed
from {{ ref('int_ltc_lcs_moc_events') }}
where event_code = 'S'
    and event_date >= dateadd(month, -12, current_date())
group by person_id
