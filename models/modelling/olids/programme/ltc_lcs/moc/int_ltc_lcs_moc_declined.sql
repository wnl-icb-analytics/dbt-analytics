-- Programme declined activity in the inclusive trailing 12 months.
select
    person_id,
    max(event_date) as latest_declined_date,
    true as moc_declined
from {{ ref('int_ltc_lcs_moc_events') }}
where event_code = 'D'
    and event_date >= dateadd(month, -12, current_date())
group by person_id
