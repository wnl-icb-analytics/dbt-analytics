-- Programme check test activity in the inclusive trailing 12 months.
select
    person_id,
    max(event_date) as latest_completed_date,
    true as check_test_completed
from {{ ref('int_ltc_lcs_moc_events') }}
where event_code = 'C'
    and event_date >= dateadd(month, -12, current_date())
group by person_id
