-- The table's rolling scope is fixed at build time, including across month ends.
select count(*) as invalid_provider_period_count
from {{ ref('dq_person_activity_provider_period') }}
where activity_month < dateadd(month, -24, date_trunc('month', profiled_at)::date)
    or activity_month >= date_trunc('month', profiled_at)::date
    or activity_month <> date_trunc('month', activity_month)::date
    or event_record_count < 0
    or unlinked_event_record_count < 0 or unlinked_event_record_count > event_record_count
    or scheduled_milestone_record_count < 0 or scheduled_milestone_record_count > event_record_count
    or accepted_submission_count < 0 or submitted_contact_record_count < 0
    or first_event_date < activity_month or first_event_date >= dateadd(month, 1, activity_month)
    or last_event_date < first_event_date or last_event_date >= dateadd(month, 1, activity_month)
    or (event_record_count = 0 and (first_event_date is not null or last_event_date is not null))
    or (event_record_count > 0 and (first_event_date is null or last_event_date is null))
having count(*) > 0
