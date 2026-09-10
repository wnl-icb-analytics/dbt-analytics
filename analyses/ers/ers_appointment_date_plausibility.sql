-- Whole-table diagnostic, not a clinical rule for rejecting future appointments.
select count(*) as slots,
    count_if(appointment_at > dateadd('year', 2, current_date())) as scheduled_beyond_two_years,
    count_if(appointment_at < '2015-01-01'::date) as scheduled_before_2015,
    min(appointment_at) as earliest_appointment,
    max(appointment_at) as latest_appointment
from {{ ref('fct_ers_appointment') }}
