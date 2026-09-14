-- Scheduled slots by latest recorded appointment action at the current extract cut-off.
-- This is not historical status at the scheduled date, attendance or a DNA-rate denominator.
select date_trunc('month', appointment_at)::date as scheduled_month,
    latest_appointment_action_code, max(latest_appointment_action_name) as latest_appointment_action_name,
    count(*) as planned_slots,
    sum(recorded_booking_action_count) as recorded_booking_actions,
    count_if(recorded_booking_action_count > 1) as slots_with_repeated_booking_actions,
    count_if(recorded_booking_action_count = 0) as slots_without_booking_action,
    count_if(sk_patient_id is null) as slots_without_patient_key
from {{ ref('fct_ers_appointment') }}
group by scheduled_month, latest_appointment_action_code
having count(*) >= 100
order by scheduled_month, latest_appointment_action_code
