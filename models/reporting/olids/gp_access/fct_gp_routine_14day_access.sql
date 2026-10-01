{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP Contract KPI: Routine appointment within 14 days

Percentage of non-clinically-urgent appointments where the patient was
seen within 14 days of booking.

Numerator: Routine attended appointments with booking_to_slot_days <= 14
Denominator: All routine attended appointments with a booking date
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    COUNT(*) as routine_attended_total,
    SUM(CASE WHEN booking_to_slot_days <= 14 THEN 1 ELSE 0 END) as routine_within_14d,
    ROUND(
        100.0 * SUM(CASE WHEN booking_to_slot_days <= 14 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) as routine_within_14d_pct
from {{ ref('int_appointment_gp_clinical_recent') }}
where urgency = 'Routine'
  and is_attended = TRUE
  and booking_to_slot_days IS NOT NULL
group by practice_code, report_month
