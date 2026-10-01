{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP Contract KPI: Urgent same-day access rate

Percentage of clinically urgent appointments where the patient was seen
on the same day the appointment was booked.

Numerator: Urgent attended appointments booked and seen same day
Denominator: All urgent attended appointments with a booking date

2026/27 GP contract requires practices to provide a same-day response
for all clinically urgent patient requests.
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    COUNT(*) as urgent_attended_total,
    SUM(CASE WHEN is_same_day THEN 1 ELSE 0 END) as urgent_same_day,
    ROUND(
        100.0 * SUM(CASE WHEN is_same_day THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        1
    ) as urgent_same_day_pct
from {{ ref('int_appointment_gp_clinical_recent') }}
where urgency = 'Urgent'
  and is_attended = TRUE
  and datetime_booked IS NOT NULL
group by practice_code, report_month
