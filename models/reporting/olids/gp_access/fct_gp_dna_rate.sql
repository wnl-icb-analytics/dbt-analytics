{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP appointment DNA (Did Not Attend) rate

Percentage of booked appointments where the patient did not attend.

Numerator: Appointments with is_dna = TRUE
Denominator: In-scope appointments (is_attended OR is_dna only — excludes
            cancelled, rescheduled, and other non-attendance statuses)

DNA rates vary significantly by deprivation and demographics. Join to
practice-level demographics for equity analysis.
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    SUM(CASE WHEN is_attended OR is_dna THEN 1 ELSE 0 END) as total_appointments,
    SUM(CASE WHEN is_attended THEN 1 ELSE 0 END) as attended,
    SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) as dna,
    ROUND(
        100.0 * SUM(CASE WHEN is_dna THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_attended OR is_dna THEN 1 ELSE 0 END), 0),
        1
    ) as dna_rate_pct
from {{ ref('int_appointment_gp_clinical_recent') }}
where is_attended = TRUE or is_dna = TRUE
group by practice_code, report_month
