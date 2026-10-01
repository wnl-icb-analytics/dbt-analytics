{{
    config(
        materialized='table',
        tags=['fact', 'gp_access', 'kpi']
    )
}}

/*
GP appointment volume by contact mode

Tracks digital transformation and access mode trends.
Face-to-face vs telephone vs online vs video by practice and month.

Includes practitioner role mix and average duration for workforce
and costing analysis.
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    contact_mode,
    practitioner_role_group,
    COUNT(*) as appointment_count,
    SUM(CASE WHEN is_attended THEN 1 ELSE 0 END) as attended_count,
    SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) as dna_count,
    ROUND(AVG(duration_minutes), 1) as avg_duration_minutes,
    ROUND(AVG(booking_to_slot_days), 1) as avg_booking_to_slot_days,
    ROUND(AVG(patient_wait_mins), 1) as avg_patient_wait_minutes
from {{ ref('int_appointment_gp_clinical_recent') }}
group by practice_code, report_month, contact_mode, practitioner_role_group
