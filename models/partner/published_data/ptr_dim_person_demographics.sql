{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'demographics', 'current_state'],
        cluster_by=['sk_patient_id'],)
}}

/*
Current Person Demographics Dimension Table, with partner row level access policy applied.

Provides current demographic snapshot for ALL persons with registration history.
Built as a thin wrapper over dim_person_demographics_historical, selecting only current periods.

Key Features:

• One row per person (current demographics only)

• Includes ALL persons with registration history (active and inactive)

• Age calculated dynamically for today's date

• is_active flag shows current registration status

• Practice details show latest/current registration

• ESP 2013 weights denormalised via age_band_esp for semantic view compatibility

Data Quality Filters:

• Excludes persons without birth dates (required for age)

• Excludes persons without any registration history

For historical analysis, use dim_person_demographics_historical.
For monthly snapshots, use person_month_analysis_base.
*/

SELECT * FROM {{ref('dim_person_demographics')}}