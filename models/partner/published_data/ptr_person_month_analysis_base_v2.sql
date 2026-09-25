{{
    config(
        materialized='table',
        tags=['person', 'population-health'],
        cluster_by=['sk_patient_id'],)
}}

/*
Analysis foundation table providing person-months with demographics and condition flags for population health analytics.

Grain: One row per person per month (active registrations only, last 60 months)

Key Features:
• Person-month grain for time-series analysis
• Demographics from dim_person_demographics_historical via temporal SCD-2 join
• Age calculated dynamically for each analysis month using calculate_age_attributes macro
• Condition flags for all major long-term conditions (has_* and new_* flags)
• New episode detection for incidence analysis
• UK financial year and quarter dimensions

Inclusion Criteria:
• Must have active registration during the month (from dim_person_historical_practice)
• Must have valid birth_date_approx (inherited from dim_person_demographics_historical)
• Must have registration history (inherited from dim_person_demographics_historical)
• Limited to last 60 months (5-year rolling window)

Known Limitations:
• Address/geographic fields use current address for all historical months (address SCD dates not yet populated)

*/
SELECT * FROM {{ref('person_month_analysis_base')}}
