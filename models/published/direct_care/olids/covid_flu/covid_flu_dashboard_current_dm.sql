{{
    config(
        materialized='view',
        tags=['covid_flu'],
    )
}}


select * 
FROM {{ ref('int_covid_flu_dashboard_current') }}