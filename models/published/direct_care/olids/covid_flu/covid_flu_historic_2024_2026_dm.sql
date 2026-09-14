{{
    config(
        materialized='view',
        tags=['covid_flu'],
    )
}}

/*patients registered at the end of 20th Jue 2026 Season End. Not refreshed */
select * 
FROM {{ ref('int_covid_flu_historic_dashboard') }}