{{ config(materialized='table', cluster_by=['person_id']) }}

-- Flu vaccination in the most recently completed NICE August to March season.
SELECT
    person_id,
    season_start_date,
    season_end_date,
    latest_vaccination_date,
    is_laiv
FROM {{ ref('int_nice_flu_season_vaccination_all') }}
WHERE season_year = {{ nice_flu_season_year() }}
