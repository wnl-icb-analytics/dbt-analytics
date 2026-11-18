{{ config(materialized='table', cluster_by=['season_year', 'person_id']) }}

-- NICE flu vaccination evidence at person/season grain, without campaign audit windows.
WITH season_events AS (
    SELECT
        person_id,
        event_date,
        is_laiv,
        YEAR(event_date) - IFF(MONTH(event_date) <= 3, 1, 0) AS season_year
    FROM {{ ref('int_nice_flu_vaccination_all') }}
    -- April to July administrations do not belong to a NICE August to March season.
    WHERE MONTH(event_date) >= 8
        OR MONTH(event_date) <= 3
)

SELECT
    person_id,
    season_year,
    DATE_FROM_PARTS(season_year, 8, 1) AS season_start_date,
    DATE_FROM_PARTS(season_year + 1, 3, 31) AS season_end_date,
    MAX(event_date) AS latest_vaccination_date,
    BOOLOR_AGG(is_laiv) AS is_laiv
FROM season_events
GROUP BY person_id, season_year
