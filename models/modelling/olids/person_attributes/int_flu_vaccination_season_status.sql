{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Flu vaccination in the NICE season window, one row per person vaccinated in that
window. The window is the preceding 1 August to 31 March, read as the most
recently completed season: from 1 April, the season that began the previous
August; before that, the one before. nice_flu_season_year() owns the rule. Vaccination
comes from fct_flu_status for the matching flu campaign, counting administered
and LAIV records dated on or after 1 August. People not in that campaign's
population, or with no vaccination record, have no row.
*/

WITH season AS (
    SELECT
        {{ nice_flu_season_year() }} AS season_year
),

season_window AS (
    SELECT
        season_year,
        'Flu ' || season_year || '-' || RIGHT((season_year + 1)::VARCHAR, 2) AS campaign_id,
        DATE_FROM_PARTS(season_year, 8, 1) AS season_start_date,
        DATE_FROM_PARTS(season_year + 1, 3, 31) AS season_end_date
    FROM season
)

SELECT
    status.person_id,
    season_window.campaign_id,
    season_window.season_start_date,
    season_window.season_end_date,
    MAX(status.status_date::DATE) AS latest_vaccination_date,
    BOOLOR_AGG(status.status_type = 'LAIV_ADMINISTERED') AS is_laiv
FROM {{ ref('fct_flu_status') }} AS status
INNER JOIN season_window
    ON status.campaign_id = season_window.campaign_id
WHERE status.status_type IN ('VACCINATION_ADMINISTERED', 'LAIV_ADMINISTERED')
    AND status.status_date::DATE BETWEEN season_window.season_start_date AND season_window.season_end_date
GROUP BY status.person_id, season_window.campaign_id, season_window.season_start_date, season_window.season_end_date
