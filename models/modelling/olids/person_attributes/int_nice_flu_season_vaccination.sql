{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Flu vaccination in the NICE season window, one row per person vaccinated in
that window. The window is the preceding 1 August to 31 March, read as the most
recently completed season: from 1 April the season that began the previous
August, before that the one before. nice_flu_season_year() owns the rule.
Vaccination is any flu or LAIV administration code (UKHSA FLUVAX_COD, LAIV_COD)
or dispensed vaccine order (FLURX_COD, LAIVRX_COD) dated in the window. The
clusters are read directly rather than through the flu campaign models, whose
audit windows run 1 September to 28 February and are tied to a campaign list.
*/

WITH season AS (
    SELECT
        DATE_FROM_PARTS({{ nice_flu_season_year() }}, 8, 1) AS season_start_date,
        DATE_FROM_PARTS({{ nice_flu_season_year() }} + 1, 3, 31) AS season_end_date
),

vaccination_events AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        cluster_id = 'LAIV_COD' AS is_laiv
    FROM ({{ get_observations("'FLUVAX_COD', 'LAIV_COD'", 'UKHSA_FLU') }})

    UNION ALL

    SELECT
        person_id,
        order_date::DATE AS event_date,
        cluster_id = 'LAIVRX_COD' AS is_laiv
    FROM ({{ get_medication_orders(cluster_id="'FLURX_COD', 'LAIVRX_COD'", source='UKHSA_FLU') }})
)

SELECT
    events.person_id,
    season.season_start_date,
    season.season_end_date,
    MAX(events.event_date) AS latest_vaccination_date,
    BOOLOR_AGG(events.is_laiv) AS is_laiv
FROM vaccination_events AS events
INNER JOIN season
    ON events.event_date BETWEEN season.season_start_date AND season.season_end_date
GROUP BY events.person_id, season.season_start_date, season.season_end_date
