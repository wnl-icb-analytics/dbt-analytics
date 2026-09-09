{{ config(materialized='table', cluster_by=['person_id']) }}

/*
Per-child immunisation dose counts inside the NICE age windows, one row per
currently registered child under 20 in int_childhood_imms_current_population.
Doses come from the childhood immunisation event model, counting administered
events (administration codes and dispensed vaccine orders) by vaccine group and
distinct date, so schedule variants recorded against the same visit count once.
Contraindication flags come from contraindicated event codes for the group
and are false when nothing is recorded. Age windows include the lower bound
and exclude the upper one.

Groups: DTAP_PRIMARY (6-in-1), DTAP_BOOSTER (4-in-1 preschool), MMR (MMR and
MMRV), ROTAVIRUS and MENB. Windows are measured from the approximate birth date.
*/

WITH population AS (
    SELECT person_id, birth_date_approx::DATE AS birth_date_approx
    FROM {{ ref('int_childhood_imms_current_population') }}
),

events AS (
    SELECT
        e.person_id,
        e.event_date::DATE AS event_date,
        e.event_type,
        CASE
            WHEN e.vaccine_id LIKE '6IN1%' THEN 'DTAP_PRIMARY'
            WHEN e.vaccine_id LIKE '4IN1%' THEN 'DTAP_BOOSTER'
            WHEN e.vaccine_id LIKE 'MMR%' THEN 'MMR'
            WHEN e.vaccine_id LIKE 'ROTA%' THEN 'ROTAVIRUS'
            WHEN e.vaccine_id LIKE 'MENB%' THEN 'MENB'
        END AS vaccine_group
    FROM {{ ref('int_childhood_imms_vaccination_events_current') }} AS e
),

grouped AS (
    SELECT
        p.person_id,
        p.birth_date_approx,
        ev.vaccine_group,
        ev.event_date,
        ev.event_type IN ('Administration', 'Administration_drug') AS is_administered,
        ev.event_type = 'Contraindicated' AS is_contraindicated
    FROM population AS p
    LEFT JOIN events AS ev
        ON p.person_id = ev.person_id
        AND ev.vaccine_group IS NOT NULL
)

SELECT
    person_id,
    birth_date_approx,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'DTAP_PRIMARY'
        AND event_date < DATEADD(month, 8, birth_date_approx) THEN event_date END) AS dtap_doses_by_8_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MMR'
        AND event_date >= DATEADD(month, 12, birth_date_approx)
        AND event_date < DATEADD(month, 18, birth_date_approx)
        THEN event_date END) AS mmr_doses_12_to_18_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MMR'
        AND event_date >= DATEADD(year, 1, birth_date_approx)
        AND event_date < DATEADD(year, 5, birth_date_approx)
        THEN event_date END) AS mmr_doses_1_to_5_years,
    COALESCE(BOOLOR_AGG(is_administered AND vaccine_group = 'DTAP_BOOSTER'
        AND event_date >= DATEADD(year, 1, birth_date_approx)
        AND event_date < DATEADD(year, 5, birth_date_approx)), FALSE) AS has_dtap_booster_1_to_5_years,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'ROTAVIRUS'
        AND event_date < DATEADD(week, 24, birth_date_approx) THEN event_date END) AS rotavirus_doses_by_24_weeks,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 8, birth_date_approx) THEN event_date END) AS menb_doses_by_8_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 18, birth_date_approx) THEN event_date END) AS menb_doses_by_18_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date < DATEADD(month, 12, birth_date_approx) THEN event_date END) AS menb_primary_doses_by_12_months,
    COUNT(DISTINCT CASE WHEN is_administered AND vaccine_group = 'MENB'
        AND event_date >= DATEADD(month, 12, birth_date_approx)
        AND event_date < DATEADD(month, 18, birth_date_approx)
        THEN event_date END) AS menb_booster_doses_12_to_18_months,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group IN ('DTAP_PRIMARY', 'DTAP_BOOSTER')), FALSE) AS has_dtap_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'MMR'), FALSE) AS has_mmr_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'ROTAVIRUS'), FALSE) AS has_rotavirus_contraindication,
    COALESCE(BOOLOR_AGG(is_contraindicated AND vaccine_group = 'MENB'), FALSE) AS has_menb_contraindication
FROM grouped
GROUP BY person_id, birth_date_approx
