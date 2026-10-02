{% macro calculate_nice_childhood_immunisation_profile(reference='current') %}
{#-
    Calculate vaccine dose counts and contraindication state at each reference date.
    Args: reference is current or by_month; population scope comes from its adapter.
    Returns: person_id, birth_date_approx, reporting_date and the existing vaccine
             dose-count and contraindication fields, one person per date.
-#}
WITH population AS (
    SELECT
        person_id,
        birth_date_approx,
        reporting_date
    FROM ({{ nice_reference_population(reference, scope='childhood') }})
),

birth_dates AS (
    SELECT DISTINCT
        person_id,
        birth_date_approx
    FROM population
),

classified AS (
    SELECT
        p.person_id,
        p.birth_date_approx,
        ev.event_date,
        ev.vaccine_group,
        ev.event_type IN ('Administration', 'Administration_drug') AS is_administered,
        ev.event_type = 'Contraindicated' AS is_contraindicated,
        -- Classify each source age before deduplicating dose dates.
        COALESCE(ev.age_at_event >= 1,
            ev.event_date >= DATEADD(year, 1, p.birth_date_approx)) AS is_on_or_after_first_birthday,
        COALESCE(ev.age_at_event BETWEEN 1 AND 4,
            ev.event_date >= DATEADD(year, 1, p.birth_date_approx)
            AND ev.event_date < DATEADD(year, 5, p.birth_date_approx)) AS is_age_1_to_4,
        COALESCE(ev.age_at_event = 0,
            ev.event_date < DATEADD(year, 1, p.birth_date_approx)) AS is_before_first_birthday,
        COALESCE(ev.age_at_event = 1,
            ev.event_date >= DATEADD(year, 1, p.birth_date_approx)) AS is_age_1
    FROM birth_dates AS p
    INNER JOIN {{ ref('int_nice_childhood_immunisation_all') }} AS ev
        ON p.person_id = ev.person_id
        AND ev.event_date >= DATE_TRUNC('month', p.birth_date_approx)
),

daily AS (
    SELECT
        person_id,
        birth_date_approx,
        event_date,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'DTAP_PRIMARY'
            AND event_date < DATEADD(month, 8, birth_date_approx)
        ), FALSE)::INT AS dtap_doses_by_8_months,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MMR'
            AND is_on_or_after_first_birthday
            AND event_date < DATEADD(month, 18, birth_date_approx)
        ), FALSE)::INT AS mmr_doses_12_to_18_months,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MMR'
            AND is_age_1_to_4
        ), FALSE)::INT AS mmr_doses_1_to_5_years,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'DTAP_BOOSTER'
            AND is_age_1_to_4
        ), FALSE)::INT AS has_dtap_booster_1_to_5_years,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'ROTAVIRUS'
            AND event_date < DATEADD(week, 24, birth_date_approx)
        ), FALSE)::INT AS rotavirus_doses_by_24_weeks,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MENB'
            AND event_date < DATEADD(month, 8, birth_date_approx)
        ), FALSE)::INT AS menb_doses_by_8_months,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MENB'
            AND event_date < DATEADD(month, 18, birth_date_approx)
        ), FALSE)::INT AS menb_doses_by_18_months,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MENB'
            AND is_before_first_birthday
        ), FALSE)::INT AS menb_primary_doses_by_12_months,
        COALESCE(BOOLOR_AGG(
            is_administered
            AND vaccine_group = 'MENB'
            AND is_age_1
            AND event_date < DATEADD(month, 18, birth_date_approx)
        ), FALSE)::INT AS menb_booster_doses_12_to_18_months,
        COALESCE(BOOLOR_AGG(
            is_contraindicated
            AND vaccine_group IN ('DTAP_PRIMARY', 'DTAP_BOOSTER')
        ), FALSE)::INT AS has_dtap_contraindication,
        COALESCE(BOOLOR_AGG(
            is_contraindicated
            AND vaccine_group = 'MMR'
        ), FALSE)::INT AS has_mmr_contraindication,
        COALESCE(BOOLOR_AGG(
            is_contraindicated
            AND vaccine_group = 'ROTAVIRUS'
        ), FALSE)::INT AS has_rotavirus_contraindication,
        COALESCE(BOOLOR_AGG(
            is_contraindicated
            AND vaccine_group = 'MENB'
        ), FALSE)::INT AS has_menb_contraindication
    FROM classified
    GROUP BY person_id, birth_date_approx, event_date
),

cumulative AS (
    -- Each date contributes at most one dose per window, even with several codes.
    SELECT
        person_id,
        birth_date_approx,
        event_date,
        SUM(dtap_doses_by_8_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS dtap_doses_by_8_months,
        SUM(mmr_doses_12_to_18_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS mmr_doses_12_to_18_months,
        SUM(mmr_doses_1_to_5_years) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS mmr_doses_1_to_5_years,
        MAX(has_dtap_booster_1_to_5_years) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS has_dtap_booster_1_to_5_years,
        SUM(rotavirus_doses_by_24_weeks) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS rotavirus_doses_by_24_weeks,
        SUM(menb_doses_by_8_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS menb_doses_by_8_months,
        SUM(menb_doses_by_18_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS menb_doses_by_18_months,
        SUM(menb_primary_doses_by_12_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS menb_primary_doses_by_12_months,
        SUM(menb_booster_doses_12_to_18_months) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS menb_booster_doses_12_to_18_months,
        MAX(has_dtap_contraindication) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS has_dtap_contraindication,
        MAX(has_mmr_contraindication) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS has_mmr_contraindication,
        MAX(has_rotavirus_contraindication) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS has_rotavirus_contraindication,
        MAX(has_menb_contraindication) OVER (
            PARTITION BY person_id, birth_date_approx
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS has_menb_contraindication
    FROM daily
)
SELECT
    p.person_id,
    p.birth_date_approx,
    p.reporting_date,
    COALESCE(e.dtap_doses_by_8_months, 0)::NUMBER(18,0) AS dtap_doses_by_8_months,
    COALESCE(e.mmr_doses_12_to_18_months, 0)::NUMBER(18,0) AS mmr_doses_12_to_18_months,
    COALESCE(e.mmr_doses_1_to_5_years, 0)::NUMBER(18,0) AS mmr_doses_1_to_5_years,
    COALESCE(e.has_dtap_booster_1_to_5_years > 0, FALSE) AS has_dtap_booster_1_to_5_years,
    COALESCE(e.rotavirus_doses_by_24_weeks, 0)::NUMBER(18,0) AS rotavirus_doses_by_24_weeks,
    COALESCE(e.menb_doses_by_8_months, 0)::NUMBER(18,0) AS menb_doses_by_8_months,
    COALESCE(e.menb_doses_by_18_months, 0)::NUMBER(18,0) AS menb_doses_by_18_months,
    COALESCE(e.menb_primary_doses_by_12_months, 0)::NUMBER(18,0) AS menb_primary_doses_by_12_months,
    COALESCE(e.menb_booster_doses_12_to_18_months, 0)::NUMBER(18,0) AS menb_booster_doses_12_to_18_months,
    COALESCE(e.has_dtap_contraindication > 0, FALSE) AS has_dtap_contraindication,
    COALESCE(e.has_mmr_contraindication > 0, FALSE) AS has_mmr_contraindication,
    COALESCE(e.has_rotavirus_contraindication > 0, FALSE) AS has_rotavirus_contraindication,
    COALESCE(e.has_menb_contraindication > 0, FALSE) AS has_menb_contraindication
FROM population p
ASOF JOIN cumulative AS e
    MATCH_CONDITION (p.reporting_date >= e.event_date)
    ON p.person_id = e.person_id
    AND p.birth_date_approx = e.birth_date_approx
{% endmacro %}
