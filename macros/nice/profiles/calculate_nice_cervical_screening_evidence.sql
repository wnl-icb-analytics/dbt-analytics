{% macro calculate_nice_cervical_screening_evidence(reference='current') %}
{#-
    Calculate cervical screening and pregnancy evidence at each reference date.
    Args: reference is current or by_month.
    Returns: one active, non-test female person aged 25-64 per reporting_date,
             screening dates and programme status, cervix-removal first date,
             pregnancy/delivery dates and current-pregnancy flag.
-#}
-- NICE cervical evidence for general and SMI screening measures, with clinical dates capped at R.
WITH candidates AS (
    SELECT
        person_id,
        reporting_date,
        age
    FROM ({{ nice_reference_population(reference) }})
    WHERE gender = 'Female'
        AND age BETWEEN 25 AND 64
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM candidates
),

screening_daily AS (
    SELECT
        screening.person_id,
        screening.clinical_effective_date::DATE AS event_date,
        MAX(screening.clinical_effective_date) AS latest_screening_date,
        MAX(IFF(screening.is_completed_screening, screening.clinical_effective_date, NULL)) AS completed_date,
        MAX(IFF(screening.is_unsuitable_screening, screening.clinical_effective_date, NULL)) AS unsuitable_date,
        MAX(IFF(screening.is_declined_screening, screening.clinical_effective_date, NULL)) AS declined_date,
        MAX(IFF(screening.is_non_response_screening, screening.clinical_effective_date, NULL)) AS non_response_date
    FROM {{ ref('int_cervical_screening_all') }} AS screening
    INNER JOIN candidate_keys AS candidate
        ON screening.person_id = candidate.person_id
    GROUP BY screening.person_id, screening.clinical_effective_date::DATE
),

screening_state AS (
    SELECT
        person_id,
        event_date,
        latest_screening_date,
        -- Independent running maxima retain completed screens after later declined or unsuitable records.
        MAX(completed_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS latest_completed_date,
        MAX(unsuitable_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS latest_unsuitable_date,
        MAX(declined_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS latest_declined_date,
        MAX(non_response_date) OVER (
            PARTITION BY person_id ORDER BY event_date ROWS UNBOUNDED PRECEDING
        ) AS latest_non_response_date
    FROM screening_daily
),

screening_at_reference AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        candidate.age,
        state.latest_screening_date,
        state.latest_completed_date,
        state.latest_unsuitable_date,
        state.latest_declined_date,
        state.latest_non_response_date
    FROM candidates AS candidate
    ASOF JOIN screening_state AS state
        MATCH_CONDITION (candidate.reporting_date >= state.event_date)
        ON candidate.person_id = state.person_id
),

pregnancy_daily AS (
    SELECT
        pregnancy.person_id,
        -- The existing point-date rule compares timestamps with midnight at R.
        IFF(pregnancy.clinical_effective_date = pregnancy.clinical_effective_date::DATE,
            pregnancy.clinical_effective_date::DATE,
            DATEADD(day, 1, pregnancy.clinical_effective_date::DATE)) AS eligible_date,
        pregnancy.is_pregnancy_code,
        MAX(pregnancy.clinical_effective_date) AS clinical_effective_date
    FROM {{ ref('int_pregnancy_observations_all') }} AS pregnancy
    INNER JOIN candidate_keys AS candidate
        ON pregnancy.person_id = candidate.person_id
    WHERE pregnancy.is_pregnancy_code
        OR pregnancy.is_delivery_outcome_code
    GROUP BY pregnancy.person_id, eligible_date, pregnancy.is_pregnancy_code
),

pregnancy_at_reference AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        pregnancy.clinical_effective_date AS latest_pregnancy_date
    FROM candidates AS candidate
    ASOF JOIN (SELECT * FROM pregnancy_daily WHERE is_pregnancy_code) AS pregnancy
        MATCH_CONDITION (candidate.reporting_date >= pregnancy.eligible_date)
        ON candidate.person_id = pregnancy.person_id
),

delivery_at_reference AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        delivery.clinical_effective_date AS latest_delivery_date
    FROM candidates AS candidate
    ASOF JOIN (SELECT * FROM pregnancy_daily WHERE NOT is_pregnancy_code) AS delivery
        MATCH_CONDITION (candidate.reporting_date >= delivery.eligible_date)
        ON candidate.person_id = delivery.person_id
),

cervix_removal AS (
    SELECT
        person_id,
        MIN(clinical_effective_date) AS first_cervix_removal_date
    FROM {{ ref('int_cervix_removal_all') }}
    GROUP BY person_id
)

SELECT
    screening.person_id,
    screening.reporting_date,
    screening.latest_completed_date,
    screening.latest_screening_date,
    CASE
        WHEN screening.latest_unsuitable_date IS NOT NULL
            AND (screening.latest_completed_date IS NULL
                OR screening.latest_unsuitable_date > screening.latest_completed_date) THEN 'Unsuitable'
        WHEN screening.latest_completed_date IS NULL THEN 'Never Screened'
        WHEN DATEDIFF(day, screening.latest_completed_date, screening.reporting_date)
            <= IFF(screening.age <= 49, 1277, 2007) THEN 'Up to Date'
        ELSE 'Overdue'
    END AS programme_status,
    screening.latest_unsuitable_date,
    screening.latest_declined_date,
    screening.latest_non_response_date,
    IFF(removal.first_cervix_removal_date <= screening.reporting_date,
        removal.first_cervix_removal_date, NULL) AS first_cervix_removal_date,
    pregnancy.latest_pregnancy_date,
    delivery.latest_delivery_date,
    -- Point-date pregnancy_status_history rule, including same-time outcome priority.
    COALESCE(pregnancy.latest_pregnancy_date >= DATEADD(month, -9, screening.reporting_date)
        AND (delivery.latest_delivery_date IS NULL
            OR pregnancy.latest_pregnancy_date > delivery.latest_delivery_date), FALSE) AS is_currently_pregnant
FROM screening_at_reference AS screening
LEFT JOIN cervix_removal AS removal
    ON screening.person_id = removal.person_id
LEFT JOIN pregnancy_at_reference AS pregnancy
    ON screening.person_id = pregnancy.person_id
    AND screening.reporting_date = pregnancy.reporting_date
LEFT JOIN delivery_at_reference AS delivery
    ON screening.person_id = delivery.person_id
    AND screening.reporting_date = delivery.reporting_date
{% endmacro %}
