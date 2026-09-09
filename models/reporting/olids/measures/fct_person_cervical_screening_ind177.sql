{{ config(materialized='view') }}

-- NICE IND177: https://www.nice.org.uk/indicators/ind177
-- Cervical screening recorded in 5.5 years for women aged 50 to 64; excludes unsuitable (no cervix), non-response to invitations in 12 months and pregnancy.
WITH indicator_population AS (
    SELECT
        demographics.person_id,
        age.age,
        screening.latest_completed_date,
        screening.latest_screening_date,
        screening.programme_status
    FROM {{ ref('dim_person_demographics') }} AS demographics
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON demographics.person_id = age.person_id
    LEFT JOIN {{ ref('fct_cervical_screening_status') }} AS screening
        ON demographics.person_id = screening.person_id
    WHERE demographics.gender = 'Female'
        AND age.age BETWEEN 50 AND 64
        -- No cervix or otherwise unsuitable: any unsuitable screening record
        AND NOT COALESCE(screening.total_unsuitable_records > 0, FALSE)
        -- NICE exclusions: non-response to invitations recorded in 12 months, current pregnancy
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('int_cervical_screening_all') }} AS invite
            WHERE invite.person_id = demographics.person_id
                AND invite.screening_observation_type = 'Non-response to Invitations'
                AND invite.clinical_effective_date::DATE >= DATEADD(month, -12, CURRENT_DATE())
        )
        AND NOT EXISTS (
            SELECT 1 FROM {{ ref('int_pregnancy_observations_all') }} AS pregnancy
            WHERE pregnancy.person_id = demographics.person_id
                AND pregnancy.is_pregnancy_code
                AND pregnancy.clinical_effective_date::DATE >= DATEADD(month, -9, CURRENT_DATE())
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        active.current_practice_code,
        active.current_practice_name,
        population.latest_completed_date,
        population.latest_screening_date,
        population.programme_status,
        CASE WHEN population.latest_completed_date >= DATEADD(month, -66, CURRENT_DATE())
            THEN population.latest_completed_date END AS latest_record_date,
        COALESCE(population.latest_completed_date >= DATEADD(month, -66, CURRENT_DATE()), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    INNER JOIN {{ ref('dim_person_active_patients') }} AS active
        ON population.person_id = active.person_id
)

SELECT
    person_id,
    'IND177' AS indicator_id,
    'Screening: cervical screening (50 to 64 years)' AS indicator_name,
    CURRENT_DATE() AS reporting_date,
    DATEADD(month, -66, CURRENT_DATE()) AS measurement_period_start,
    age,
    'Women aged 50 to 64' AS condition_name,
    current_practice_code,
    current_practice_name,
    latest_completed_date,
    latest_screening_date,
    programme_status,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
