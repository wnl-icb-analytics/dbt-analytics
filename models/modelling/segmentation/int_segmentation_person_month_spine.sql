{{
    config(
        materialized='table',
        cluster_by=['month_end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Monthly population and registration state for historical segmentation.
-- Every person in the current segmentation scope is retained for each of the
-- last 60 completed month-ends. Only active rows are segmented downstream.

WITH month_ends AS (
    SELECT month_end_date
    FROM {{ ref('int_date_spine') }}
    WHERE month_end_date BETWEEN LAST_DAY(DATEADD('month', -60, CURRENT_DATE))
        AND LAST_DAY(DATEADD('month', -1, CURRENT_DATE))
),

population AS (
    SELECT
        person_id,
        sk_patient_id,
        birth_date_approx,
        death_date_approx
    FROM {{ ref('dim_person_demographics') }}
),

person_months AS (
    SELECT
        p.person_id,
        p.sk_patient_id,
        m.month_end_date,
        p.birth_date_approx,
        p.death_date_approx
    FROM population AS p
    CROSS JOIN month_ends AS m
),

monthly_attributes AS (
    SELECT
        pm.person_id,
        pm.sk_patient_id,
        pm.month_end_date,
        pm.birth_date_approx,
        pm.death_date_approx,
        d.gender,
        d.practice_code,
        d.practice_name,
        d.borough_registered,
        d.neighbourhood_registered,
        d.registration_start_date,
        d.registration_end_date
    FROM person_months AS pm
    LEFT JOIN {{ ref('dim_person_demographics_historical') }} AS d
        ON pm.person_id = d.person_id
        AND pm.month_end_date >= d.effective_start_date
        AND (
            d.effective_end_date IS NULL
            OR pm.month_end_date < d.effective_end_date
        )
),

monthly_flags AS (
    SELECT
        *,
        birth_date_approx <= month_end_date AS is_born,
        birth_date_approx <= month_end_date
            AND (
                death_date_approx IS NULL
                OR death_date_approx > month_end_date
            ) AS is_alive,
        -- int_patient_registrations treats an end before its start as open.
        COALESCE(
            registration_start_date <= month_end_date
                AND (
                    registration_end_date IS NULL
                    OR registration_end_date > month_end_date
                    OR registration_end_date < registration_start_date
                ),
            FALSE
        ) AS is_registered
    FROM monthly_attributes
)

SELECT
    person_id,
    sk_patient_id,
    month_end_date,
    is_born,
    is_alive,
    is_registered,
    is_alive AND is_registered AS is_active,
    death_date_approx IS NOT NULL
        AND death_date_approx <= month_end_date AS is_deceased,
    CASE
        WHEN birth_date_approx > month_end_date THEN NULL
        ELSE FLOOR(
            DATEDIFF(
                'month',
                birth_date_approx,
                LEAST(
                    month_end_date,
                    COALESCE(death_date_approx, month_end_date)
                )
            ) / 12
        )
    END AS age,
    birth_date_approx,
    death_date_approx,
    gender,
    practice_code,
    practice_name,
    borough_registered,
    neighbourhood_registered
FROM monthly_flags
