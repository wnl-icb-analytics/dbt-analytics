{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

WITH previous_egfr AS (
    SELECT
        e.person_id,
        e.clinical_effective_date,
        e.egfr_value
    FROM {{ ref('int_egfr_all') }} AS e
    INNER JOIN {{ ref('int_egfr_latest') }} AS le
        ON e.person_id = le.person_id
        AND e.clinical_effective_date::DATE < le.clinical_effective_date::DATE
    WHERE e.is_eligible_egfr = TRUE
    -- Same-day results are one occasion; ordering must match int_egfr_latest.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.person_id
        ORDER BY e.clinical_effective_date::DATE DESC,
            e.date_recorded DESC NULLS LAST, e.id DESC
    ) = 1
),

latest_egfr_pair AS (
    SELECT
        le.person_id,
        le.egfr_value AS latest_egfr_value,
        le.clinical_effective_date AS latest_egfr_date,
        le.ckd_stage AS latest_egfr_stage,
        pe.egfr_value AS previous_egfr_value,
        pe.clinical_effective_date AS previous_egfr_date
    FROM {{ ref('int_egfr_latest') }} AS le
    LEFT JOIN previous_egfr AS pe ON le.person_id = pe.person_id
),

latest_egfr AS (
    SELECT
        *,
        -- CVDP002CKD uses the last two eGFRs, more than three months apart.
        -- https://api.cvdprevent.nhs.uk/indicator/13/details
        COALESCE(
            latest_egfr_value < 60
            AND previous_egfr_value < 60
            AND latest_egfr_date::DATE > DATEADD(month, 3, previous_egfr_date::DATE),
            FALSE
        ) AS has_confirmed_low_egfr
    FROM latest_egfr_pair
),

latest_acr AS (
    SELECT
        person_id,
        acr_value AS latest_acr_value,
        clinical_effective_date AS latest_acr_date,
        acr_category AS latest_acr_stage
    FROM {{ ref('int_urine_acr_latest') }}
),

high_acr_events AS (
    SELECT
        person_id,
        -- Check the full date window, not just the preceding high result.
        COUNT(clinical_effective_date) OVER (
            PARTITION BY person_id
            ORDER BY clinical_effective_date::DATE
            RANGE BETWEEN 730 PRECEDING AND 90 PRECEDING
        ) > 0 AS has_prior_high_acr
    FROM {{ ref('int_urine_acr_all') }}
    WHERE acr_value >= 3
),

acr_confirmation AS (
    SELECT
        person_id,
        BOOLOR_AGG(has_prior_high_acr) AS has_confirmed_high_acr
    FROM high_acr_events
    GROUP BY person_id
),

ckd_lab_classification AS (
    SELECT
        le.latest_egfr_value,
        le.latest_egfr_date,
        le.latest_egfr_stage,
        la.latest_acr_value,
        la.latest_acr_date,
        la.latest_acr_stage,
        COALESCE(le.has_confirmed_low_egfr, FALSE)
            AS has_confirmed_low_egfr,
        COALESCE(ac.has_confirmed_high_acr, FALSE)
            AS has_confirmed_high_acr,
        COALESCE(le.person_id, la.person_id) AS person_id,
        CASE
            WHEN
                le.latest_egfr_stage IS NOT NULL
                AND la.latest_acr_stage IS NOT NULL
                THEN le.latest_egfr_stage || ' ' || la.latest_acr_stage
            WHEN le.latest_egfr_stage IS NOT NULL THEN le.latest_egfr_stage
            WHEN la.latest_acr_stage IS NOT NULL THEN la.latest_acr_stage
        END AS latest_ckd_stage_inferred,
        CASE
            WHEN le.latest_egfr_value < 60 THEN TRUE
            WHEN
                le.latest_egfr_value >= 60 AND la.latest_acr_value >= 3
                THEN TRUE
            ELSE FALSE
        END AS latest_labs_meet_ckd_criteria,
        (
            COALESCE(le.has_confirmed_low_egfr, FALSE)
            OR COALESCE(ac.has_confirmed_high_acr, FALSE)
        ) AS has_confirmed_ckd_by_labs,
        -- The retained 730-day cap is local, not part of CVDPREVENT.
        COALESCE(
            le.has_confirmed_low_egfr
            AND DATEDIFF('day', le.previous_egfr_date, le.latest_egfr_date) <= 730,
            FALSE
        ) AS has_confirmed_low_egfr_within_2_years_local
    FROM latest_egfr AS le
    FULL OUTER JOIN latest_acr AS la ON le.person_id = la.person_id
    LEFT JOIN acr_confirmation AS ac ON la.person_id = ac.person_id
)

SELECT * FROM ckd_lab_classification
