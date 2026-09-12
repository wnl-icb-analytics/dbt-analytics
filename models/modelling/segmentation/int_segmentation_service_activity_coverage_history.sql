{{
    config(
        materialized='table',
        tags=['monthly-full']
    )
}}

-- Month-level source coverage and reporting lag for historical segmentation.

WITH reference_months AS (
    SELECT DISTINCT month_end_date AS end_date
    FROM {{ ref('int_segmentation_person_month_spine') }}
),

date_bounds AS (
    SELECT
        DATEADD('month', -12, MIN(end_date)) AS earliest_activity_date,
        MAX(end_date) AS latest_end_date
    FROM reference_months
),

community_source_months AS (
    SELECT
        DATE_TRUNC('month', CAST(care_contact_date AS DATE)) AS activity_month,
        MIN(CAST(care_contact_date AS DATE)) AS first_date_in_month,
        MAX(CAST(care_contact_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_csds_contact_currency') }}
    WHERE
        care_contact_date IS NOT NULL
        AND attendance_status IN ('5', '6')
    GROUP BY DATE_TRUNC('month', CAST(care_contact_date AS DATE))
),

community_source_bounds AS (
    SELECT MIN(first_date_in_month) AS source_first_date
    FROM community_source_months
),

-- Latest activity date in each source, by month. Every source except CSDS is
-- read over the date range the rolling windows can reach, which is the same
-- range the activity models themselves read. Attendance filters match the
-- measure each source feeds, so the lag describes counted activity.
source_months AS (
    SELECT
        'community' AS source_name,
        activity_month,
        last_date_in_month
    FROM community_source_months

    UNION ALL

    SELECT
        'gp' AS source_name,
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_appointment_gp_clinical') }}
    WHERE
        is_attended
        AND CAST(start_date AS DATE)
            >= (SELECT earliest_activity_date FROM date_bounds)
        AND CAST(start_date AS DATE)
            <= (SELECT latest_end_date FROM date_bounds)
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))

    UNION ALL

    SELECT
        'outpatient' AS source_name,
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_sus_op_appointment') }}
    WHERE
        appointment_attended_or_dna IN ('5', '6')
        AND CAST(start_date AS DATE)
            >= (SELECT earliest_activity_date FROM date_bounds)
        AND CAST(start_date AS DATE)
            <= (SELECT latest_end_date FROM date_bounds)
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))

    UNION ALL

    SELECT
        'apc' AS source_name,
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_sus_apc_encounter') }}
    WHERE
        CAST(start_date AS DATE)
            >= (SELECT earliest_activity_date FROM date_bounds)
        AND CAST(start_date AS DATE)
            <= (SELECT latest_end_date FROM date_bounds)
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))

    UNION ALL

    SELECT
        'ed' AS source_name,
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_sus_uec_encounter') }}
    WHERE
        CAST(start_date AS DATE)
            >= (SELECT earliest_activity_date FROM date_bounds)
        AND CAST(start_date AS DATE)
            <= (SELECT latest_end_date FROM date_bounds)
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))

    UNION ALL

    SELECT
        'mh' AS source_name,
        DATE_TRUNC('month', CAST(start_date AS DATE)) AS activity_month,
        MAX(CAST(start_date AS DATE)) AS last_date_in_month
    FROM {{ ref('int_mhsds_spell_encounters') }}
    WHERE
        CAST(start_date AS DATE)
            >= (SELECT earliest_activity_date FROM date_bounds)
        AND CAST(start_date AS DATE)
            <= (SELECT latest_end_date FROM date_bounds)
    GROUP BY DATE_TRUNC('month', CAST(start_date AS DATE))
),

source_reference_dates AS (
    SELECT
        r.end_date,
        MAX(IFF(s.source_name = 'community', s.last_date_in_month, NULL))
            AS community_window_end_date,
        MAX(IFF(s.source_name = 'gp', s.last_date_in_month, NULL))
            AS gp_latest_activity_date,
        MAX(IFF(s.source_name = 'outpatient', s.last_date_in_month, NULL))
            AS outpatient_latest_activity_date,
        MAX(IFF(s.source_name = 'apc', s.last_date_in_month, NULL))
            AS apc_latest_activity_date,
        MAX(IFF(s.source_name = 'ed', s.last_date_in_month, NULL))
            AS ed_latest_activity_date,
        MAX(IFF(s.source_name = 'mh', s.last_date_in_month, NULL))
            AS mh_latest_activity_date
    FROM reference_months AS r
    LEFT JOIN source_months AS s
        ON s.activity_month <= DATE_TRUNC('month', r.end_date)
    GROUP BY r.end_date
)

SELECT
    r.end_date,
    b.source_first_date AS community_source_first_date,
    r.community_window_end_date,
    DATEADD('month', -12, r.community_window_end_date)
        AS community_window_start_date,
    COALESCE(
        DATEADD('month', -12, r.community_window_end_date)
            >= b.source_first_date,
        FALSE
    ) AS is_community_window_complete,
    DATEDIFF('day', r.community_window_end_date, r.end_date)
        AS community_lag_days,
    r.gp_latest_activity_date,
    DATEDIFF('day', r.gp_latest_activity_date, r.end_date) AS gp_lag_days,
    r.outpatient_latest_activity_date,
    DATEDIFF('day', r.outpatient_latest_activity_date, r.end_date)
        AS outpatient_lag_days,
    r.apc_latest_activity_date,
    DATEDIFF('day', r.apc_latest_activity_date, r.end_date) AS apc_lag_days,
    r.ed_latest_activity_date,
    DATEDIFF('day', r.ed_latest_activity_date, r.end_date) AS ed_lag_days,
    r.mh_latest_activity_date,
    DATEDIFF('day', r.mh_latest_activity_date, r.end_date) AS mh_lag_days
FROM source_reference_dates AS r
CROSS JOIN community_source_bounds AS b
