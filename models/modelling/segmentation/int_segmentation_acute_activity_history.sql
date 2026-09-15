{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Exact rolling 12-month GP, ED and non-elective activity at each month-end.

WITH date_bounds AS (
    SELECT
        MIN(month_end_date) AS first_month,
        MAX(month_end_date) AS last_month
    FROM {{ ref('int_segmentation_person_month_spine') }}
),

gp_activity AS (
    SELECT
        person_id,
        CAST(start_date AS DATE) AS activity_date
    FROM {{ ref('int_appointment_gp_clinical') }}
    WHERE
        is_attended
        AND CAST(start_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(start_date AS DATE) <= (SELECT last_month FROM date_bounds)
),

gp_monthly AS (
    SELECT
        person_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        COUNT(*) AS appointment_count
    FROM gp_activity
    GROUP BY person_id, DATE_TRUNC('month', activity_date)
),

gp_full_months AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(g.appointment_count) AS appointment_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN gp_monthly AS g
        ON pm.person_id = g.person_id
        AND g.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND g.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

-- Each window is the whole months after the boundary month, plus the part of
-- the boundary month from the window start onwards. The two branches cover
-- different months, so their counts add without overlapping. The boundary
-- branch has to be a range because the start is not always a month end:
-- 12 months before 28 February in a non-leap year is 28 February in a leap
-- year, so 29 February also falls inside the window.
gp_boundary_partial_month AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(*) AS appointment_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN gp_activity AS g
        ON pm.person_id = g.person_id
        AND g.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

gp_rolling AS (
    SELECT
        COALESCE(f.person_id, b.person_id) AS person_id,
        COALESCE(f.end_date, b.end_date) AS end_date,
        ZEROIFNULL(f.appointment_count) + ZEROIFNULL(b.appointment_count)
            AS gp_appointments_12mo
    FROM gp_full_months AS f
    FULL OUTER JOIN gp_boundary_partial_month AS b
        ON f.person_id = b.person_id AND f.end_date = b.end_date
),

ed_activity AS (
    SELECT DISTINCT
        sk_patient_id,
        visit_occurrence_id,
        CAST(start_date AS DATE) AS activity_date
    FROM {{ ref('int_sus_uec_encounter') }}
    WHERE
        sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
        AND CAST(start_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(start_date AS DATE) <= (SELECT last_month FROM date_bounds)
),

ed_monthly AS (
    SELECT
        sk_patient_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        COUNT(DISTINCT visit_occurrence_id) AS attendance_count
    FROM ed_activity
    GROUP BY sk_patient_id, DATE_TRUNC('month', activity_date)
),

ed_full_months AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(e.attendance_count) AS attendance_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN ed_monthly AS e
        ON pm.sk_patient_id = e.sk_patient_id
        AND e.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND e.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

ed_boundary_partial_month AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(DISTINCT e.visit_occurrence_id) AS attendance_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN ed_activity AS e
        ON pm.sk_patient_id = e.sk_patient_id
        AND e.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

ed_rolling AS (
    SELECT
        COALESCE(f.person_id, b.person_id) AS person_id,
        COALESCE(f.end_date, b.end_date) AS end_date,
        ZEROIFNULL(f.attendance_count) + ZEROIFNULL(b.attendance_count)
            AS ed_attendances_12mo
    FROM ed_full_months AS f
    FULL OUTER JOIN ed_boundary_partial_month AS b
        ON f.person_id = b.person_id AND f.end_date = b.end_date
),

-- The current recent mart reads int_sus_apc_imputed_spells, which emits only
-- two years. The encounter model underneath it has continuous coverage for
-- all 60 historical windows, so history is built from it directly, sharing
-- the mart's admission-method exclusions and spell deduplication key.
--
-- It does not share the mart's date imputation, which fills a missing
-- admission date from the discharge date and duration before deduplicating.
-- So spells with no admission date are excluded here although the mart
-- recovers them, and a missing discharge date leaves the start_date =
-- end_date term of the deduplication key null, letting same-day spells the
-- mart keeps separate collapse into one. Together this affects under 0.1% of
-- in-scope spells and can move nel_admissions_12mo in either direction
-- against fct_person_sus_apc_recent.apc_nel_12mo.
nel_spells_deduplicated AS (
    SELECT
        sk_patient_id,
        visit_occurrence_id,
        start_date,
        start_time,
        end_date,
        end_time,
        organisation_id,
        spell_admission_method
    FROM {{ ref('int_sus_apc_encounter') }}
    WHERE
        sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
        AND spell_admission_method NOT IN ('2C', '82', '31')
        AND CAST(start_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(start_date AS DATE) <= (SELECT last_month FROM date_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            sk_patient_id,
            start_date,
            start_time,
            organisation_id,
            IFF(
                start_date = end_date
                AND (start_time IS NULL OR organisation_id IS NULL),
                visit_occurrence_id,
                NULL
            )
        ORDER BY
            end_date DESC NULLS LAST,
            end_time DESC NULLS LAST,
            visit_occurrence_id DESC
    ) = 1
),

nel_activity AS (
    SELECT
        sk_patient_id,
        visit_occurrence_id,
        CAST(start_date AS DATE) AS activity_date
    FROM nel_spells_deduplicated
    WHERE LEFT(spell_admission_method, 1) = '2'
),

nel_monthly AS (
    SELECT
        sk_patient_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        COUNT(DISTINCT visit_occurrence_id) AS admission_count
    FROM nel_activity
    GROUP BY sk_patient_id, DATE_TRUNC('month', activity_date)
),

nel_full_months AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(n.admission_count) AS admission_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN nel_monthly AS n
        ON pm.sk_patient_id = n.sk_patient_id
        AND n.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND n.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

nel_boundary_partial_month AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(DISTINCT n.visit_occurrence_id) AS admission_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN nel_activity AS n
        ON pm.sk_patient_id = n.sk_patient_id
        AND n.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

nel_rolling AS (
    SELECT
        COALESCE(f.person_id, b.person_id) AS person_id,
        COALESCE(f.end_date, b.end_date) AS end_date,
        ZEROIFNULL(f.admission_count) + ZEROIFNULL(b.admission_count)
            AS nel_admissions_12mo
    FROM nel_full_months AS f
    FULL OUTER JOIN nel_boundary_partial_month AS b
        ON f.person_id = b.person_id AND f.end_date = b.end_date
),

activity_keys AS (
    SELECT person_id, end_date FROM gp_rolling
    UNION
    SELECT person_id, end_date FROM ed_rolling
    UNION
    SELECT person_id, end_date FROM nel_rolling
)

SELECT
    k.person_id,
    pm.sk_patient_id,
    k.end_date,
    ZEROIFNULL(g.gp_appointments_12mo) AS gp_appointments_12mo,
    ZEROIFNULL(e.ed_attendances_12mo) AS ed_attendances_12mo,
    ZEROIFNULL(n.nel_admissions_12mo) AS nel_admissions_12mo
FROM activity_keys AS k
INNER JOIN {{ ref('int_segmentation_person_month_spine') }} AS pm
    ON k.person_id = pm.person_id AND k.end_date = pm.month_end_date
LEFT JOIN gp_rolling AS g
    ON k.person_id = g.person_id AND k.end_date = g.end_date
LEFT JOIN ed_rolling AS e
    ON k.person_id = e.person_id AND k.end_date = e.end_date
LEFT JOIN nel_rolling AS n
    ON k.person_id = n.person_id AND k.end_date = n.end_date
