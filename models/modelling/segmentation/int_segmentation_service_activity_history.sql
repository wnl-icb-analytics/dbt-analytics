{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Exact rolling 12-month outpatient, mental health and community activity.

WITH date_bounds AS (
    SELECT
        MIN(month_end_date) AS first_month,
        MAX(month_end_date) AS last_month
    FROM {{ ref('int_segmentation_person_month_spine') }}
),

op_activity AS (
    SELECT DISTINCT
        sk_patient_id,
        visit_occurrence_id,
        CAST(start_date AS DATE) AS activity_date,
        treatment_function_code,
        main_specialty_code
    FROM {{ ref('int_sus_op_appointment') }}
    WHERE
        appointment_attended_or_dna IN ('5', '6')
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
        AND CAST(start_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(start_date AS DATE) <= (SELECT last_month FROM date_bounds)
),

op_treatment_function_monthly AS (
    SELECT DISTINCT
        sk_patient_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        treatment_function_code
    FROM op_activity
    WHERE treatment_function_code IS NOT NULL
),

-- Each window is the whole months after the boundary month, plus the part of
-- the boundary month from the window start onwards. The two branches cover
-- different months, so they do not overlap. The boundary branch has to be a
-- range because the start is not always a month end: 12 months before
-- 28 February in a non-leap year is 28 February in a leap year, so
-- 29 February also falls inside the window, and the community window start
-- is 12 months before an arbitrary CSDS contact date.
op_treatment_function_presence AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        o.treatment_function_code
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN op_treatment_function_monthly AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND o.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active

    UNION

    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        o.treatment_function_code
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN op_activity AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE pm.is_active AND o.treatment_function_code IS NOT NULL
),

op_treatment_functions AS (
    SELECT
        person_id,
        end_date,
        COUNT(DISTINCT treatment_function_code)
            AS outpatient_treatment_functions_12mo
    FROM op_treatment_function_presence
    GROUP BY person_id, end_date
),

paediatric_op_monthly AS (
    SELECT
        o.sk_patient_id,
        DATE_TRUNC('month', o.activity_date) AS activity_month,
        COUNT(DISTINCT o.visit_occurrence_id) AS appointment_count
    FROM op_activity AS o
    INNER JOIN {{ ref('paediatric_treatment_function_codes') }} AS p
        ON o.treatment_function_code = p.treatment_function_code
    GROUP BY o.sk_patient_id, DATE_TRUNC('month', o.activity_date)
),

paediatric_op_full_months AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(o.appointment_count) AS appointment_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN paediatric_op_monthly AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND o.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

paediatric_op_boundary_partial_month AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(DISTINCT o.visit_occurrence_id) AS appointment_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN op_activity AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    INNER JOIN {{ ref('paediatric_treatment_function_codes') }} AS p
        ON o.treatment_function_code = p.treatment_function_code
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

paediatric_op_rolling AS (
    SELECT
        COALESCE(f.person_id, b.person_id) AS person_id,
        COALESCE(f.end_date, b.end_date) AS end_date,
        ZEROIFNULL(f.appointment_count) + ZEROIFNULL(b.appointment_count)
            AS paediatric_op_appointments_12mo
    FROM paediatric_op_full_months AS f
    FULL OUTER JOIN paediatric_op_boundary_partial_month AS b
        ON f.person_id = b.person_id AND f.end_date = b.end_date
),

child_specialty_monthly AS (
    SELECT DISTINCT
        sk_patient_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        main_specialty_code
    FROM op_activity
    WHERE
        main_specialty_code NOT IN ('110', '120', '130', '180', '501', '560')
        AND COALESCE(treatment_function_code, '')
            NOT IN ('214', '215', '216', '501', '560')
),

child_specialty_presence AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        o.main_specialty_code
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN child_specialty_monthly AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND o.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active

    UNION

    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        o.main_specialty_code
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN op_activity AS o
        ON pm.sk_patient_id = o.sk_patient_id
        AND o.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE
        pm.is_active
        AND o.main_specialty_code
            NOT IN ('110', '120', '130', '180', '501', '560')
        AND COALESCE(o.treatment_function_code, '')
            NOT IN ('214', '215', '216', '501', '560')
),

child_specialties AS (
    SELECT
        person_id,
        end_date,
        COUNT(DISTINCT main_specialty_code) AS outpatient_specialties_12mo
    FROM child_specialty_presence
    GROUP BY person_id, end_date
),

mh_inpatient_rolling AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        COUNT(DISTINCT m.encounter_id) AS mh_inpatient_stays_12mo
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('int_mhsds_spell_encounters') }} AS m
        ON pm.sk_patient_id = m.sk_patient_id
        AND m.start_date <= pm.month_end_date
        AND (
            m.end_date IS NULL
            OR m.end_date >= DATEADD('month', -12, pm.month_end_date)
        )
    WHERE
        pm.is_active
        AND m.sk_patient_id IS NOT NULL
        AND m.sk_patient_id != '1'
        AND m.start_date <= (SELECT last_month FROM date_bounds)
        AND (
            m.end_date IS NULL
            OR m.end_date >= DATEADD(
                'month', -12, (SELECT first_month FROM date_bounds)
            )
        )
    GROUP BY pm.person_id, pm.month_end_date
),

-- Attended contacts excluding Health Visiting Service (team type 16), as in
-- int_segmentation_community_activity.
community_activity AS (
    SELECT
        sk_patient_id,
        CAST(care_contact_date AS DATE) AS activity_date,
        COUNT(*) AS contact_count
    FROM {{ ref('int_csds_contact_currency') }}
    WHERE
        attendance_status IN ('5', '6')
        AND COALESCE(team_type_code, '') != '16'
        AND sk_patient_id IS NOT NULL
        AND sk_patient_id != '1'
        AND CAST(care_contact_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(care_contact_date AS DATE)
            <= (SELECT last_month FROM date_bounds)
    GROUP BY sk_patient_id, CAST(care_contact_date AS DATE)
),

community_monthly AS (
    SELECT
        sk_patient_id,
        DATE_TRUNC('month', activity_date) AS activity_month,
        SUM(contact_count) AS contact_count
    FROM community_activity
    GROUP BY sk_patient_id, DATE_TRUNC('month', activity_date)
),

community_full_months AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(c.contact_count) AS contact_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN community_monthly AS c
        ON pm.sk_patient_id = c.sk_patient_id
        AND c.activity_month
            > DATE_TRUNC('month', DATEADD('month', -12, pm.month_end_date))
        AND c.activity_month <= DATE_TRUNC('month', pm.month_end_date)
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

community_boundary_partial_month AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        SUM(c.contact_count) AS contact_count
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN community_activity AS c
        ON pm.sk_patient_id = c.sk_patient_id
        AND c.activity_date BETWEEN
            DATEADD('month', -12, pm.month_end_date)
            AND LAST_DAY(DATEADD('month', -12, pm.month_end_date))
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

community_rolling AS (
    SELECT
        COALESCE(f.person_id, b.person_id) AS person_id,
        COALESCE(f.end_date, b.end_date) AS end_date,
        ZEROIFNULL(f.contact_count) + ZEROIFNULL(b.contact_count)
            AS community_contacts_12mo
    FROM community_full_months AS f
    FULL OUTER JOIN community_boundary_partial_month AS b
        ON f.person_id = b.person_id AND f.end_date = b.end_date
),

activity_keys AS (
    SELECT person_id, end_date FROM op_treatment_functions
    UNION
    SELECT person_id, end_date FROM paediatric_op_rolling
    UNION
    SELECT person_id, end_date FROM child_specialties
    UNION
    SELECT person_id, end_date FROM mh_inpatient_rolling
    UNION
    SELECT person_id, end_date FROM community_rolling
)

SELECT
    k.person_id,
    pm.sk_patient_id,
    k.end_date,
    ZEROIFNULL(tf.outpatient_treatment_functions_12mo)
        AS outpatient_treatment_functions_12mo,
    ZEROIFNULL(po.paediatric_op_appointments_12mo)
        AS paediatric_op_appointments_12mo,
    ZEROIFNULL(os.outpatient_specialties_12mo) AS outpatient_specialties_12mo,
    ZEROIFNULL(mh.mh_inpatient_stays_12mo) AS mh_inpatient_stays_12mo,
    ZEROIFNULL(c.community_contacts_12mo) AS community_contacts_12mo
FROM activity_keys AS k
INNER JOIN {{ ref('int_segmentation_person_month_spine') }} AS pm
    ON k.person_id = pm.person_id AND k.end_date = pm.month_end_date
LEFT JOIN op_treatment_functions AS tf
    ON k.person_id = tf.person_id AND k.end_date = tf.end_date
LEFT JOIN paediatric_op_rolling AS po
    ON k.person_id = po.person_id AND k.end_date = po.end_date
LEFT JOIN child_specialties AS os
    ON k.person_id = os.person_id AND k.end_date = os.end_date
LEFT JOIN mh_inpatient_rolling AS mh
    ON k.person_id = mh.person_id AND k.end_date = mh.end_date
LEFT JOIN community_rolling AS c
    ON k.person_id = c.person_id AND k.end_date = c.end_date
