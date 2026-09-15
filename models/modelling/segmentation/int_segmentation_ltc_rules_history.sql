{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['monthly-full']
    )
}}

-- Segmentation LTC registers with age, medication or condition-specific rules.

WITH date_bounds AS (
    SELECT
        MIN(month_end_date) AS first_month,
        MAX(month_end_date) AS last_month
    FROM {{ ref('int_segmentation_person_month_spine') }}
),

events AS (
    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'AST' AS condition_code,
        is_diagnosis_code AS is_qualifying_diagnosis,
        is_resolved_code,
        FALSE AS is_other_state
    FROM {{ ref('int_asthma_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'CKD',
        is_stage_3_5_code,
        is_resolved_code,
        is_stage_1_2_code
    FROM {{ ref('int_ckd_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'COPD',
        is_diagnosis_code,
        is_resolved_code,
        FALSE
    FROM {{ ref('int_copd_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'DEP',
        is_diagnosis_code AND is_first_or_new_episode,
        is_resolved_code,
        FALSE
    FROM {{ ref('int_depression_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'DM',
        is_general_diabetes_code,
        is_diabetes_resolved_code,
        FALSE
    FROM {{ ref('int_diabetes_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'EP',
        is_diagnosis_code,
        is_resolved_code,
        FALSE
    FROM {{ ref('int_epilepsy_diagnoses_all') }}

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        date_recorded,
        'SMI',
        is_diagnosis_code,
        FALSE,
        FALSE
    FROM {{ ref('int_smi_diagnoses_all') }}
),

diagnosis_state AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        pm.age,
        e.condition_code,
        MIN(IFF(e.is_qualifying_diagnosis, e.clinical_effective_date, NULL))
            AS earliest_diagnosis_date,
        MAX(IFF(e.is_qualifying_diagnosis, e.clinical_effective_date, NULL))
            AS latest_diagnosis_date,
        MAX(IFF(e.is_resolved_code, e.clinical_effective_date, NULL))
            AS latest_resolved_date,
        MAX(IFF(e.is_other_state, e.clinical_effective_date, NULL))
            AS latest_other_state_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN events AS e
        ON pm.person_id = e.person_id
        AND e.clinical_effective_date <= pm.month_end_date
        AND (
            e.date_recorded IS NULL
            OR CAST(e.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date, pm.age, e.condition_code
),

diagnosis_qualified AS (
    SELECT *
    FROM diagnosis_state
    WHERE
        CASE
            -- The segmentation canonicalises the QOF and under-18 asthma
            -- registers, so their combined AST condition has no age gap.
            WHEN condition_code = 'AST'
                THEN latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            WHEN condition_code = 'CKD'
                THEN age >= 18
                    AND latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_other_state_date IS NULL
                        OR latest_diagnosis_date > latest_other_state_date
                    )
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            -- Spirometry affects COPD indicator fields but does not gate the
            -- current QOF v50 register. A lone diagnosis and resolution on the
            -- same date follows the existing EUNRESCOPD rule and remains in.
            WHEN condition_code = 'COPD'
                THEN latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                        OR (
                            latest_diagnosis_date = latest_resolved_date
                            AND earliest_diagnosis_date = latest_diagnosis_date
                        )
                    )
            WHEN condition_code = 'DEP'
                THEN age >= 18
                    AND latest_diagnosis_date >= '2006-04-01'
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            WHEN condition_code = 'DM'
                THEN age >= 17
                    AND latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            WHEN condition_code = 'EP'
                THEN age >= 18
                    AND latest_diagnosis_date IS NOT NULL
                    AND (
                        latest_resolved_date IS NULL
                        OR latest_diagnosis_date > latest_resolved_date
                    )
            WHEN condition_code = 'SMI'
                THEN latest_diagnosis_date IS NOT NULL
        END
),

-- Orders are bucketed by recorded month as well as order month so the join
-- below can drop orders recorded after the month-end. Every reference date is
-- a month-end, so LAST_DAY(date_recorded) <= end_date is the same cut as
-- date_recorded <= end_date.
asthma_medication_months AS (
    SELECT
        person_id,
        LAST_DAY(CAST(order_date AS DATE)) AS order_month,
        LAST_DAY(CAST(date_recorded AS DATE)) AS recorded_month,
        MAX(CAST(order_date AS DATE)) AS latest_order_date
    FROM {{ ref('int_asthma_medications_all') }}
    WHERE
        CAST(order_date AS DATE) >= DATEADD(
            'month', -12, (SELECT first_month FROM date_bounds)
        )
        AND CAST(order_date AS DATE) <= (SELECT last_month FROM date_bounds)
    GROUP BY
        person_id,
        LAST_DAY(CAST(order_date AS DATE)),
        LAST_DAY(CAST(date_recorded AS DATE))
),

asthma_register AS (
    SELECT
        d.person_id,
        d.end_date,
        d.condition_code,
        d.earliest_diagnosis_date,
        d.latest_diagnosis_date,
        d.latest_resolved_date,
        d.latest_other_state_date,
        MAX(m.latest_order_date) AS latest_medication_date,
        NULL::DATE AS latest_stop_date
    FROM diagnosis_qualified AS d
    INNER JOIN asthma_medication_months AS m
        ON d.person_id = m.person_id
        AND m.latest_order_date >= DATEADD('month', -12, d.end_date)
        AND m.latest_order_date <= d.end_date
        AND (
            m.recorded_month IS NULL
            OR m.recorded_month <= d.end_date
        )
    WHERE d.condition_code = 'AST'
    GROUP BY ALL
),

epilepsy_medication_months AS (
    SELECT
        person_id,
        LAST_DAY(CAST(order_date AS DATE)) AS order_month,
        LAST_DAY(CAST(date_recorded AS DATE)) AS recorded_month,
        MAX(CAST(order_date AS DATE)) AS latest_order_date
    FROM {{ ref('int_epilepsy_medications_all') }}
    WHERE
        CAST(order_date AS DATE) >= DATEADD(
            'month', -6, (SELECT first_month FROM date_bounds)
        )
        AND CAST(order_date AS DATE) <= (SELECT last_month FROM date_bounds)
    GROUP BY
        person_id,
        LAST_DAY(CAST(order_date AS DATE)),
        LAST_DAY(CAST(date_recorded AS DATE))
),

epilepsy_register AS (
    SELECT
        d.person_id,
        d.end_date,
        d.condition_code,
        d.earliest_diagnosis_date,
        d.latest_diagnosis_date,
        d.latest_resolved_date,
        d.latest_other_state_date,
        MAX(m.latest_order_date) AS latest_medication_date,
        NULL::DATE AS latest_stop_date
    FROM diagnosis_qualified AS d
    INNER JOIN epilepsy_medication_months AS m
        ON d.person_id = m.person_id
        AND m.latest_order_date >= DATEADD('month', -6, d.end_date)
        AND m.latest_order_date <= d.end_date
        AND (
            m.recorded_month IS NULL
            OR m.recorded_month <= d.end_date
        )
    WHERE d.condition_code = 'EP'
    GROUP BY ALL
),

lithium_recent_orders AS (
    SELECT
        pm.person_id,
        pm.month_end_date AS end_date,
        MAX(CAST(m.order_date AS DATE)) AS latest_medication_date
    FROM {{ ref('int_segmentation_person_month_spine') }} AS pm
    INNER JOIN {{ ref('int_lithium_medications_all') }} AS m
        ON pm.person_id = m.person_id
        AND CAST(m.order_date AS DATE) > DATEADD('month', -6, pm.month_end_date)
        AND CAST(m.order_date AS DATE) <= pm.month_end_date
        AND (
            m.date_recorded IS NULL
            OR CAST(m.date_recorded AS DATE) <= pm.month_end_date
        )
    WHERE pm.is_active
    GROUP BY pm.person_id, pm.month_end_date
),

lithium_stops AS (
    SELECT
        person_id,
        clinical_effective_date,
        date_recorded
    FROM ({{ get_observations("'LITSTP_COD'", source='PCD') }})
),

lithium_status AS (
    SELECT
        l.person_id,
        l.end_date,
        l.latest_medication_date,
        MAX(s.clinical_effective_date) AS latest_stop_date
    FROM lithium_recent_orders AS l
    LEFT JOIN lithium_stops AS s
        ON l.person_id = s.person_id
        AND s.clinical_effective_date <= l.end_date
        AND (
            s.date_recorded IS NULL
            OR CAST(s.date_recorded AS DATE) <= l.end_date
        )
    GROUP BY l.person_id, l.end_date, l.latest_medication_date
),

lithium_register AS (
    SELECT
        person_id,
        end_date,
        'SMI' AS condition_code,
        NULL::DATE AS earliest_diagnosis_date,
        NULL::DATE AS latest_diagnosis_date,
        NULL::DATE AS latest_resolved_date,
        NULL::DATE AS latest_other_state_date,
        latest_medication_date,
        latest_stop_date
    FROM lithium_status
    WHERE
        latest_stop_date IS NULL
        OR latest_stop_date <= latest_medication_date
),

register_rows AS (
    SELECT
        person_id,
        end_date,
        condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        latest_resolved_date,
        latest_other_state_date,
        NULL::DATE AS latest_medication_date,
        NULL::DATE AS latest_stop_date
    FROM diagnosis_qualified
    WHERE condition_code IN ('CKD', 'COPD', 'DEP', 'DM', 'SMI')

    UNION ALL

    SELECT * FROM asthma_register

    UNION ALL

    SELECT * FROM epilepsy_register

    UNION ALL

    SELECT * FROM lithium_register
)

SELECT
    person_id,
    end_date,
    condition_code,
    TRUE AS is_on_register,
    MIN(earliest_diagnosis_date) AS earliest_diagnosis_date,
    MAX(latest_diagnosis_date) AS latest_diagnosis_date,
    MAX(latest_resolved_date) AS latest_resolved_date,
    MAX(latest_other_state_date) AS latest_other_state_date,
    MAX(latest_medication_date) AS latest_medication_date,
    MAX(latest_stop_date) AS latest_stop_date
FROM register_rows
GROUP BY person_id, end_date, condition_code
