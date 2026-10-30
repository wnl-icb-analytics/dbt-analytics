{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

{% set bp_systolic_target = 130 %}
{% set bp_diastolic_target = 80 %}
{% set bp_achieves %}
    is_valid_bp AND systolic_value < {{ bp_systolic_target }} AND diastolic_value < {{ bp_diastolic_target }}
{% endset %}

WITH control_bp AS (
    SELECT *, IFF(is_valid_bp, systolic_value < {{ bp_systolic_target }}
        AND diastolic_value < {{ bp_diastolic_target }}, NULL) AS meets_bp_target
    FROM ({{ blood_pressure_control_latest_contexts() }})
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY {{ bp_control_context_order(bp_achieves) }}
    ) = 1
),

twelve_months_ago AS (
    SELECT DATEADD(MONTH, -12, CURRENT_DATE()) AS twelve_months_ago
),

diabetes_register AS (
    -- Base population: people on diabetes register with proper type classification
    SELECT
        person_id,
        diabetes_type
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE
)

SELECT
    dr.person_id,
    dr.diabetes_type,

    hba.clinical_effective_date AS latest_hba1c_date,
    hba.hba1c_original_value AS latest_hba1c_value,
    hba.result_unit_display AS hba1c_unit,
    hba.hba1c_category AS hba1c_clinical_category,
    bp.clinical_effective_date AS latest_bp_date,

    -- Latest valid general reading, independent of the control assessment.
    bp.systolic_value AS latest_systolic,
    bp.diastolic_value AS latest_diastolic,
    chol.clinical_effective_date AS latest_chol_date,

    -- Cholesterol data
    chol.cholesterol_value AS latest_chol_value,
    CASE
        WHEN hba.is_ifcc THEN 'IFCC'
        WHEN hba.is_dcct THEN 'DCCT'
    END AS hba1c_type,

    -- Target achievement flags
    COALESCE(hba.meets_qof_target, FALSE) AS hba1c_in_target_range,

    -- Local BP target for this measure: systolic <130 and diastolic <80 mmHg.
    control.meets_bp_target AS bp_in_target_range,

    (chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5)
        AS cholesterol_in_target_range,

    -- Overall target achievement
    (
        COALESCE(hba.meets_qof_target, FALSE)
        AND COALESCE(control.meets_bp_target, FALSE)
        AND (chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5)
    ) AS all_three_targets_met,

    -- Recency flags (within last 12 months)
    (
        hba.clinical_effective_date IS NOT NULL
        AND hba.clinical_effective_date >= t.twelve_months_ago
    ) AS hba1c_measured_in_last_12m,

    (
        bp.clinical_effective_date IS NOT NULL
        AND bp.clinical_effective_date >= t.twelve_months_ago
    ) AS bp_measured_in_last_12m,

    (
        chol.clinical_effective_date IS NOT NULL
        AND chol.clinical_effective_date >= t.twelve_months_ago
    ) AS cholesterol_measured_in_last_12m,

    -- Recent but out of range flags
    (
        hba.clinical_effective_date IS NOT NULL
        AND hba.clinical_effective_date >= t.twelve_months_ago
        AND NOT COALESCE(hba.meets_qof_target, FALSE)
    ) AS hba1c_recent_but_out_of_range,

    (
        control.reading_date IS NOT NULL
        AND control.reading_date >= t.twelve_months_ago
        AND COALESCE(control.meets_bp_target = FALSE, FALSE)
    ) AS bp_recent_but_out_of_range,

    (
        chol.clinical_effective_date IS NOT NULL
        AND chol.clinical_effective_date >= t.twelve_months_ago
        AND NOT (
            chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5
        )
    ) AS cholesterol_recent_but_out_of_range,

    control.reading_date AS control_evidence_date,
    control.bp_context_group AS control_evidence_context,
    COALESCE(control.is_valid_bp, FALSE) AS is_control_evidence_valid

FROM diabetes_register AS dr
CROSS JOIN twelve_months_ago AS t
LEFT JOIN {{ ref('int_hba1c_latest') }} AS hba
    ON dr.person_id = hba.person_id
LEFT JOIN control_bp AS control ON dr.person_id = control.person_id
LEFT JOIN {{ ref('int_blood_pressure_latest') }} AS bp
    ON dr.person_id = bp.person_id
LEFT JOIN {{ ref('int_cholesterol_latest') }} AS chol
    ON dr.person_id = chol.person_id

ORDER BY dr.person_id
