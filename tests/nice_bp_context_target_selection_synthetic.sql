{{ config(tags=['monthly-full', 'nice-history']) }}

{% set latest = calculate_nice_blood_pressure_control_latest_by_context('by_month') %}
{% set latest = latest | replace(nice_blood_pressure_register_population('by_month') | string, 'synthetic_population') %}
{% set latest = latest | replace(ref('int_blood_pressure_control_daily_by_context') | string, 'synthetic_context') %}
{% set evidence = nice_blood_pressure_for_target('by_month', 140, 90, 135, 85) %}
{% set evidence = evidence | replace(nice_ref('int_nice_blood_pressure_control_latest_by_context', 'by_month') | string, '(SELECT * FROM synthetic_latest)') %}
{% set stricter = nice_blood_pressure_for_target('by_month', 130, 80, 125, 75) %}
{% set stricter = stricter | replace(nice_ref('int_nice_blood_pressure_control_latest_by_context', 'by_month') | string, '(SELECT * FROM synthetic_latest)') %}

WITH synthetic_population AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reporting_date
    FROM VALUES
        (-14421, '2026-08-31'), (-14422, '2026-08-31'),
        (-14423, '2026-08-31'), (-14424, '2026-08-31'),
        (-14425, '2026-08-31'), (-14426, '2026-08-31'),
        (-14427, '2026-08-31'), (-14428, '2026-08-31'),
        (-14429, '2026-08-31'), (-14429, '2026-09-30'), (-14430, '2026-08-31')
),

synthetic_context AS (
    SELECT column1::NUMBER AS person_id, column2::DATE AS reading_date,
        column3::VARCHAR AS bp_context_group,
        column4::FLOAT AS systolic_value, column5::FLOAT AS diastolic_value,
        column6::BOOLEAN AS is_valid_bp,
        bp_context_group = 'HOME' AS is_home_bp_event,
        bp_context_group = 'ABPM' AS is_abpm_bp_event,
        IFF(bp_context_group = 'CLINIC', 'CLINIC', 'HBPM_ABPM') AS applied_measurement_context,
        '2026-08-02'::TIMESTAMP_NTZ AS date_recorded, 'SYN'::VARCHAR AS observation_id
    FROM VALUES
        (-14421, '2026-08-01', 'CLINIC', 139, 89, TRUE),
        (-14421, '2026-08-01', 'HOME', 136, 84, TRUE),
        (-14422, '2026-08-01', 'CLINIC', 140, 89, TRUE),
        (-14422, '2026-08-01', 'HOME', 134, 84, TRUE),
        (-14423, '2026-08-01', 'CLINIC', 140, 90, TRUE),
        (-14423, '2026-08-01', 'HOME', 135, 85, TRUE),
        (-14424, '2026-07-31', 'HOME', 120, 70, TRUE),
        (-14424, '2026-08-01', 'CLINIC', 150, 95, TRUE),
        (-14425, '2026-07-31', 'HOME', 120, 70, TRUE),
        (-14425, '2026-08-01', 'CLINIC', 400, 250, FALSE),
        (-14426, '2026-08-01', 'CLINIC', 139, 89, TRUE),
        (-14426, '2026-08-01', 'HOME', 134, 84, TRUE),
        (-14427, '2026-08-01', 'CLINIC', 129, 79, TRUE),
        (-14427, '2026-08-01', 'HOME', 125, 75, TRUE),
        (-14428, '2026-08-01', 'CLINIC', 130, 80, TRUE),
        (-14428, '2026-08-01', 'ABPM', 124, 74, TRUE),
        (-14429, '2026-08-01', 'CLINIC', 139, 89, TRUE),
        (-14429, '2026-09-01', 'CLINIC', 400, 250, FALSE),
        (-14430, '2026-08-01', 'HOME', 130, 95, TRUE)
),

synthetic_latest AS ({{ latest }}),
actual AS ({{ evidence }}),
strict_actual AS ({{ stricter }}),

checks AS (
    SELECT 'row_count' AS check_name, 11 AS expected_count, COUNT(*) AS actual_count,
        NULL::ARRAY AS selected_evidence
    FROM actual

    UNION ALL
    SELECT 'clinic_passes_home_fails', 1, COALESCE(COUNT_IF(applied_measurement_context = 'CLINIC'), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14421

    UNION ALL
    SELECT 'home_passes_clinic_at_boundary', 1, COALESCE(COUNT_IF(is_home_bp_event), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14422

    UNION ALL
    SELECT 'strict_boundary_keeps_clinic', 1, COALESCE(COUNT_IF(latest_systolic_value = 140 AND latest_diastolic_value = 90), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14423

    UNION ALL
    SELECT 'latest_day_overrides_older_passing_home', 1, COALESCE(COUNT_IF(latest_bp_date = '2026-08-01' AND latest_systolic_value = 150), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14424

    UNION ALL
    SELECT 'invalid_latest_day_remains_invalid', 1, COALESCE(COUNT_IF(NOT is_valid_bp), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14425

    UNION ALL
    SELECT 'both_pass_clinic_priority', 1, COALESCE(COUNT_IF(applied_measurement_context = 'CLINIC'), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14426

    UNION ALL
    SELECT 'history_before_invalid_reading', 1, COALESCE(COUNT_IF(reporting_date = '2026-08-31' AND is_valid_bp), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14429

    UNION ALL
    SELECT 'history_after_invalid_reading', 1, COALESCE(COUNT_IF(reporting_date = '2026-09-30' AND NOT is_valid_bp), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14429

    UNION ALL
    SELECT 'stricter_target_clinic_passes', 1, COALESCE(COUNT_IF(applied_measurement_context = 'CLINIC'), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM strict_actual
    WHERE person_id = -14427

    UNION ALL
    SELECT 'stricter_target_abpm_passes', 1, COALESCE(COUNT_IF(is_abpm_bp_event), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM strict_actual
    WHERE person_id = -14428

    UNION ALL
    SELECT 'home_average_diastolic_above_target', 1, COALESCE(COUNT_IF(latest_systolic_value = 130 AND latest_diastolic_value = 95 AND NOT (latest_systolic_value < 135 AND latest_diastolic_value < 85)), 0),
        ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(
            'person_id', person_id, 'reporting_date', reporting_date,
            'latest_bp_date', latest_bp_date, 'context', applied_measurement_context,
            'systolic', latest_systolic_value, 'diastolic', latest_diastolic_value,
            'is_valid_bp', is_valid_bp
        ))
    FROM actual
    WHERE person_id = -14430
)

SELECT check_name, expected_count, actual_count, selected_evidence
FROM checks
WHERE actual_count <> expected_count
