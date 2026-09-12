-- Pair: macros/qof_registers/calculate_qof_ndh_gdm_register.sql.
-- This live fact includes future-dated records. Its PIT pair is strict as-of
-- and derives age at the reference date rather than using current age.

{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
QOF v51 NDH_REG at the current date.

NDH, impaired glucose tolerance and pre-diabetes require age 18 or over.
Gestational diabetes qualifies at any age. The register then applies the five
ordered diabetes-history rules from NDH_REG v102.

The live fact retains future-dated records for compatibility with the existing
fact family. The PIT view applies strict as-of filtering.
*/

WITH parameters AS (
    SELECT DATE_FROM_PARTS(
        IFF(MONTH(CURRENT_DATE()) >= 4, YEAR(CURRENT_DATE()), YEAR(CURRENT_DATE()) - 1),
        4,
        1
    ) AS quality_service_start_date
),

ndh_gdm_events AS (
    SELECT
        diagnosis.id,
        diagnosis.person_id,
        diagnosis.clinical_effective_date,
        TRUE AS is_any_ndh_type_code,
        FALSE AS is_gestational_diabetes_code
    FROM {{ ref('int_ndh_diagnoses_all') }} AS diagnosis

    UNION ALL

    SELECT
        diagnosis.id,
        diagnosis.person_id,
        diagnosis.clinical_effective_date,
        FALSE AS is_any_ndh_type_code,
        TRUE AS is_gestational_diabetes_code
    FROM {{ ref('int_gestational_diabetes_diagnoses_all') }} AS diagnosis
),

diabetes_events AS (
    SELECT
        person_id,
        clinical_effective_date,
        is_general_diabetes_code,
        is_diabetes_resolved_code
    FROM {{ ref('int_diabetes_diagnoses_all') }}
),

ndh_gdm_person_aggregates AS (
    SELECT
        person_id,
        MIN(clinical_effective_date) AS earliest_diagnosis_date,
        MAX(clinical_effective_date) AS latest_diagnosis_date,
        MAX(is_any_ndh_type_code) AS has_ndh_diagnosis,
        MAX(is_gestational_diabetes_code) AS has_gestational_diabetes_diagnosis
    FROM ndh_gdm_events
    GROUP BY person_id
),

diabetes_person_aggregates AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END)
            AS earliest_diabetes_diagnosis_date,
        MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END)
            AS latest_diabetes_diagnosis_date,
        MAX(CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END)
            AS latest_diabetes_resolved_date
    FROM diabetes_events
    GROUP BY person_id
),

reporting_year_event_context AS (
    SELECT
        event.person_id,
        event.id,
        event.clinical_effective_date,
        MAX(CASE
            WHEN diabetes.is_general_diabetes_code
                AND diabetes.clinical_effective_date <= event.clinical_effective_date
                THEN diabetes.clinical_effective_date
        END) AS latest_diabetes_before_event,
        MAX(CASE WHEN diabetes.is_diabetes_resolved_code
            THEN diabetes.clinical_effective_date END)
            AS latest_diabetes_resolved_date
    FROM ndh_gdm_events AS event
    CROSS JOIN parameters AS parameter
    LEFT JOIN diabetes_events AS diabetes
        ON event.person_id = diabetes.person_id
    WHERE event.clinical_effective_date >= parameter.quality_service_start_date
    GROUP BY event.person_id, event.id, event.clinical_effective_date
),

rule_4_qualifiers AS (
    SELECT DISTINCT person_id
    FROM reporting_year_event_context
    WHERE latest_diabetes_before_event IS NULL
        OR latest_diabetes_resolved_date > latest_diabetes_before_event
),

before_reporting_year_events AS (
    SELECT
        event.person_id,
        MAX(event.clinical_effective_date) AS latest_diagnosis_date
    FROM ndh_gdm_events AS event
    CROSS JOIN parameters AS parameter
    WHERE event.clinical_effective_date < parameter.quality_service_start_date
    GROUP BY event.person_id
),

before_reporting_year_diabetes_context AS (
    SELECT
        event.person_id,
        MAX(CASE
            WHEN diabetes.is_general_diabetes_code
                AND diabetes.clinical_effective_date <= parameter.quality_service_start_date
                THEN diabetes.clinical_effective_date
        END) AS latest_diabetes_at_service_start,
        MAX(CASE WHEN diabetes.is_diabetes_resolved_code
            THEN diabetes.clinical_effective_date END)
            AS latest_diabetes_resolved_date
    FROM before_reporting_year_events AS event
    CROSS JOIN parameters AS parameter
    LEFT JOIN diabetes_events AS diabetes
        ON event.person_id = diabetes.person_id
    GROUP BY event.person_id
),

rule_5_qualifiers AS (
    SELECT person_id
    FROM before_reporting_year_diabetes_context
    WHERE latest_diabetes_at_service_start IS NULL
        OR latest_diabetes_resolved_date > latest_diabetes_at_service_start
),

register_logic AS (
    SELECT
        event.person_id,
        age.age,
        parameter.quality_service_start_date,
        event.earliest_diagnosis_date,
        event.latest_diagnosis_date,
        event.has_ndh_diagnosis,
        event.has_gestational_diabetes_diagnosis,
        diabetes.earliest_diabetes_diagnosis_date,
        diabetes.latest_diabetes_diagnosis_date,
        diabetes.latest_diabetes_resolved_date,
        COALESCE(age.age >= 18 AND event.has_ndh_diagnosis, FALSE)
            AS has_ndh_route,
        COALESCE(event.has_gestational_diabetes_diagnosis, FALSE)
            AS has_gdm_route,
        COALESCE(
            event.has_gestational_diabetes_diagnosis
            OR (age.age >= 18 AND event.has_ndh_diagnosis),
            FALSE
        ) AS passes_entry_rule,
        CASE
            WHEN diabetes.earliest_diabetes_diagnosis_date IS NULL THEN 2
            WHEN diabetes.latest_diabetes_resolved_date
                > diabetes.latest_diabetes_diagnosis_date THEN 3
            WHEN rule_4.person_id IS NOT NULL THEN 4
            WHEN rule_5.person_id IS NOT NULL THEN 5
        END AS qualifying_rule
    FROM ndh_gdm_person_aggregates AS event
    CROSS JOIN parameters AS parameter
    LEFT JOIN diabetes_person_aggregates AS diabetes
        ON event.person_id = diabetes.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age
        ON event.person_id = age.person_id
    LEFT JOIN rule_4_qualifiers AS rule_4
        ON event.person_id = rule_4.person_id
    LEFT JOIN rule_5_qualifiers AS rule_5
        ON event.person_id = rule_5.person_id
),

register_membership AS (
    SELECT
        person_id,
        COALESCE(passes_entry_rule AND qualifying_rule IS NOT NULL, FALSE)
            AS is_on_register,
        qualifying_rule,
        age,
        quality_service_start_date,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        has_ndh_diagnosis,
        has_gestational_diabetes_diagnosis,
        has_ndh_route,
        has_gdm_route,
        earliest_diabetes_diagnosis_date,
        latest_diabetes_diagnosis_date,
        latest_diabetes_resolved_date
    FROM register_logic
),

qualifying_diagnoses AS (
    SELECT
        diagnosis.id,
        diagnosis.person_id,
        diagnosis.clinical_effective_date,
        diagnosis.concept_code,
        diagnosis.concept_display,
        diagnosis.is_ndh_diagnosis_code,
        diagnosis.is_igt_diagnosis_code,
        diagnosis.is_pre_diabetes_diagnosis_code,
        FALSE AS is_gestational_diabetes_code
    FROM {{ ref('int_ndh_diagnoses_all') }} AS diagnosis

    UNION ALL

    SELECT
        diagnosis.id,
        diagnosis.person_id,
        diagnosis.clinical_effective_date,
        diagnosis.concept_code,
        diagnosis.concept_display,
        FALSE AS is_ndh_diagnosis_code,
        FALSE AS is_igt_diagnosis_code,
        FALSE AS is_pre_diabetes_diagnosis_code,
        diagnosis.is_diagnosis_code AS is_gestational_diabetes_code
    FROM {{ ref('int_gestational_diabetes_diagnoses_all') }} AS diagnosis
),

diagnosis_details AS (
    SELECT
        diagnosis.person_id,
        MIN(CASE WHEN diagnosis.is_ndh_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS earliest_ndh_date,
        MAX(CASE WHEN diagnosis.is_ndh_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS latest_ndh_date,
        MIN(CASE WHEN diagnosis.is_igt_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS earliest_igt_date,
        MAX(CASE WHEN diagnosis.is_igt_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS latest_igt_date,
        MIN(CASE WHEN diagnosis.is_pre_diabetes_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS earliest_prd_date,
        MAX(CASE WHEN diagnosis.is_pre_diabetes_diagnosis_code
            THEN diagnosis.clinical_effective_date END) AS latest_prd_date,
        MIN(CASE WHEN diagnosis.is_gestational_diabetes_code
            THEN diagnosis.clinical_effective_date END)
            AS earliest_gestational_diabetes_date,
        MAX(CASE WHEN diagnosis.is_gestational_diabetes_code
            THEN diagnosis.clinical_effective_date END)
            AS latest_gestational_diabetes_date,
        COUNT(CASE WHEN diagnosis.is_ndh_diagnosis_code THEN 1 END)
            AS total_ndh_episodes,
        COUNT(CASE WHEN diagnosis.is_igt_diagnosis_code THEN 1 END)
            AS total_igt_episodes,
        COUNT(CASE WHEN diagnosis.is_pre_diabetes_diagnosis_code THEN 1 END)
            AS total_prd_episodes,
        COUNT(CASE WHEN diagnosis.is_gestational_diabetes_code THEN 1 END)
            AS total_gestational_diabetes_episodes,
        MAX(diagnosis.is_ndh_diagnosis_code) AS has_ndh_diagnosis,
        MAX(diagnosis.is_igt_diagnosis_code) AS has_igt_diagnosis,
        MAX(diagnosis.is_pre_diabetes_diagnosis_code) AS has_prd_diagnosis,
        MAX(diagnosis.is_gestational_diabetes_code)
            AS has_gestational_diabetes_diagnosis,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_ndh_diagnosis_code
            THEN diagnosis.concept_code END) AS ndh_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_ndh_diagnosis_code
            THEN diagnosis.concept_display END) AS ndh_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_igt_diagnosis_code
            THEN diagnosis.concept_code END) AS igt_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_igt_diagnosis_code
            THEN diagnosis.concept_display END) AS igt_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_pre_diabetes_diagnosis_code
            THEN diagnosis.concept_code END) AS prd_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_pre_diabetes_diagnosis_code
            THEN diagnosis.concept_display END) AS prd_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_gestational_diabetes_code
            THEN diagnosis.concept_code END)
            AS gestational_diabetes_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN diagnosis.is_gestational_diabetes_code
            THEN diagnosis.concept_display END)
            AS gestational_diabetes_diagnosis_displays,
        ARRAY_AGG(DISTINCT diagnosis.id::VARCHAR) AS all_ids
    FROM qualifying_diagnoses AS diagnosis
    GROUP BY diagnosis.person_id
)

SELECT
    membership.person_id,
    membership.is_on_register,
    CASE
        WHEN membership.has_gdm_route
            AND COALESCE(membership.age < 18, TRUE)
            THEN 'Gestational diabetes - any-age QOF route'
        WHEN membership.has_gdm_route
            AND membership.has_ndh_route
            THEN 'NDH and gestational diabetes - eligible for QOF care'
        WHEN membership.has_gdm_route
            THEN 'Gestational diabetes - eligible for QOF care'
        ELSE 'NDH - eligible for diabetes prevention'
    END AS ndh_status,
    membership.earliest_diagnosis_date,
    membership.latest_diagnosis_date,
    details.earliest_ndh_date,
    details.latest_ndh_date,
    details.earliest_igt_date,
    details.latest_igt_date,
    details.earliest_prd_date,
    details.latest_prd_date,
    details.earliest_gestational_diabetes_date,
    details.latest_gestational_diabetes_date,
    CASE
        WHEN membership.earliest_diagnosis_date IS NOT NULL
            THEN membership.age - DATEDIFF(
                YEAR,
                membership.earliest_diagnosis_date,
                CURRENT_DATE()
            )
    END AS age_at_first_ndh_diagnosis,
    details.total_ndh_episodes,
    details.total_igt_episodes,
    details.total_prd_episodes,
    details.total_gestational_diabetes_episodes,
    details.has_ndh_diagnosis,
    details.has_igt_diagnosis,
    details.has_prd_diagnosis,
    details.has_gestational_diabetes_diagnosis,
    membership.has_ndh_route,
    membership.has_gdm_route,
    membership.earliest_diabetes_diagnosis_date IS NOT NULL
        AS has_diabetes_diagnosis,
    COALESCE(
        membership.latest_diabetes_resolved_date
            > membership.latest_diabetes_diagnosis_date,
        FALSE
    ) AS is_diabetes_resolved,
    membership.earliest_diabetes_diagnosis_date,
    membership.latest_diabetes_resolved_date,
    membership.qualifying_rule,
    membership.quality_service_start_date,
    details.ndh_diagnosis_codes,
    details.ndh_diagnosis_displays,
    details.igt_diagnosis_codes,
    details.igt_diagnosis_displays,
    details.prd_diagnosis_codes,
    details.prd_diagnosis_displays,
    details.gestational_diabetes_diagnosis_codes,
    details.gestational_diabetes_diagnosis_displays,
    details.all_ids
FROM register_membership AS membership
INNER JOIN diagnosis_details AS details
    ON membership.person_id = details.person_id
WHERE membership.is_on_register = TRUE
ORDER BY membership.earliest_diagnosis_date DESC, membership.person_id
