{% macro calculate_qof_ndh_gdm_register(reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
    {# Pair: fct_person_qof_ndh_gdm_register.sql. Evidence is bounded by the reference date. #}
    {#
    Calculates QOF v51 NDH_REG status at one or more achievement dates.

    NDH/IGT/PRD requires age 18 or over. Gestational diabetes qualifies at any
    age. The five ordered rules retain people who have never had diabetes,
    whose diabetes is resolved, or whose reporting-year/pre-year diagnosis has
    the required diabetes-resolution history.

    Evidence is bounded by each reference date.

    Parameters:
        reference_date_expr: SQL expression for a single reference date (default: CURRENT_DATE())
        reference_dates: query returning a reference_date column; evaluates every
            date it returns instead of reference_date_expr

    Returns: one row per person with NDH or GDM evidence known by each reference date:
        reference_date, person_id, register_name, is_on_register,
        earliest_diagnosis_date, latest_diagnosis_date, qualifying_rule, age,
        quality_service_start_date, has_ndh_diagnosis,
        has_gestational_diabetes_diagnosis, has_ndh_route, has_gdm_route,
        earliest_diabetes_diagnosis_date, latest_diabetes_diagnosis_date,
        latest_diabetes_resolved_date
    #}

    WITH reference_dates AS (
        {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
    ),

    parameters AS (
        SELECT
            CAST(reference_date AS DATE) AS reference_date,
            DATE_FROM_PARTS(
                IFF(
                    MONTH(CAST(reference_date AS DATE)) >= 4,
                    YEAR(CAST(reference_date AS DATE)),
                    YEAR(CAST(reference_date AS DATE)) - 1
                ),
                4,
                1
            ) AS quality_service_start_date
        FROM reference_dates
    ),

    ndh_gdm_events AS (
        SELECT
            ref_date.reference_date,
            diagnosis.id,
            diagnosis.person_id,
            diagnosis.clinical_effective_date,
            TRUE AS is_any_ndh_type_code,
            FALSE AS is_gestational_diabetes_code
        FROM {{ ref('int_ndh_diagnoses_all') }} AS diagnosis
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diagnosis.clinical_effective_date', 'diagnosis.date_recorded', 'ref_date.reference_date') }}

        UNION ALL

        SELECT
            ref_date.reference_date,
            diagnosis.id,
            diagnosis.person_id,
            diagnosis.clinical_effective_date,
            FALSE AS is_any_ndh_type_code,
            TRUE AS is_gestational_diabetes_code
        FROM {{ ref('int_gestational_diabetes_diagnoses_all') }} AS diagnosis
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diagnosis.clinical_effective_date', 'diagnosis.date_recorded', 'ref_date.reference_date') }}
    ),

    diabetes_events AS (
        SELECT
            ref_date.reference_date,
            diagnosis.person_id,
            diagnosis.clinical_effective_date,
            diagnosis.is_general_diabetes_code,
            diagnosis.is_diabetes_resolved_code
        FROM {{ ref('int_diabetes_diagnoses_all') }} AS diagnosis
        INNER JOIN reference_dates AS ref_date
            ON {{ ltc_register_known_by('diagnosis.clinical_effective_date', 'diagnosis.date_recorded', 'ref_date.reference_date') }}
    ),

    ndh_gdm_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date,
            MAX(clinical_effective_date) AS latest_diagnosis_date,
            MAX(is_any_ndh_type_code) AS has_ndh_diagnosis,
            MAX(is_gestational_diabetes_code)
                AS has_gestational_diabetes_diagnosis
        FROM ndh_gdm_events
        GROUP BY reference_date, person_id
    ),

    diabetes_person_aggregates AS (
        SELECT
            reference_date,
            person_id,
            MIN(CASE
                WHEN is_general_diabetes_code THEN clinical_effective_date
            END) AS earliest_diabetes_diagnosis_date,
            MAX(CASE
                WHEN is_general_diabetes_code THEN clinical_effective_date
            END) AS latest_diabetes_diagnosis_date,
            MAX(CASE
                WHEN is_diabetes_resolved_code THEN clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM diabetes_events
        GROUP BY reference_date, person_id
    ),

    reporting_year_event_context AS (
        SELECT
            event.reference_date,
            event.person_id,
            event.id,
            event.clinical_effective_date,
            MAX(CASE
                WHEN
                    diabetes.is_general_diabetes_code
                    AND CAST(diabetes.clinical_effective_date AS DATE)
                        <= CAST(event.clinical_effective_date AS DATE)
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_before_event,
            MAX(CASE
                WHEN diabetes.is_diabetes_resolved_code
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM ndh_gdm_events AS event
        INNER JOIN parameters AS parameter
            ON event.reference_date = parameter.reference_date
        LEFT JOIN diabetes_events AS diabetes
            ON event.person_id = diabetes.person_id
            AND event.reference_date = diabetes.reference_date
        WHERE
            CAST(event.clinical_effective_date AS DATE)
            >= CAST(parameter.quality_service_start_date AS DATE)
            AND CAST(event.clinical_effective_date AS DATE) <= parameter.reference_date
        GROUP BY
            event.reference_date,
            event.person_id,
            event.id,
            event.clinical_effective_date
    ),

    rule_4_qualifiers AS (
        SELECT DISTINCT reference_date, person_id
        FROM reporting_year_event_context
        WHERE
            latest_diabetes_before_event IS NULL
            OR CAST(latest_diabetes_resolved_date AS DATE) > CAST(latest_diabetes_before_event AS DATE)
    ),

    before_reporting_year_events AS (
        SELECT
            event.reference_date,
            event.person_id,
            MAX(event.clinical_effective_date) AS latest_diagnosis_date
        FROM ndh_gdm_events AS event
        INNER JOIN parameters AS parameter
            ON event.reference_date = parameter.reference_date
        WHERE
            CAST(event.clinical_effective_date AS DATE)
            < CAST(parameter.quality_service_start_date AS DATE)
        GROUP BY event.reference_date, event.person_id
    ),

    before_reporting_year_diabetes_context AS (
        SELECT
            event.reference_date,
            event.person_id,
            MAX(CASE
                WHEN
                    diabetes.is_general_diabetes_code
                    AND CAST(diabetes.clinical_effective_date AS DATE)
                        <= CAST(parameter.quality_service_start_date AS DATE)
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_at_service_start,
            MAX(CASE
                WHEN diabetes.is_diabetes_resolved_code
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM before_reporting_year_events AS event
        INNER JOIN parameters AS parameter
            ON event.reference_date = parameter.reference_date
        LEFT JOIN diabetes_events AS diabetes
            ON event.person_id = diabetes.person_id
            AND event.reference_date = diabetes.reference_date
        GROUP BY event.reference_date, event.person_id
    ),

    rule_5_qualifiers AS (
        SELECT reference_date, person_id
        FROM before_reporting_year_diabetes_context
        WHERE
            latest_diabetes_at_service_start IS NULL
            OR CAST(latest_diabetes_resolved_date AS DATE)
                > CAST(latest_diabetes_at_service_start AS DATE)
    ),

    age_at_reference AS (
        SELECT
            event.reference_date,
            event.person_id,
            FLOOR(
                DATEDIFF(
                    'month',
                    person.birth_date_approx,
                    event.reference_date
                ) / 12
            ) AS age
        FROM ndh_gdm_person_aggregates AS event
        INNER JOIN {{ ref('dim_person_birth_death') }} AS person
            ON event.person_id = person.person_id
        WHERE person.birth_date_approx IS NOT NULL
    ),

    register_logic AS (
        SELECT
            event.reference_date,
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
            COALESCE(
                age.age >= 18
                AND event.has_ndh_diagnosis,
                FALSE
            ) AS has_ndh_route,
            COALESCE(
                event.has_gestational_diabetes_diagnosis,
                FALSE
            ) AS has_gdm_route,
            COALESCE(
                event.has_gestational_diabetes_diagnosis
                OR (
                    age.age >= 18
                    AND event.has_ndh_diagnosis
                ),
                FALSE
            ) AS passes_entry_rule,
            CASE
                WHEN diabetes.earliest_diabetes_diagnosis_date IS NULL
                    THEN 2
                WHEN
                    CAST(diabetes.latest_diabetes_resolved_date AS DATE)
                    > CAST(diabetes.latest_diabetes_diagnosis_date AS DATE)
                    THEN 3
                WHEN rule_4.person_id IS NOT NULL
                    THEN 4
                WHEN rule_5.person_id IS NOT NULL
                    THEN 5
            END AS qualifying_rule
        FROM ndh_gdm_person_aggregates AS event
        INNER JOIN parameters AS parameter
            ON event.reference_date = parameter.reference_date
        LEFT JOIN diabetes_person_aggregates AS diabetes
            ON event.person_id = diabetes.person_id
            AND event.reference_date = diabetes.reference_date
        LEFT JOIN age_at_reference AS age
            ON event.person_id = age.person_id
            AND event.reference_date = age.reference_date
        LEFT JOIN rule_4_qualifiers AS rule_4
            ON event.person_id = rule_4.person_id
            AND event.reference_date = rule_4.reference_date
        LEFT JOIN rule_5_qualifiers AS rule_5
            ON event.person_id = rule_5.person_id
            AND event.reference_date = rule_5.reference_date
    )

    SELECT
        reference_date,
        person_id,
        'NDH' AS register_name,
        COALESCE(
            passes_entry_rule
            AND qualifying_rule IS NOT NULL,
            FALSE
        ) AS is_on_register,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        qualifying_rule,
        age,
        quality_service_start_date,
        has_ndh_diagnosis,
        has_gestational_diabetes_diagnosis,
        has_ndh_route,
        has_gdm_route,
        earliest_diabetes_diagnosis_date,
        latest_diabetes_diagnosis_date,
        latest_diabetes_resolved_date
    FROM register_logic

{% endmacro %}
