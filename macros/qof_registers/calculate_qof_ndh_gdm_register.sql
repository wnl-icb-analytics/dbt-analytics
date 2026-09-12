{% macro calculate_qof_ndh_gdm_register(reference_date_expr='CURRENT_DATE()', include_future_records=false) %}
    {# Pair: fct_person_qof_ndh_gdm_register.sql. The default is strict as-of with age at the reference date; the live fact explicitly enables future-dated records. #}
    {#
    Calculates QOF v51 NDH_REG status at a supplied achievement date.

    NDH/IGT/PRD requires age 18 or over. Gestational diabetes qualifies at any
    age. The five ordered rules retain people who have never had diabetes,
    whose diabetes is resolved, or whose reporting-year/pre-year diagnosis has
    the required diabetes-resolution history.

    PIT calls use the default strict as-of scope. The live fact sets
    include_future_records=true to retain the established future-record scope.
    #}

    WITH parameters AS (
        SELECT
            CAST({{ reference_date_expr }} AS DATE) AS reference_date,
            DATE_FROM_PARTS(
                IFF(
                    MONTH(CAST({{ reference_date_expr }} AS DATE)) >= 4,
                    YEAR(CAST({{ reference_date_expr }} AS DATE)),
                    YEAR(CAST({{ reference_date_expr }} AS DATE)) - 1
                ),
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
        CROSS JOIN parameters AS parameter
        {% if not include_future_records %}
        WHERE
            diagnosis.clinical_effective_date <= parameter.reference_date
            AND (
                diagnosis.date_recorded IS NULL
                OR CAST(diagnosis.date_recorded AS DATE)
                    <= parameter.reference_date
            )
        {% endif %}

        UNION ALL

        SELECT
            diagnosis.id,
            diagnosis.person_id,
            diagnosis.clinical_effective_date,
            FALSE AS is_any_ndh_type_code,
            TRUE AS is_gestational_diabetes_code
        FROM {{ ref('int_gestational_diabetes_diagnoses_all') }} AS diagnosis
        CROSS JOIN parameters AS parameter
        {% if not include_future_records %}
        WHERE diagnosis.clinical_effective_date <= parameter.reference_date
        {% endif %}
    ),

    diabetes_events AS (
        SELECT
            diagnosis.person_id,
            diagnosis.clinical_effective_date,
            diagnosis.is_general_diabetes_code,
            diagnosis.is_diabetes_resolved_code
        FROM {{ ref('int_diabetes_diagnoses_all') }} AS diagnosis
        CROSS JOIN parameters AS parameter
        {% if not include_future_records %}
        WHERE
            diagnosis.clinical_effective_date <= parameter.reference_date
            AND (
                diagnosis.date_recorded IS NULL
                OR CAST(diagnosis.date_recorded AS DATE)
                    <= parameter.reference_date
            )
        {% endif %}
    ),

    ndh_gdm_person_aggregates AS (
        SELECT
            person_id,
            MIN(clinical_effective_date) AS earliest_diagnosis_date,
            MAX(clinical_effective_date) AS latest_diagnosis_date,
            MAX(is_any_ndh_type_code) AS has_ndh_diagnosis,
            MAX(is_gestational_diabetes_code)
                AS has_gestational_diabetes_diagnosis
        FROM ndh_gdm_events
        GROUP BY person_id
    ),

    diabetes_person_aggregates AS (
        SELECT
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
        GROUP BY person_id
    ),

    reporting_year_event_context AS (
        SELECT
            event.person_id,
            event.id,
            event.clinical_effective_date,
            MAX(CASE
                WHEN
                    diabetes.is_general_diabetes_code
                    AND diabetes.clinical_effective_date
                        <= event.clinical_effective_date
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_before_event,
            MAX(CASE
                WHEN diabetes.is_diabetes_resolved_code
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM ndh_gdm_events AS event
        CROSS JOIN parameters AS parameter
        LEFT JOIN diabetes_events AS diabetes
            ON event.person_id = diabetes.person_id
        WHERE
            event.clinical_effective_date
            >= parameter.quality_service_start_date
        GROUP BY
            event.person_id,
            event.id,
            event.clinical_effective_date
    ),

    rule_4_qualifiers AS (
        SELECT DISTINCT person_id
        FROM reporting_year_event_context
        WHERE
            latest_diabetes_before_event IS NULL
            OR latest_diabetes_resolved_date > latest_diabetes_before_event
    ),

    before_reporting_year_events AS (
        SELECT
            event.person_id,
            MAX(event.clinical_effective_date) AS latest_diagnosis_date
        FROM ndh_gdm_events AS event
        CROSS JOIN parameters AS parameter
        WHERE
            event.clinical_effective_date
            < parameter.quality_service_start_date
        GROUP BY event.person_id
    ),

    before_reporting_year_diabetes_context AS (
        SELECT
            event.person_id,
            MAX(CASE
                WHEN
                    diabetes.is_general_diabetes_code
                    AND diabetes.clinical_effective_date
                        <= parameter.quality_service_start_date
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_at_service_start,
            MAX(CASE
                WHEN diabetes.is_diabetes_resolved_code
                    THEN diabetes.clinical_effective_date
            END) AS latest_diabetes_resolved_date
        FROM before_reporting_year_events AS event
        CROSS JOIN parameters AS parameter
        LEFT JOIN diabetes_events AS diabetes
            ON event.person_id = diabetes.person_id
        GROUP BY event.person_id
    ),

    rule_5_qualifiers AS (
        SELECT person_id
        FROM before_reporting_year_diabetes_context
        WHERE
            latest_diabetes_at_service_start IS NULL
            OR latest_diabetes_resolved_date
                > latest_diabetes_at_service_start
    ),

    age_at_reference AS (
        SELECT
            person.person_id,
            FLOOR(
                DATEDIFF(
                    'month',
                    person.birth_date_approx,
                    parameter.reference_date
                ) / 12
            ) AS age
        FROM {{ ref('dim_person_birth_death') }} AS person
        CROSS JOIN parameters AS parameter
        WHERE person.birth_date_approx IS NOT NULL
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
                    diabetes.latest_diabetes_resolved_date
                    > diabetes.latest_diabetes_diagnosis_date
                    THEN 3
                WHEN rule_4.person_id IS NOT NULL
                    THEN 4
                WHEN rule_5.person_id IS NOT NULL
                    THEN 5
            END AS qualifying_rule
        FROM ndh_gdm_person_aggregates AS event
        CROSS JOIN parameters AS parameter
        LEFT JOIN diabetes_person_aggregates AS diabetes
            ON event.person_id = diabetes.person_id
        LEFT JOIN age_at_reference AS age
            ON event.person_id = age.person_id
        LEFT JOIN rule_4_qualifiers AS rule_4
            ON event.person_id = rule_4.person_id
        LEFT JOIN rule_5_qualifiers AS rule_5
            ON event.person_id = rule_5.person_id
    )

    SELECT
        person_id,
        'NDH' AS register_name,
        COALESCE(
            passes_entry_rule
            AND qualifying_rule IS NOT NULL,
            FALSE
        ) AS is_on_register,
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

{% endmacro %}
