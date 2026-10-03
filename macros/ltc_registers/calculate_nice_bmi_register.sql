{% macro calculate_nice_bmi_register(register_type, reference='current', reference_date_expr='CURRENT_DATE()', reference_dates=none) %}
{#-
    Calculate IND237 or IND238 membership using the shared adult BMI classification.
    Args: register_type is overweight or obesity; reference_dates supplies reference_date rows.
    Returns: one adult register member per person and reference_date with BMI category evidence.
-#}
{% if register_type not in ['overweight', 'obesity'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE BMI register: ' ~ register_type) }}
{% endif %}
-- NICE IND237: https://www.nice.org.uk/indicators/ind237
-- NICE IND238: https://www.nice.org.uk/indicators/ind238
-- Uses NG246 higher-risk groups, rather than lower thresholds for everyone not recorded as White.
WITH reference_dates AS (
    {{ ltc_register_reference_dates(reference_date_expr, reference_dates) }}
), population AS (
    SELECT population.person_id, dates.reference_date, population.age, population.practice_code
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN reference_dates AS dates ON population.reporting_date = dates.reference_date
    WHERE population.age >= 18
), people AS (
    SELECT DISTINCT person_id FROM population
), bmi_candidates AS (
    SELECT bmi.person_id, bmi.id, bmi.clinical_effective_date, bmi.date_recorded,
        bmi.bmi_value, bmi.bmi_source
    FROM {{ ref('int_bmi_all') }} AS bmi
    INNER JOIN people ON bmi.person_id = people.person_id
    -- Older events cannot qualify at any requested date or supersede a newer result.
    WHERE bmi.clinical_effective_date::DATE > DATEADD(month, -12, (SELECT MIN(reference_date) FROM reference_dates))
), heights AS (
    -- Match the contributing height selected by int_bmi_all; its recorded date also bounds knowledge.
    SELECT obs.person_id, obs.clinical_effective_date, obs.date_recorded
    FROM ({{ get_observations("'HEIGHT'") }}) AS obs
    INNER JOIN people ON obs.person_id = people.person_id
    WHERE obs.clinical_effective_date IS NOT NULL
        AND TRY_CAST(obs.result_value AS FLOAT) BETWEEN 50 AND 250
        AND obs.age_at_event >= 18
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY obs.person_id, obs.clinical_effective_date ORDER BY obs.id DESC
    ) = 1
), bmi_records AS (
    SELECT bmi.person_id, bmi.clinical_effective_date::DATE AS bmi_date,
        bmi.bmi_value, bmi.bmi_source,
        IFF(bmi.bmi_source = 'calculated',
            GREATEST({{ ltc_known_date('bmi.clinical_effective_date', 'bmi.date_recorded') }},
                COALESCE(height.date_recorded::DATE, bmi.clinical_effective_date::DATE)),
            {{ ltc_known_date('bmi.clinical_effective_date', 'bmi.date_recorded') }}) AS known_date,
        TO_VARCHAR(bmi.clinical_effective_date, 'YYYY-MM-DD HH24:MI:SS.FF9')
            || '|' || TO_VARCHAR(bmi.id) AS record_key
    FROM bmi_candidates AS bmi
    ASOF JOIN heights AS height
        MATCH_CONDITION (bmi.clinical_effective_date >= height.clinical_effective_date)
        ON bmi.person_id = height.person_id
), latest_bmi AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM bmi_records') }}
), higher_risk_ethnicity AS (
    -- The shared risk profile prioritises any higher-risk record, even after a later White record.
    SELECT ethnicity.person_id,
        MIN({{ ltc_known_date('ethnicity.clinical_effective_date', 'ethnicity.date_recorded') }}) AS first_known_date
    FROM {{ ref('int_ethnicity_qof_all') }} AS ethnicity
    INNER JOIN people ON ethnicity.person_id = people.person_id
    WHERE ethnicity.is_bame
    GROUP BY ethnicity.person_id
), assessed AS (
    SELECT population.person_id, population.reference_date, population.age, population.practice_code,
        bmi.bmi_date, bmi.bmi_value, bmi.bmi_source,
        COALESCE(ethnicity.first_known_date <= population.reference_date, FALSE) AS requires_lower_bmi_thresholds,
        {{ bmi_category('bmi.bmi_value', 'requires_lower_bmi_thresholds') }} AS bmi_category,
        {{ bmi_category('bmi.bmi_value', 'requires_lower_bmi_thresholds', 'risk_sort_key') }} AS bmi_risk_sort_key
    FROM population
    INNER JOIN latest_bmi AS latest ON population.person_id = latest.person_id
        AND population.reference_date = latest.reference_date
    INNER JOIN bmi_records AS bmi ON latest.person_id = bmi.person_id
        AND latest.record_key = bmi.record_key
    LEFT JOIN higher_risk_ethnicity AS ethnicity ON population.person_id = ethnicity.person_id
    WHERE bmi.bmi_date > DATEADD(month, -12, population.reference_date)
        AND bmi.bmi_value BETWEEN 10 AND 150
)
SELECT person_id, reference_date, age, practice_code, TRUE AS is_on_register,
    bmi_date AS latest_bmi_date, bmi_value, bmi_source, requires_lower_bmi_thresholds,
    bmi_category, bmi_risk_sort_key
FROM assessed
WHERE bmi_category IN (
    {% if register_type == 'overweight' %}'Overweight', {% endif %}
    'Obese Class I', 'Obese Class II', 'Obese Class III'
)
{% endmacro %}
