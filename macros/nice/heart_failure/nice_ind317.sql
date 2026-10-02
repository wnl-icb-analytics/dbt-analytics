{% macro nice_ind317(reference='current') %}
WITH assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        {% for name in ['ras', 'beta_blocker', 'hf_licensed_beta_blocker', 'mra', 'sglt2'] %}
        therapy.latest_{{ name }}_order_date,
        COALESCE(therapy.latest_{{ name }}_order_date::DATE BETWEEN DATEADD(month, -6, population.reporting_date)
            AND population.reporting_date, FALSE) AS is_{{ name }}_in_period{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM ({{ nice_register('HF', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id AND register.reporting_date = population.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_cardiac_therapy', reference) }} AS therapy
        ON population.person_id = therapy.person_id AND population.reporting_date = therapy.reporting_date
    WHERE register.is_on_hfref_register
),
result AS (
    SELECT *, is_ras_in_period AND is_beta_blocker_in_period AND is_mra_in_period AND is_sglt2_in_period AS is_in_numerator
    FROM assessed
)
SELECT
    person_id,
    'IND317' AS indicator_id,
    'Heart failure: 4 pillars (HFrEF)' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Heart failure with reduced ejection fraction' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    {% for name in ['ras', 'beta_blocker', 'hf_licensed_beta_blocker', 'mra', 'sglt2'] %}
    latest_{{ name }}_order_date,
    is_{{ name }}_in_period,
    {% endfor %}
    GREATEST_IGNORE_NULLS(latest_ras_order_date, latest_beta_blocker_order_date, latest_mra_order_date, latest_sglt2_order_date)::DATE AS latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_TREATED_IN_PERIOD') AS indicator_status
FROM result
{% endmacro %}
