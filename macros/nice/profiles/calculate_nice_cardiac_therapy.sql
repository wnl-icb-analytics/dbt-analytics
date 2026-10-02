{% macro calculate_nice_cardiac_therapy(reference='current') %}
WITH reference_population AS (
    {{ nice_reference_population(reference) }}
),
mi_candidates AS (
    SELECT population.person_id, population.reporting_date
    FROM reference_population AS population
    INNER JOIN {{ ref('int_myocardial_infarction_diagnoses_all') }} AS mi
        ON population.person_id = mi.person_id
        AND {{ ltc_register_known_by('mi.clinical_effective_date', 'mi.date_recorded', 'population.reporting_date') }}
    GROUP BY population.person_id, population.reporting_date
),
cardiac_candidates AS (
    SELECT person_id, reporting_date FROM ({{ nice_register('HF', reference) }})
    UNION
    SELECT person_id, reporting_date FROM mi_candidates
),
population AS (
    SELECT population.person_id, population.reporting_date
    FROM reference_population AS population
    INNER JOIN cardiac_candidates AS candidate
        ON population.person_id = candidate.person_id AND population.reporting_date = candidate.reporting_date
),
candidate_keys AS (
    SELECT DISTINCT person_id FROM population
),
orders AS (
    {% for name, model, flag in [
        ('ace_inhibitor', 'int_ace_inhibitor_medications_all', 'TRUE'),
        ('arb', 'int_arb_medications_all', 'TRUE'),
        ('beta_blocker', 'int_beta_blocker_medications_all', 'TRUE'),
        ('hf_licensed_beta_blocker', 'int_beta_blocker_medications_all', 'orders.is_hf_licensed'),
        ('mra', 'int_mra_medications_all', 'TRUE'),
        ('sglt2', 'int_sglt2_medications_all', 'TRUE'),
        ('aspirin', 'int_antiplatelet_medications_all', 'orders.is_aspirin'),
        ('p2y12', 'int_antiplatelet_medications_all', 'orders.is_p2y12_inhibitor'),
        ('clopidogrel', 'int_antiplatelet_medications_all', 'orders.is_clopidogrel'),
        ('antiplatelet', 'int_antiplatelet_medications_all', 'TRUE'),
        ('statin', 'int_lipid_lowering_medications_all', 'orders.is_statin'),
        ('anticoagulant', 'int_anticoagulant_medications_all', 'TRUE')
    ] %}
    SELECT orders.person_id, orders.order_date, '{{ name }}' AS therapy_class
    FROM {{ ref(model) }} AS orders
    INNER JOIN candidate_keys AS candidate ON orders.person_id = candidate.person_id
    WHERE {{ flag }}
        AND orders.order_date BETWEEN '1990-01-01' AND (SELECT MAX(reporting_date) FROM population)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
daily AS (
    SELECT person_id, therapy_class, MAX(order_date) AS order_date
    FROM orders
    GROUP BY person_id, therapy_class, order_date::DATE
),
class_candidates AS (
    SELECT population.person_id, population.reporting_date, classes.value::VARCHAR AS therapy_class
    FROM population
    CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_CONSTRUCT(
        'ace_inhibitor', 'arb', 'beta_blocker', 'hf_licensed_beta_blocker', 'mra', 'sglt2',
        'aspirin', 'p2y12', 'clopidogrel', 'antiplatelet', 'statin', 'anticoagulant'
    ))) AS classes
),
selected AS (
    SELECT candidate.person_id, candidate.reporting_date, candidate.therapy_class, daily.order_date
    FROM class_candidates AS candidate
    ASOF JOIN daily
        MATCH_CONDITION (candidate.reporting_date >= daily.order_date::DATE)
        ON candidate.person_id = daily.person_id AND candidate.therapy_class = daily.therapy_class
),
antiplatelet_chemicals AS (
    SELECT population.person_id, population.reporting_date,
        COUNT(DISTINCT LEFT(orders.bnf_code, 9)) AS antiplatelet_chemical_count_in_period
    FROM population
    LEFT JOIN {{ ref('int_antiplatelet_medications_all') }} AS orders
        ON population.person_id = orders.person_id
        AND orders.order_date::DATE BETWEEN DATEADD(month, -6, population.reporting_date) AND population.reporting_date
    GROUP BY population.person_id, population.reporting_date
),
therapy_dates AS (
    SELECT person_id, reporting_date,
        {% for name in ['ace_inhibitor', 'arb', 'beta_blocker', 'hf_licensed_beta_blocker', 'mra', 'sglt2', 'aspirin', 'p2y12', 'clopidogrel', 'antiplatelet', 'statin', 'anticoagulant'] %}
        MAX(IFF(therapy_class = '{{ name }}', order_date, NULL)) AS latest_{{ name }}_order_date{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM selected
    GROUP BY person_id, reporting_date
)
SELECT therapy_dates.*,
    GREATEST_IGNORE_NULLS(latest_ace_inhibitor_order_date, latest_arb_order_date) AS latest_ras_order_date,
    chemicals.antiplatelet_chemical_count_in_period
FROM therapy_dates
LEFT JOIN antiplatelet_chemicals AS chemicals
    ON therapy_dates.person_id = chemicals.person_id AND therapy_dates.reporting_date = chemicals.reporting_date
{% endmacro %}
