-- Fusion's unit comparator rounds FLOAT values to one decimal place.
-- Check the shared conversion macro numerically on small synthetic inputs.
{% for measurement in ['haemoglobin_est', 'bilirubin_level'] %}
{% set conversion_sql %}
    {{ standardise_count_observation('synthetic_observations', measurement) }}
{% endset %}
{% set conversion_sql = conversion_sql
    | replace(ref('observation_unit_rules') | string, 'synthetic_unit_rules')
    | replace(ref('observation_standard_units') | string, 'synthetic_standard_units')
    | replace(ref('observation_value_bounds') | string, 'synthetic_value_bounds') %}

{% if not loop.first %}UNION ALL{% endif %}
SELECT id
FROM (
    WITH synthetic_observations AS (
        SELECT column1::VARCHAR AS id, column2::FLOAT AS result_value,
            column3::VARCHAR AS result_unit_code, column3::VARCHAR AS result_unit_display,
            column4::NUMBER(20,9) AS expected_value
        FROM VALUES
        {% if measurement == 'haemoglobin_est' %}
            ('SYN_PREC_K', 12.345678, 'g/dL', 123.45678),
            ('SYN_PREC_I', 12.345678, NULL, 123.45678),
            ('SYN_RAW', 123.456789, 'g/L', 123.456789)
        {% else %}
            ('SYN_BILI_CONVERT', 100000, 'pmol/L', 0.1),
            ('SYN_BILI_PREC', 123456.7, 'pmol/L', 0.123457)
        {% endif %}
    ),
    synthetic_unit_rules AS (
        SELECT '{{ measurement }}'::VARCHAR AS definition_name,
            column1::VARCHAR AS convert_from_unit, column2::VARCHAR AS convert_to_unit,
            column3::FLOAT AS multiply_by, 0::FLOAT AS pre_offset, 0::FLOAT AS post_offset
        FROM VALUES
        {% if measurement == 'haemoglobin_est' %}
            ('g/dL', 'g/L', 10), ('g/L', 'g/L', 1)
        {% else %}
            ('pmol/L', 'umol/L', 0.000001)
        {% endif %}
    ),
    synthetic_standard_units AS (
        SELECT '{{ measurement }}'::VARCHAR AS definition_name, TRUE AS primary_unit,
            '{{ "g/L" if measurement == "haemoglobin_est" else "umol/L" }}'::VARCHAR AS unit
    ),
    synthetic_value_bounds AS (
        SELECT '{{ measurement }}'::VARCHAR AS definition_name,
        {% if measurement == 'haemoglobin_est' %}
            25::FLOAT AS absolute_lower, 300::FLOAT AS absolute_upper,
            -1::NUMBER AS canonical_exponent, 25::FLOAT AS biological_lower,
            300::FLOAT AS biological_upper
        {% else %}
            0::FLOAT AS absolute_lower, 1500::FLOAT AS absolute_upper,
            NULL::NUMBER AS canonical_exponent, NULL::FLOAT AS biological_lower,
            1000::FLOAT AS biological_upper
        {% endif %}
    ),
    {{ conversion_sql }}
    SELECT id
    FROM flagged
    WHERE inferred_value IS NULL
        OR ABS(inferred_value - expected_value) > 0.000000001
)
{% endfor %}
