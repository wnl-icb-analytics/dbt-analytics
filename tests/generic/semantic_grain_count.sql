{% test semantic_grain_count(model, source_model, entity, metric, grain_key, source_filter=none) %}
{{ config(static_analysis='off') }}

-- The semantic metric must count each source entity once at its declared grain.
SELECT actual.actual_count, expected.expected_count
FROM (
    SELECT {{ metric }} AS actual_count
    FROM SEMANTIC_VIEW({{ model }} METRICS {{ entity }}.{{ metric }})
) actual
CROSS JOIN (
    SELECT COUNT(DISTINCT {{ grain_key }}) AS expected_count
    FROM {{ ref(source_model) }}
    {% if source_filter %}WHERE {{ source_filter }}{% endif %}
) expected
WHERE COALESCE(actual.actual_count, 0) <> expected.expected_count
{% endtest %}
