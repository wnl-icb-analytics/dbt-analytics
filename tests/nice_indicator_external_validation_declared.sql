-- Every NICE measure in the roll-up must declare meta.indicator.external_validation, so new
-- indicators are checked against a published figure (docs/nice-benchmarks.md) before release.
-- Read from model metadata at compile time, like nice_indicator_status_ids_defined.
{% set metadata = extract_indicator_metadata() %}
{% set declared = [] %}
{% for ind in metadata.indicators %}
    {% if ind.validation_status in ('validated', 'not_validated') %}
        {% do declared.append(ind.indicator_id) %}
    {% endif %}
{% endfor %}

SELECT
    indicator_id,
    COUNT(*) AS row_count
FROM {{ ref('fct_person_nice_indicator_status') }}
WHERE indicator_id NOT IN (
    {%- for id in declared %}
    '{{ id }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
GROUP BY indicator_id
