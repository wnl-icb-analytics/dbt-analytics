-- Every indicator_id in the roll-up must have a meta.indicator definition in
-- the project. Checked against the model metadata at compile time rather than
-- def_indicator, because merge-queue builds defer that table to production,
-- where it lags behind newly added indicators until the next full build.
{% set metadata = extract_indicator_metadata() %}
{% set defined_ids = [] %}
{% for ind in metadata.indicators %}
    {% do defined_ids.append(ind.indicator_id) %}
{% endfor %}

SELECT
    indicator_id,
    COUNT(*) AS row_count
FROM {{ ref('fct_person_nice_indicator_status') }}
WHERE indicator_id NOT IN (
    {%- for id in defined_ids %}
    '{{ id }}'{% if not loop.last %},{% endif %}
    {%- endfor %}
)
GROUP BY indicator_id
