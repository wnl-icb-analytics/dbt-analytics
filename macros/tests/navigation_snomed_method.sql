{% test navigation_snomed_method(model, code_column='snomed_code') %}
    select {{ code_column }}, snomed_mapping_method
    from {{ model }}
    where (snomed_mapping_method is null) <> ({{ code_column }} is null)
{% endtest %}
