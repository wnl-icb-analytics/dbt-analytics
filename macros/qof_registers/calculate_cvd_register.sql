{% macro calculate_cvd_register(reference_date_expr='CURRENT_DATE()') %}
    {# Pair: fct_person_cvd_register.sql. This macro is strict as-of; the live fact includes future-dated component membership. #}
    {#
    Calculates QOF v51 CD_REG status at a reference date by composing the CHD
    and stroke/TIA register macros.

    Unlike its live sibling fct_person_cvd_register.sql, this macro is strictly
    as-of: component evidence dated or recorded after reference_date_expr is
    excluded by the component macros.
    #}

    WITH chd_register AS (
        {{ calculate_chd_register(reference_date_expr=reference_date_expr) }}
    ),

    stroke_tia_register AS (
        {{ calculate_stroke_tia_register(reference_date_expr=reference_date_expr) }}
    ),

    cvd_members AS (
        SELECT person_id
        FROM chd_register
        WHERE is_on_register = TRUE

        UNION

        SELECT person_id
        FROM stroke_tia_register
        WHERE is_on_register = TRUE
    )

    SELECT
        person_id,
        'Cardiovascular Disease' AS register_name,
        TRUE AS is_on_register
    FROM cvd_members

{% endmacro %}
