{#
    NHS Talking Therapies (IAPT data set) IDS202 procedures are SNOMED CT expressions (user
    guidance v1.6.1 p40). Only two whole-string forms are interpreted: one concept with an
    optional |term|, and one concept refined only by 408730004 |Procedure context|. Any other
    expression keeps its text and is marked unrecognised; nothing is asserted from it.
#}

{% macro iapt_procedure_expression_type(expression) -%}
    case
        when {{ expression }} is null then null
        when regexp_like({{ expression }}, '[0-9]{6,18}( *[|][^|]*[|])?') then 'bare_concept'
        when regexp_like(
            {{ expression }}
            , '[0-9]{6,18}( *[|][^|]*[|])? *: *408730004( *[|][^|]*[|])? *= *[0-9]{6,18}( *[|][^|]*[|])?'
        ) then 'procedure_context'
        when regexp_like({{ expression }}, '[0-9]{6,18}([^0-9].*)?') then 'unrecognised_refinement'
        else 'unparsed'
    end
{%- endmacro %}

{% macro iapt_procedure_focus_code(expression) -%}
    {#- Leading concept. It is the focus only for bare_concept and procedure_context forms. -#}
    iff(
        {{ iapt_procedure_expression_type(expression) }} in ('bare_concept', 'procedure_context', 'unrecognised_refinement')
        , regexp_substr({{ expression }}, '^[0-9]{6,18}'), null
    )
{%- endmacro %}

{% macro iapt_procedure_context_code(expression) -%}
    iff(
        {{ iapt_procedure_expression_type(expression) }} = 'procedure_context'
        , regexp_substr({{ expression }}, '408730004( *[|][^|]*[|])? *= *([0-9]{6,18})', 1, 1, 'e', 2), null
    )
{%- endmacro %}

{% macro iapt_asserted_procedure_code(expression) -%}
    {#- A procedure is asserted only as a bare concept or with context 385658003 |Done| alone. -#}
    case {{ iapt_procedure_expression_type(expression) }}
        when 'bare_concept' then regexp_substr({{ expression }}, '^[0-9]{6,18}')
        when 'procedure_context' then iff(
            {{ iapt_procedure_context_code(expression) }} = '385658003'
            , regexp_substr({{ expression }}, '^[0-9]{6,18}'), null
        )
    end
{%- endmacro %}
