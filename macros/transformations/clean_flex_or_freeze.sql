{# Standardise the DLP Flex/Freeze flag to exactly 'Flex' or 'Freeze'.
   Folds case variants (FLEX/flex), 'Frozen', and the DLP spec's source terms
   (PRIMARY = Flex, REFRESH = Freeze). Anything else (blanks, '1'/'0' booleans,
   stray text) -> NULL. Used by the SDL DLP feeds (COMOPL, REF) and MHCORL. #}
{% macro clean_flex_or_freeze(col) %}
    case
        when upper(trim({{ col }})) in ('FLEX', 'PRIMARY')              then 'Flex'
        when upper(trim({{ col }})) in ('FREEZE', 'FROZEN', 'REFRESH')  then 'Freeze'
    end
{% endmacro %}
