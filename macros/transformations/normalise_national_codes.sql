{# Normalisation helpers for the community PLD feeds (COMOPL, REF). Source code
   fields mix national codes, leading-zero-stripped numerics, free text and
   genuine provider-local codes. These coerce the recognisable national codes
   to their canonical form and pass everything else through unchanged (so local
   codes survive for downstream local lookups). #}

{# Zero-pad a bare integer of <= width digits to the national-code width
   (e.g. '6' -> '06'); strip a stray leading '?'; longer or non-numeric values
   (genuine local codes / free text) pass through trimmed. #}
{% macro nc_pad(col, width) %}
    case
        when regexp_replace(trim({{ col }}), '^[?]+', '') rlike '^[0-9]{1,{{ width }}}$'
            then lpad(regexp_replace(trim({{ col }}), '^[?]+', ''), {{ width }}, '0')
        else nullif(regexp_replace(trim({{ col }}), '^[?]+', ''), '')
    end
{% endmacro %}

{# Gender -> national code (1 Male, 2 Female, 9 Indeterminate, X Not known).
   Maps the common word/letter variants; bare 1/2/9/X pass through; anything
   else -> trimmed passthrough. #}
{% macro nc_gender(col) %}
    case
        when upper(trim({{ col }})) in ('1', 'M', 'MALE')                        then '1'
        when upper(trim({{ col }})) in ('2', 'F', 'FEMALE')                      then '2'
        when upper(trim({{ col }})) in ('9', 'INDETERMINATE')                    then '9'
        when upper(trim({{ col }})) in ('X', '0', 'UNKNOWN', 'NOT KNOWN', 'NOT STATED', 'NOT SPECIFIED')
                                                                                 then 'X'
        else nullif(trim({{ col }}), '')
    end
{% endmacro %}

{# Priority type -> national code (1 Routine, 2 Urgent, 3 Two Week Wait).
   Emergency/immediate variants fold to Urgent (2); local descriptions pass
   through. #}
{% macro nc_priority(col) %}
    case
        when upper(trim({{ col }})) in ('1', 'ROUTINE')                          then '1'
        when upper(trim({{ col }})) like 'URGENT%'
          or upper(trim({{ col }})) like 'EMERGENC%'
          or upper(trim({{ col }})) = '2'                                        then '2'
        when upper(trim({{ col }})) in ('3', 'TWO WEEK WAIT', '2 WEEK WAIT')     then '3'
        else nullif(trim({{ col }}), '')
    end
{% endmacro %}
