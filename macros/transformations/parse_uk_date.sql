{# Parse a string column that may use mixed date formats from spreadsheet exports.
   Returns DATE. Each branch is gated by a regex so only well-formed inputs reach
   each parser — avoids Snowflake's lenient auto-format treating '22-12-14' as
   year 0022, or DD-MON-YY parsing '24-Apr-24' as year 0024.

   Branch order:
     1. ISO YYYY-MM-DD (with optional time suffix)
     2. US MM/DD/YYYY HH:MI[:SS] AM/PM (SQL Server export). Tested before the UK
        branch because the AM/PM suffix is the only tell; otherwise day <= 12
        would swap day and month and day > 12 would be NULL.
     3. UK DD/MM/YYYY (with optional time suffix; ambiguous dates read as UK)
     4. UK DD-MM-YYYY
     5. DD-Mon-YYYY  (Excel 4-digit year)
     6. DD-Mon-YY    (Excel default; RR infers century: 00-49 → 20xx, 50-99 → 19xx)
     7. UK DD/MM/YY or DD-MM-YY (century from TWO_DIGIT_CENTURY_START)
     8. Excel serial day number, accepted only for 2000-01-01 to 2030-12-31
        (36526-47848) so other integers are not read as dates.
   Excel time-only artefacts (e.g. '00:00.0') match no branch → NULL.
#}
{# NB: Snowflake RLIKE treats \d literally — use POSIX [0-9] / [A-Za-z]. #}
{% macro parse_uk_date(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'
            then try_to_date(left({{ col }}, 10), 'YYYY-MM-DD')
        when upper({{ col }}) rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}.* ?(AM|PM)$'
            then try_to_date(split_part({{ col }}, ' ', 1), 'MM/DD/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*'
            then try_to_date(split_part({{ col }}, ' ', 1), 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}.*'
            then try_to_date(split_part({{ col }}, ' ', 1), 'DD-MM-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_date({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_date({{ col }}, 'DD-MON-YY')
        when {{ col }} rlike '^[0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2}$'
            then try_to_date(replace({{ col }}, '-', '/'), 'DD/MM/YY')
        when {{ col }} rlike '^[34][0-9]{4}([.][0-9]+)?$'
            and try_to_number({{ col }}, 18, 6) between 36526 and 47848
            then dateadd('day', floor(try_to_number({{ col }}, 18, 6)), '1899-12-30'::date)
    end
{% endmacro %}

{% macro parse_uk_timestamp(col) %}
    case
        when {{ col }} rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2}.*'
            then try_to_timestamp({{ col }})
        when upper({{ col }}) rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}.* ?(AM|PM)$'
            then try_to_timestamp({{ col }}, 'MM/DD/YYYY HH12:MI:SS AM')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4} [0-9]{1,2}:[0-9]{2}.*'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY HH24:MI:SS')
        when {{ col }} rlike '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
            then try_to_timestamp({{ col }}, 'DD/MM/YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}.*'
            then try_to_timestamp({{ col }}, 'DD-MON-YYYY')
        when {{ col }} rlike '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}'
            then try_to_timestamp({{ col }}, 'DD-MON-YY')
    end
{% endmacro %}
