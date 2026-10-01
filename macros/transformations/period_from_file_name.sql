{# Extract a reporting-month DATE from an SDL submission file name, as the
   period-of-last-resort when a row has neither a stated period nor a usable
   activity date. SDL community file names are free-form provider naming, so
   several formats appear; tried in order, each gated to a plausible month so
   the leading file-id digits cannot create a false match:
     1. Calendar YYYY-MM / YYYY_MM / YYYY MM    (e.g. '2017_06', '2026-05')
     2. Mon-YY / Mon YY                          (e.g. 'Jul-19', 'May 19')
     3. Financial-month + FY token 'M# 2627'     (e.g. 'M2 2627' -> Apr+1 FY26/27)
   Returns the first day of the resolved calendar month, or NULL. #}
{% macro period_from_file_name(fn) %}
    coalesce(
        -- 1. Calendar year-month. Year and month are taken from the SAME
        --    anchored match (group 1 of each) so a stray '20xx' elsewhere in
        --    the name cannot be spliced onto a different month.
        try_to_date(
            regexp_substr({{ fn }}, '(20[0-9]{2})[-_ ][01][0-9]', 1, 1, 'e', 1) || '-'
            || regexp_substr({{ fn }}, '20[0-9]{2}[-_ ]([01][0-9])', 1, 1, 'e', 1) || '-01',
            'YYYY-MM-DD'),
        -- 2. Mon-YY (RR infers century: 00-49 -> 20xx)
        try_to_date(
            '01-' || regexp_substr({{ fn }},
                '(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]([0-9]{2})([^0-9]|$)',
                1, 1, 'ie', 1)
            || '-' || regexp_substr({{ fn }},
                '(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]([0-9]{2})([^0-9]|$)',
                1, 1, 'ie', 2),
            'DD-MON-RR'),
        -- 3. Financial month + 'YYYY' FY token (e.g. 'M2 2627'): FY start year +
        --    financial month -> calendar month (fin 1 = April).
        case
            when regexp_substr({{ fn }}, '[ _]M([0-1]?[0-9])[ _].*[ _](2[0-9])(2[0-9])([ _])', 1, 1, 'e', 3) is not null
                 and regexp_substr({{ fn }}, '[ _]M([0-1]?[0-9])[ _]', 1, 1, 'e', 1)::int between 1 and 12
            then dateadd('month',
                regexp_substr({{ fn }}, '[ _]M([0-1]?[0-9])[ _]', 1, 1, 'e', 1)::int - 1,
                ('20' || regexp_substr({{ fn }}, '[ _]M[0-1]?[0-9][ _].*[ _](2[0-9])2[0-9][ _]', 1, 1, 'e', 1) || '-04-01')::date)
        end
    )
{% endmacro %}
