{#
    Derives the numeric age band ID from an age in years.

    Bands are fixed NHS reporting convention:
      1  = 0
      2  = 1-4
      3-18 = five-year bands from 5-9 up to 80-84
      19 = 85+

    Returns NULL for ages outside 0-130, which includes the 999
    unknown-age sentinel used in SUS.

    Usage:
      {{ age_range_derived('a.age_at_cds_activity_date') }} as age_range_derived
#}

{% macro age_range_derived(age_column) %}
    case
        when {{ age_column }} is null then null
        when {{ age_column }} = 0 then 1
        when {{ age_column }} between 1 and 4 then 2
        when {{ age_column }} between 5 and 84
            then floor(({{ age_column }} - 5) / 5) + 3
        when {{ age_column }} between 85 and 130 then 19
        else null
    end
{% endmacro %}
