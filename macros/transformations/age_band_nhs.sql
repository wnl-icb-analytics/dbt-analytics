{% macro age_band_nhs(age) %}
    {#- NHS age bands from a supplied age in years, the single definition also used by calculate_age_attributes. -#}
    case
        when {{ age }} is null or {{ age }} < 0 then 'Unknown'
        when {{ age }} < 5 then '0-4'
        when {{ age }} < 15 then '5-14'
        when {{ age }} < 25 then '15-24'
        when {{ age }} < 35 then '25-34'
        when {{ age }} < 45 then '35-44'
        when {{ age }} < 55 then '45-54'
        when {{ age }} < 65 then '55-64'
        when {{ age }} < 75 then '65-74'
        when {{ age }} < 85 then '75-84'
        else '85+'
    end
{%- endmacro %}
