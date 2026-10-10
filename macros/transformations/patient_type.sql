{#
    Classifies an attendance as adult or child from age in years.

      CHI = 0-18
      ADU = 19-130, and 999
      ERR = anything else

    NOTE: 999 is the SUS unknown-age sentinel, not a real age. The legacy
    view classified it as ADU. Retained here for parity, but it means
    unknown-age records are silently counted as adults -- worth revisiting
    with the commissioning team.

    Usage:
      {{ patient_type('a.age_at_cds_activity_date') }} as patient_type
#}

{% macro patient_type(age_column, unknown_age_sentinel=999) %}
    case
        when {{ age_column }} between 0 and 18 then 'CHI'
        when {{ age_column }} between 19 and 130 then 'ADU'
        when {{ age_column }} = {{ unknown_age_sentinel }} then 'ADU'
        else 'ERR'
    end
{% endmacro %}
