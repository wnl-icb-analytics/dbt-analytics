{% macro calculate_age_attributes(birth_date_field, reference_date_field, birth_year_field=none, birth_month_field=none, is_deceased_field=none, death_date_field=none) %}
    {#
    Calculates age and related attributes for a person at a given reference date.

    Parameters:
        birth_date_field: approximate birth date field
        reference_date_field: date to calculate age at (e.g., analysis_month or CURRENT_DATE)
        birth_year_field: optional, for age_at_least calculation
        birth_month_field: optional, for age_at_least calculation
        is_deceased_field: optional, to handle deceased persons
        death_date_field: optional, for age at death calculations

    Returns: CTE-style columns for age, age_at_least, age_bands, life_stage, and school age flags
    #}

    -- Age calculation (current or at death)
    {% if is_deceased_field and death_date_field %}
    CASE
        WHEN {{ is_deceased_field }} AND {{ death_date_field }} IS NOT NULL
            THEN FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ death_date_field }}) / 12)
        WHEN {{ birth_date_field }} IS NOT NULL
            THEN FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ reference_date_field }}) / 12)
        ELSE NULL
    END AS age,
    {% else %}
    CASE
        WHEN {{ birth_date_field }} IS NOT NULL
            THEN FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ reference_date_field }}) / 12)
        ELSE NULL
    END AS age,
    {% endif %}

    -- Age at least (conservative calculation using end of birth month)
    {% if birth_year_field and birth_month_field %}
    CASE
        WHEN {{ birth_year_field }} IS NOT NULL AND {{ birth_month_field }} IS NOT NULL THEN
            {% if is_deceased_field and death_date_field %}
            CASE
                WHEN {{ is_deceased_field }} AND {{ death_date_field }} IS NOT NULL THEN
                    CASE
                        WHEN {{ death_date_field }} >= DATEADD(
                                year,
                                DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ death_date_field }}),
                                LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1))
                             )
                        THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ death_date_field }})
                        ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ death_date_field }}) - 1
                    END
                ELSE
                    CASE
                        WHEN {{ reference_date_field }} >= DATEADD(
                                year,
                                DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }}),
                                LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1))
                             )
                        THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }})
                        ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }}) - 1
                    END
            END
            {% else %}
            CASE
                WHEN {{ reference_date_field }} >= DATEADD(
                        year,
                        DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }}),
                        LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1))
                     )
                THEN DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }})
                ELSE DATEDIFF(year, LAST_DAY(DATE_FROM_PARTS({{ birth_year_field }}, {{ birth_month_field }}, 1)), {{ reference_date_field }}) - 1
            END
            {% endif %}
        ELSE NULL
    END AS age_at_least,
    {% endif %}

    -- 5-year age bands
    {% set age_calc %}
        {% if is_deceased_field and death_date_field %}
        CASE
            WHEN {{ is_deceased_field }} AND {{ death_date_field }} IS NOT NULL
                THEN FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ death_date_field }}) / 12)
            WHEN {{ birth_date_field }} IS NOT NULL
                THEN FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ reference_date_field }}) / 12)
            ELSE NULL
        END
        {% else %}
        FLOOR(DATEDIFF(month, {{ birth_date_field }}, {{ reference_date_field }}) / 12)
        {% endif %}
    {% endset %}

    CASE
        WHEN {{ birth_date_field }} IS NULL THEN 'Unknown'
        WHEN ({{ age_calc }}) < 0 THEN 'Unknown'
        WHEN ({{ age_calc }}) >= 85 THEN '85+'
        ELSE TO_VARCHAR(FLOOR(({{ age_calc }}) / 5) * 5) || '-' || TO_VARCHAR(FLOOR(({{ age_calc }}) / 5) * 5 + 4)
    END AS age_band_5y,

    -- 10-year age bands
    CASE
        WHEN {{ birth_date_field }} IS NULL THEN 'Unknown'
        WHEN ({{ age_calc }}) < 0 THEN 'Unknown'
        WHEN ({{ age_calc }}) >= 80 THEN '80+'
        ELSE TO_VARCHAR(FLOOR(({{ age_calc }}) / 10) * 10) || '-' || TO_VARCHAR(FLOOR(({{ age_calc }}) / 10) * 10 + 9)
    END AS age_band_10y,

    -- NHS age bands
    {{ age_band_nhs('(' ~ age_calc ~ ')') }} AS age_band_nhs,

    -- ONS age bands
    CASE
        WHEN {{ birth_date_field }} IS NULL THEN 'Unknown'
        WHEN ({{ age_calc }}) < 0 THEN 'Unknown'
        WHEN ({{ age_calc }}) < 5 THEN '0-4'
        WHEN ({{ age_calc }}) < 10 THEN '5-9'
        WHEN ({{ age_calc }}) < 15 THEN '10-14'
        WHEN ({{ age_calc }}) < 20 THEN '15-19'
        WHEN ({{ age_calc }}) < 25 THEN '20-24'
        WHEN ({{ age_calc }}) < 30 THEN '25-29'
        WHEN ({{ age_calc }}) < 35 THEN '30-34'
        WHEN ({{ age_calc }}) < 40 THEN '35-39'
        WHEN ({{ age_calc }}) < 45 THEN '40-44'
        WHEN ({{ age_calc }}) < 50 THEN '45-49'
        WHEN ({{ age_calc }}) < 55 THEN '50-54'
        WHEN ({{ age_calc }}) < 60 THEN '55-59'
        WHEN ({{ age_calc }}) < 65 THEN '60-64'
        WHEN ({{ age_calc }}) < 70 THEN '65-69'
        WHEN ({{ age_calc }}) < 75 THEN '70-74'
        WHEN ({{ age_calc }}) < 80 THEN '75-79'
        WHEN ({{ age_calc }}) < 85 THEN '80-84'
        ELSE '85+'
    END AS age_band_ons,

    -- ESP 2013 age bands (matches European Standard Population 2013 structure)
    CASE
        WHEN {{ birth_date_field }} IS NULL THEN 'Unknown'
        WHEN ({{ age_calc }}) < 0 THEN 'Unknown'
        WHEN ({{ age_calc }}) < 1 THEN '<1'
        WHEN ({{ age_calc }}) < 5 THEN '1-4'
        WHEN ({{ age_calc }}) >= 95 THEN '95+'
        WHEN ({{ age_calc }}) >= 90 THEN '90-94'
        WHEN ({{ age_calc }}) >= 85 THEN '85-89'
        ELSE TO_VARCHAR(FLOOR(({{ age_calc }}) / 5) * 5) || '-' || TO_VARCHAR(FLOOR(({{ age_calc }}) / 5) * 5 + 4)
    END AS age_band_esp,

    -- Life stage
    CASE
        WHEN {{ birth_date_field }} IS NULL THEN 'Unknown'
        WHEN ({{ age_calc }}) < 0 THEN 'Unknown'
        WHEN ({{ age_calc }}) < 1 THEN 'Infant'
        WHEN ({{ age_calc }}) < 4 THEN 'Toddler'
        WHEN ({{ age_calc }}) < 13 THEN 'Child'
        WHEN ({{ age_calc }}) < 20 THEN 'Adolescent'
        WHEN ({{ age_calc }}) < 25 THEN 'Young Adult'
        WHEN ({{ age_calc }}) < 60 THEN 'Adult'
        WHEN ({{ age_calc }}) < 75 THEN 'Older Adult'
        WHEN ({{ age_calc }}) < 85 THEN 'Elderly'
        ELSE 'Very Elderly'
    END AS age_life_stage,

    -- School age flags
    {{ calculate_school_age_flags(birth_date_field, reference_date_field) }}

{% endmacro %}
