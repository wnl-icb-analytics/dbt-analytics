{% macro nice_indicator_explorer_achievement(reference='current') %}
{#-
    NICE achievement by practice, split by each inequality breakdown, for the
    NICE Indicator Explorer app. People with a National Data Opt-Out or Type 1
    opt-out are excluded.
    Args: reference is current or by_month.
    Returns: one row per reporting date, indicator, practice, breakdown and value.
    Current only: numerator_at_age_rates sums, over the people in the row, the
    achievement rate of everyone in the same ESP 2013 age band on that indicator
    (indirect age standardisation); variance_at_age_rates sums p(1 - p).
-#}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{%- set by_month = reference == 'by_month' -%}

{#- (column, breakdown label); each breakdown partitions the denominator -#}
{%- set breakdowns = [
    ('imd_quintile', 'Deprivation (IMD 2025)'),
    ('ethnicity', 'Ethnicity'),
    ('ethnicity_detail', 'Ethnicity (detailed)'),
    ('age_band', 'Age band'),
    ('sex', 'Sex'),
    ('segment', 'Population segment'),
    ('ltc_count_band', 'Long-term conditions'),
    ('frailty', 'Frailty (eFI2)'),
    ('learning_disability', 'Learning disability'),
    ('severe_mental_illness', 'Severe mental illness'),
    ('interpreter_needed', 'Interpreter needed'),
] -%}
{#- Current status only: no history of these attributes -#}
{%- if not by_month -%}
    {%- do breakdowns.append(('multimorbidity_score', 'Cambridge Multimorbidity Score')) -%}
    {%- do breakdowns.append(('residence', 'Housebound or care home')) -%}
{%- endif -%}

WITH status AS (
    SELECT
        s.person_id,
        s.reporting_date,
        s.indicator_id,
        s.{{ 'practice_code' if by_month else 'current_practice_code' }} AS practice_code,
        s.age,
        s.is_in_numerator,
        s.indicator_status
    FROM {{ ref('fct_person_nice_indicator_status_by_month' if by_month else 'fct_person_nice_indicator_status') }} AS s
    INNER JOIN {{ ref('dim_person_secondary_use_allowed') }} AS a
        ON s.person_id = a.person_id
    WHERE s.is_in_denominator
),

person_dates AS (
    SELECT DISTINCT person_id, reporting_date
    FROM status
),

segments AS (
    SELECT
        person_id,
        {% if by_month %}end_date AS reporting_date,{% endif %}
        segment_number,
        segment_name,
        ltc_count
    FROM {{ ref('fct_person_segment_by_month' if by_month else 'fct_person_segment') }}
    WHERE segment_number IS NOT NULL
),

frailty AS (
    SELECT
        person_id,
        {% if by_month %}end_date AS reporting_date,{% endif %}
        category
    FROM {{ ref('fct_person_efi2_by_month' if by_month else 'fct_person_efi2') }}
),

registers AS (
    SELECT
        person_id,
        {% if by_month %}month_end_date AS reporting_date,{% endif %}
        BOOLOR_AGG(condition_code IN ('LD', 'LD_U14')) AS has_learning_disability,
        BOOLOR_AGG(condition_code = 'SMI') AS has_severe_mental_illness
    FROM {{ ref('fct_person_ltc_summary_by_month' if by_month else 'fct_person_ltc_summary') }}
    WHERE condition_code IN ('LD', 'LD_U14', 'SMI')
        {% if not by_month %}AND is_on_register{% endif %}
    GROUP BY ALL
),

-- One row per person and reporting date with every breakdown value.
person_breakdowns AS (
    SELECT
        pd.person_id,
        pd.reporting_date,
        CASE d.imd_quintile_numeric_25
            WHEN 1 THEN '1 Most deprived'
            WHEN 5 THEN '5 Least deprived'
            ELSE COALESCE(d.imd_quintile_numeric_25::VARCHAR, 'Unknown')
        END AS imd_quintile,
        COALESCE(NULLIF(d.ethnicity_category, ''), 'Unknown') AS ethnicity,
        CASE
            WHEN d.ethnicity_subcategory IS NULL
                OR d.ethnicity_subcategory NOT LIKE '%:%' THEN 'Not stated or unknown'
            ELSE d.ethnicity_subcategory
        END AS ethnicity_detail,
        CASE
            WHEN d.gender IN ('Female', 'Male') THEN d.gender
            ELSE 'Other or unknown'
        END AS sex,
        COALESCE(sg.segment_number || ' ' || sg.segment_name, 'Not segmented') AS segment,
        CASE
            WHEN sg.ltc_count IS NULL THEN 'Not segmented'
            WHEN sg.ltc_count >= 4 THEN '4 or more'
            ELSE sg.ltc_count::VARCHAR
        END AS ltc_count_band,
        CASE f.category
            WHEN 'ROBUST' THEN 'Fit'
            WHEN 'MILD FRAILTY' THEN 'Mild'
            WHEN 'MODERATE FRAILTY' THEN 'Moderate'
            WHEN 'SEVERE FRAILTY' THEN 'Severe'
            ELSE 'Not scored'
        END AS frailty,
        IFF(r.has_learning_disability, 'On register', 'Not on register') AS learning_disability,
        IFF(r.has_severe_mental_illness, 'On register', 'Not on register') AS severe_mental_illness,
        IFF(d.interpreter_needed, 'Yes', 'No') AS interpreter_needed
        {%- if not by_month %},
        CASE
            WHEN c.cambridge_comorbidity_score IS NULL THEN 'Not scored'
            WHEN c.cambridge_comorbidity_score < 0.5 THEN 'Under 0.5'
            WHEN c.cambridge_comorbidity_score < 1 THEN '0.5 to 0.99'
            WHEN c.cambridge_comorbidity_score < 2 THEN '1 to 1.99'
            ELSE '2 or more'
        END AS multimorbidity_score,
        CASE
            WHEN ch.is_care_home_resident THEN 'Care home'
            WHEN hb.is_housebound THEN 'Housebound'
            ELSE 'Neither recorded'
        END AS residence,
        COALESCE(d.age_band_esp, 'Unknown') AS age_band_esp
        {%- endif %}
    FROM person_dates AS pd
    LEFT JOIN {{ ref('dim_person_demographics') }} AS d
        ON pd.person_id = d.person_id
    LEFT JOIN segments AS sg
        ON pd.person_id = sg.person_id
        {% if by_month %}AND pd.reporting_date = sg.reporting_date{% endif %}
    LEFT JOIN frailty AS f
        ON pd.person_id = f.person_id
        {% if by_month %}AND pd.reporting_date = f.reporting_date{% endif %}
    LEFT JOIN registers AS r
        ON pd.person_id = r.person_id
        {% if by_month %}AND pd.reporting_date = r.reporting_date{% endif %}
    {%- if not by_month %}
    LEFT JOIN {{ ref('dim_person_ccms') }} AS c
        ON pd.person_id = c.person_id
    LEFT JOIN {{ ref('dim_person_care_home') }} AS ch
        ON pd.person_id = ch.person_id
    LEFT JOIN {{ ref('dim_person_housebound_status') }} AS hb
        ON pd.person_id = hb.person_id
    {%- endif %}
),

enriched AS (
    SELECT
        s.reporting_date,
        s.indicator_id,
        s.practice_code,
        s.is_in_numerator,
        s.indicator_status,
        CASE
            WHEN s.age IS NULL THEN 'Unknown'
            WHEN s.age < 18 THEN '0 to 17'
            WHEN s.age < 40 THEN '18 to 39'
            WHEN s.age < 65 THEN '40 to 64'
            WHEN s.age < 75 THEN '65 to 74'
            WHEN s.age < 85 THEN '75 to 84'
            ELSE '85 and over'
        END AS age_band,
        {%- for column, label in breakdowns if column != 'age_band' %}
        pb.{{ column }}{{ ',' if not loop.last }}
        {%- endfor %}
        {%- if not by_month %},
        -- Achievement rate of everyone in the same ESP age band on this indicator
        AVG(IFF(s.is_in_numerator, 1, 0)::FLOAT) OVER (
            PARTITION BY s.reporting_date, s.indicator_id, pb.age_band_esp
        ) AS age_band_rate
        {%- endif %}
    FROM status AS s
    INNER JOIN person_breakdowns AS pb
        ON s.person_id = pb.person_id
        AND s.reporting_date = pb.reporting_date
),

-- One pass: the total plus one grouping set per breakdown.
grouped AS (
    SELECT
        reporting_date,
        indicator_id,
        practice_code,
        CASE
            {%- for column, label in breakdowns %}
            WHEN GROUPING({{ column }}) = 0 THEN '{{ label }}'
            {%- endfor %}
            ELSE 'Total'
        END AS breakdown,
        COALESCE(
            {%- for column, label in breakdowns %}
            {{ column }},
            {%- endfor %}
            'All people'
        ) AS breakdown_value,
        COUNT(*) AS denominator,
        COUNT_IF(is_in_numerator) AS numerator,
        COUNT_IF(indicator_status = 'NOT_RECORDED_IN_PERIOD') AS not_recorded_in_period,
        COUNT_IF(indicator_status IN ('ABOVE_TARGET', 'OUT_OF_RANGE')) AS outside_target,
        COUNT_IF(indicator_status IN (
            'NOT_TREATED_IN_PERIOD', 'NEVER_TREATED',
            'VKA_WITHOUT_DOAC_EXCEPTION', 'DOAC_WHERE_VKA_INDICATED'
        )) AS not_treated,
        COUNT_IF(indicator_status = 'NOT_ASSESSABLE') AS not_assessable
        {%- if not by_month %},
        SUM(age_band_rate) AS numerator_at_age_rates,
        SUM(age_band_rate * (1 - age_band_rate)) AS variance_at_age_rates
        {%- endif %}
    FROM enriched
    GROUP BY GROUPING SETS (
        (reporting_date, indicator_id, practice_code)
        {%- for column, label in breakdowns %},
        (reporting_date, indicator_id, practice_code, {{ column }})
        {%- endfor %}
    )
)

SELECT
    g.reporting_date,
    g.indicator_id,
    g.practice_code,
    p.practice_name,
    p.pcn_code,
    p.pcn_name,
    pn.neighbourhood_code,
    pn.neighbourhood_registered AS neighbourhood_name,
    p.borough_registered AS borough,
    g.breakdown,
    g.breakdown_value,
    g.denominator,
    g.numerator,
    g.denominator - g.numerator AS not_achieved,
    g.not_recorded_in_period,
    g.outside_target,
    g.not_treated,
    g.not_assessable
    {%- if not by_month %},
    g.numerator_at_age_rates,
    g.variance_at_age_rates
    {%- endif %}
FROM grouped AS g
LEFT JOIN {{ ref('dim_practice') }} AS p
    ON g.practice_code = p.practice_code
LEFT JOIN {{ ref('dim_practice_neighbourhood') }} AS pn
    ON g.practice_code = pn.practice_code
{% endmacro %}
