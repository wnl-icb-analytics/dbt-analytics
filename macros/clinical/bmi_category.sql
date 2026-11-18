{% macro bmi_category(bmi_value='bmi_value', requires_lower_bmi_thresholds='requires_lower_bmi_thresholds', output='category') %}
{#- Shared adult BMI categories and risk order from NICE NG246. Null risk uses standard thresholds. -#}
{% if output not in ['category', 'risk_sort_key'] %}
    {{ exceptions.raise_compiler_error('Unsupported BMI category output: ' ~ output) }}
{% endif %}
{% set labels = ["'Invalid'", "'Underweight'", "'Normal'", "'Overweight'", "'Obese Class I'", "'Obese Class II'", "'Obese Class III'"] if output == 'category' else ['0', '2', '1', '3', '4', '5', '6'] %}
CASE
    WHEN {{ bmi_value }} NOT BETWEEN 10 AND 150 THEN {{ labels[0] }}
    WHEN {{ bmi_value }} < 18.5 THEN {{ labels[1] }}
    WHEN {{ requires_lower_bmi_thresholds }} = TRUE THEN
        CASE
            WHEN {{ bmi_value }} < 23 THEN {{ labels[2] }}
            WHEN {{ bmi_value }} < 27.5 THEN {{ labels[3] }}
            WHEN {{ bmi_value }} < 32.5 THEN {{ labels[4] }}
            WHEN {{ bmi_value }} < 37.5 THEN {{ labels[5] }}
            ELSE {{ labels[6] }}
        END
    ELSE
        CASE
            WHEN {{ bmi_value }} < 25 THEN {{ labels[2] }}
            WHEN {{ bmi_value }} < 30 THEN {{ labels[3] }}
            WHEN {{ bmi_value }} < 35 THEN {{ labels[4] }}
            WHEN {{ bmi_value }} < 40 THEN {{ labels[5] }}
            ELSE {{ labels[6] }}
        END
END
{% endmacro %}
