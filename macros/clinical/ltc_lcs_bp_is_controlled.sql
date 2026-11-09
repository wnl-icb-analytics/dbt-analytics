{% macro ltc_lcs_bp_is_controlled(clinic_systolic, clinic_diastolic, home_systolic, home_diastolic, window_start="dateadd(month, -12, current_date())") %}
{# True when the latest reading's setting is controlled: both components >= 1 and at or below the
   setting's thresholds, read within the window. Columns from int_ltc_lcs_bp_latest_by_setting. #}
(
    (
        is_latest_reading_clinic
        and latest_reading_date > {{ window_start }}
        and clinic_reading_date > {{ window_start }}
        and clinic_systolic_value between 1 and {{ clinic_systolic }}
        and clinic_diastolic_value between 1 and {{ clinic_diastolic }}
    )
    or (
        is_latest_reading_home
        and home_reading_date > {{ window_start }}
        and home_systolic_value between 1 and {{ home_systolic }}
        and home_diastolic_value between 1 and {{ home_diastolic }}
    )
)
{% endmacro %}
