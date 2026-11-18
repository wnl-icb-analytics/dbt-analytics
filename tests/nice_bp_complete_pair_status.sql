-- An invalid complete pair in the window cannot prove control.
{% set measures = [
    'fct_person_bp_control_chd_ind241',
    'fct_person_bp_control_chd_ind242',
    'fct_person_bp_control_hypertension_ind239',
    'fct_person_bp_control_hypertension_ind240',
    'fct_person_bp_control_pad_ind245',
    'fct_person_bp_control_pad_ind246',
    'fct_person_bp_control_stroke_tia_ind243',
    'fct_person_bp_control_stroke_tia_ind244',
    'fct_person_ckd_bp_ind235',
    'fct_person_ckd_bp_ind264',
    'fct_person_diabetes_bp_ind249'
] %}

{% for measure in measures %}
SELECT person_id, indicator_id
FROM {{ ref(measure) }}
WHERE latest_bp_date::DATE BETWEEN DATEADD(month, -12, CURRENT_DATE()) AND CURRENT_DATE()
    AND is_valid_bp = FALSE
    AND (is_in_numerator OR indicator_status <> 'NOT_ASSESSABLE')
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
