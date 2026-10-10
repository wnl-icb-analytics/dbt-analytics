{% macro nice_register(condition_code, reference='current') %}
{#-
    Adapt the live or monthly register to membership rows at a reference date.
    Args: condition_code is a code from ltc_register_history_models();
          reference is current or by_month.
    Returns: person_id, reporting_date, condition_code, is_on_register,
             earliest_diagnosis_date, latest_diagnosis_date, plus the condition's
             DM, SMI, CKD, HF, FRAIL or NDH fields. Source timestamp precision is kept.
-#}
{% set adapters = {
    'AF': ('fct_person_atrial_fibrillation_register', 'fct_person_atrial_fibrillation_register_by_month'),
    'AST': ('fct_person_asthma_register', 'fct_person_asthma_register_by_month'),
    'CAN': ('fct_person_cancer_register', 'fct_person_cancer_register_by_month'),
    'CHD': ('fct_person_chd_register', 'fct_person_chd_register_by_month'),
    'CKD': ('fct_person_ckd_register', 'fct_person_ckd_register_by_month'),
    'COPD': ('fct_person_copd_register', 'fct_person_copd_register_by_month'),
    'CYP_AST': ('fct_person_cyp_asthma_register', 'fct_person_cyp_asthma_register_by_month'),
    'DEM': ('fct_person_dementia_register', 'fct_person_dementia_register_by_month'),
    'DEP': ('fct_person_depression_register', 'fct_person_depression_register_by_month'),
    'DM': ('fct_person_diabetes_register', 'fct_person_diabetes_register_by_month'),
    'EP': ('fct_person_epilepsy_register', 'fct_person_epilepsy_register_by_month'),
    'FH': ('fct_person_familial_hypercholesterolaemia_register', 'fct_person_familial_hypercholesterolaemia_register_by_month'),
    'HF': ('fct_person_heart_failure_register', 'fct_person_heart_failure_register_by_month'),
    'HTN': ('fct_person_hypertension_register', 'fct_person_hypertension_register_by_month'),
    'LD': ('fct_person_learning_disability_register', 'fct_person_learning_disability_register_by_month'),
    'LD_U14': ('fct_person_learning_disability_register_under_14', 'fct_person_learning_disability_register_under_14_by_month'),
    'NAFLD': ('fct_person_nafld_register', 'fct_person_nafld_register_by_month'),
    'NDH': ('fct_person_ndh_register', 'fct_person_ndh_register_by_month'),
    'OB': ('fct_person_obesity_register', 'fct_person_obesity_register_by_month'),
    'OST': ('fct_person_osteoporosis_register', 'fct_person_osteoporosis_register_by_month'),
    'OA': ('fct_person_osteoarthritis_register', 'fct_person_osteoarthritis_register_by_month'),
    'PAD': ('fct_person_pad_register', 'fct_person_pad_register_by_month'),
    'PC': ('fct_person_palliative_care_register', 'fct_person_palliative_care_register_by_month'),
    'RA': ('fct_person_rheumatoid_arthritis_register', 'fct_person_rheumatoid_arthritis_register_by_month'),
    'SMI': ('fct_person_smi_register', 'fct_person_smi_register_by_month'),
    'STIA': ('fct_person_stroke_tia_register', 'fct_person_stroke_tia_register_by_month'),
    'GESTDIAB': ('fct_person_gestational_diabetes_register', 'fct_person_gestational_diabetes_register_by_month'),
    'FRAIL': ('fct_person_frailty_register', 'fct_person_frailty_register_by_month'),
    'PD': ('fct_person_parkinsons_register', 'fct_person_parkinsons_register_by_month'),
    'CEREBRALP': ('fct_person_cerebral_palsy_register', 'fct_person_cerebral_palsy_register_by_month'),
    'MND': ('fct_person_mnd_register', 'fct_person_mnd_register_by_month'),
    'MS': ('fct_person_ms_register', 'fct_person_ms_register_by_month'),
    'ANX': ('fct_person_anxiety_register', 'fct_person_anxiety_register_by_month'),
    'THY': ('fct_person_hypothyroidism_register', 'fct_person_hypothyroidism_register_by_month'),
    'AUTISM': ('fct_person_autism_register', 'fct_person_autism_register_by_month'),
    'ADHD': ('fct_person_adhd_register', 'fct_person_adhd_register_by_month'),
    'CLD': ('fct_person_chronic_liver_disease_register', 'fct_person_chronic_liver_disease_register_by_month'),
    'SCD': ('fct_person_sickle_cell_register', 'fct_person_sickle_cell_register_by_month'),
    'THAL': ('fct_person_thalassaemia_register', 'fct_person_thalassaemia_register_by_month')
} %}
{% if condition_code not in adapters %}
    {{ exceptions.raise_compiler_error('Unsupported NICE register: ' ~ condition_code) }}
{% endif %}
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported NICE reference: ' ~ reference) }}
{% endif %}
{% if condition_code == 'HF' %}
WITH hf_members AS (
    SELECT
        person_id,
        {% if reference == 'current' %}CURRENT_DATE()::DATE{% else %}month_end_date{% endif %} AS reporting_date,
        earliest_diagnosis_date,
        latest_diagnosis_date,
        is_on_hfref_register
    FROM {{ ref(adapters['HF'][0 if reference == 'current' else 1]) }}
    {% if reference == 'current' %}WHERE is_on_register{% endif %}
),
known_hf AS (
    SELECT
        members.person_id,
        members.reporting_date,
        evidence.clinical_effective_date,
        evidence.is_diagnosis_code,
        MAX(IFF(evidence.is_resolved_code, evidence.clinical_effective_date, NULL)) OVER (
            PARTITION BY members.person_id, members.reporting_date
        ) AS latest_known_resolution_date
    FROM hf_members AS members
    INNER JOIN {{ ref('int_heart_failure_diagnoses_all') }} AS evidence
        ON members.person_id = evidence.person_id
        AND {{ ltc_register_known_by('evidence.clinical_effective_date', 'evidence.date_recorded', 'members.reporting_date') }}
),
hf_entry AS (
    SELECT
        person_id,
        reporting_date,
        MIN(clinical_effective_date) AS earliest_unresolved_diagnosis_date
    FROM known_hf
    WHERE is_diagnosis_code
        -- HF diagnoses and resolutions on the same calendar date retain membership.
        AND (latest_known_resolution_date IS NULL
            OR clinical_effective_date::DATE >= latest_known_resolution_date::DATE)
    GROUP BY person_id, reporting_date
)
SELECT
    members.person_id,
    members.reporting_date,
    'HF' AS condition_code,
    TRUE AS is_on_register,
    members.earliest_diagnosis_date,
    members.latest_diagnosis_date,
    entry.earliest_unresolved_diagnosis_date,
    members.is_on_hfref_register
FROM hf_members AS members
LEFT JOIN hf_entry AS entry
    ON members.person_id = entry.person_id
    AND members.reporting_date = entry.reporting_date
{% else %}
SELECT
    person_id,
    {% if reference == 'current' %}CURRENT_DATE()::DATE{% else %}month_end_date{% endif %} AS reporting_date,
    '{{ condition_code }}' AS condition_code,
    TRUE AS is_on_register,
    {% if condition_code == 'OB' and reference == 'current' %}
    -- The LTC summary's obesity dates describe BMI evidence, not coded diagnoses.
    latest_valid_bmi_date AS earliest_diagnosis_date,
    latest_bmi_date AS latest_diagnosis_date
    {% else %}
    earliest_diagnosis_date,
    latest_diagnosis_date{% if condition_code in ['DM', 'SMI', 'CKD', 'FRAIL', 'NDH'] %},{% endif %}
    {% endif %}
    {% if condition_code == 'DM' %}
    diabetes_type,
    earliest_type1_date,
    latest_type1_date,
    earliest_type2_date,
    latest_type2_date,
    latest_resolved_date
    {% elif condition_code == 'SMI' %}
    {% if reference == 'current' %}
    latest_resolved_date AS latest_remission_date,
    has_active_smi_diagnosis
    {% else %}
    latest_remission_date,
    -- Remission at the same timestamp as diagnosis wins; membership includes remission.
    latest_diagnosis_date IS NOT NULL
        AND (latest_remission_date IS NULL OR latest_remission_date < latest_diagnosis_date)
        AS has_active_smi_diagnosis
    {% endif %}
    {% elif condition_code == 'CKD' %}
    latest_resolved_date,
    latest_stage_1_2_date
    {% elif condition_code == 'FRAIL' %}
    latest_frailty_severity
    {% elif condition_code == 'NDH' %}
    {% if reference == 'current' %}
    COALESCE(has_diabetes_diagnosis AND NOT is_diabetes_resolved, FALSE) AS has_unresolved_diabetes
    {% else %}
    -- Monthly membership already excludes unresolved diabetes of any age.
    FALSE AS has_unresolved_diabetes
    {% endif %}
    {% endif %}
FROM {{ ref(adapters[condition_code][0 if reference == 'current' else 1]) }}
{% if reference == 'current' %}
WHERE is_on_register
{% endif %}
{% endif %}
{% endmacro %}
