{% macro ltc_register_history_models() %}
{#-
    Condition codes of fct_person_ltc_summary and the monthly register model that
    holds each one. tests/ltc_register_history_covers_seed.sql checks the codes
    against ltc_register_denominator_rules.
-#}
{{ return([
    ('AF', 'fct_person_atrial_fibrillation_register_by_month'),
    ('AST', 'fct_person_asthma_register_by_month'),
    ('CAN', 'fct_person_cancer_register_by_month'),
    ('CHD', 'fct_person_chd_register_by_month'),
    ('CKD', 'fct_person_ckd_register_by_month'),
    ('COPD', 'fct_person_copd_register_by_month'),
    ('CYP_AST', 'fct_person_cyp_asthma_register_by_month'),
    ('DEM', 'fct_person_dementia_register_by_month'),
    ('DEP', 'fct_person_depression_register_by_month'),
    ('DM', 'fct_person_diabetes_register_by_month'),
    ('EP', 'fct_person_epilepsy_register_by_month'),
    ('FH', 'fct_person_familial_hypercholesterolaemia_register_by_month'),
    ('HF', 'fct_person_heart_failure_register_by_month'),
    ('HTN', 'fct_person_hypertension_register_by_month'),
    ('LD', 'fct_person_learning_disability_register_by_month'),
    ('LD_U14', 'fct_person_learning_disability_register_under_14_by_month'),
    ('NAFLD', 'fct_person_nafld_register_by_month'),
    ('NDH', 'fct_person_ndh_register_by_month'),
    ('OB', 'fct_person_obesity_register_by_month'),
    ('OST', 'fct_person_osteoporosis_register_by_month'),
    ('OA', 'fct_person_osteoarthritis_register_by_month'),
    ('PAD', 'fct_person_pad_register_by_month'),
    ('PC', 'fct_person_palliative_care_register_by_month'),
    ('RA', 'fct_person_rheumatoid_arthritis_register_by_month'),
    ('SMI', 'fct_person_smi_register_by_month'),
    ('STIA', 'fct_person_stroke_tia_register_by_month'),
    ('GESTDIAB', 'fct_person_gestational_diabetes_register_by_month'),
    ('FRAIL', 'fct_person_frailty_register_by_month'),
    ('PD', 'fct_person_parkinsons_register_by_month'),
    ('CEREBRALP', 'fct_person_cerebral_palsy_register_by_month'),
    ('MND', 'fct_person_mnd_register_by_month'),
    ('MS', 'fct_person_ms_register_by_month'),
    ('ANX', 'fct_person_anxiety_register_by_month'),
    ('THY', 'fct_person_hypothyroidism_register_by_month'),
    ('AUTISM', 'fct_person_autism_register_by_month'),
    ('ADHD', 'fct_person_adhd_register_by_month'),
    ('CLD', 'fct_person_chronic_liver_disease_register_by_month'),
    ('SCD', 'fct_person_sickle_cell_register_by_month'),
    ('THAL', 'fct_person_thalassaemia_register_by_month')
]) }}
{% endmacro %}
