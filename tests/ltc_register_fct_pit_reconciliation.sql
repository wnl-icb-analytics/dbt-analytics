{% set register_pairs = [
    ('asthma', ref('fct_person_asthma_register'), calculate_asthma_register('CURRENT_DATE()')),
    ('atrial_fibrillation', ref('fct_person_atrial_fibrillation_register'), calculate_atrial_fibrillation_register('CURRENT_DATE()')),
    ('cancer', ref('fct_person_cancer_register'), calculate_cancer_register('CURRENT_DATE()')),
    ('chd', ref('fct_person_chd_register'), calculate_chd_register('CURRENT_DATE()')),
    ('ckd', ref('fct_person_ckd_register'), calculate_ckd_register('CURRENT_DATE()')),
    ('copd', ref('fct_person_copd_register'), calculate_copd_register('CURRENT_DATE()')),
    ('cvd', ref('fct_person_cvd_register'), calculate_cvd_register('CURRENT_DATE()')),
    ('dementia', ref('fct_person_dementia_register'), calculate_dementia_register('CURRENT_DATE()')),
    ('depression', ref('fct_person_depression_register'), calculate_depression_register('CURRENT_DATE()')),
    ('diabetes', ref('fct_person_diabetes_register'), calculate_diabetes_register('CURRENT_DATE()')),
    ('epilepsy', ref('fct_person_epilepsy_register'), calculate_epilepsy_register('CURRENT_DATE()')),
    ('heart_failure', ref('fct_person_heart_failure_register'), calculate_heart_failure_register('CURRENT_DATE()')),
    ('hypertension', ref('fct_person_hypertension_register'), calculate_hypertension_register('CURRENT_DATE()')),
    ('learning_disability', ref('fct_person_learning_disability_register'), calculate_learning_disability_register('CURRENT_DATE()')),
    ('ndh', ref('fct_person_qof_ndh_gdm_register'), calculate_qof_ndh_gdm_register('CURRENT_DATE()')),
    ('obesity', ref('fct_person_obesity_register'), calculate_obesity_register('CURRENT_DATE()')),
    ('obesity2', ref('fct_person_obesity2_register'), calculate_obesity2_register('CURRENT_DATE()')),
    ('osteoporosis', ref('fct_person_osteoporosis_register'), calculate_osteoporosis_register('CURRENT_DATE()')),
    ('pad', ref('fct_person_pad_register'), calculate_pad_register('CURRENT_DATE()')),
    ('palliative_care', ref('fct_person_palliative_care_register'), calculate_palliative_care_register('CURRENT_DATE()')),
    ('rheumatoid_arthritis', ref('fct_person_rheumatoid_arthritis_register'), calculate_rheumatoid_arthritis_register('CURRENT_DATE()')),
    ('smi', ref('fct_person_smi_register'), calculate_smi_register('CURRENT_DATE()')),
    ('stroke_tia', ref('fct_person_stroke_tia_register'), calculate_stroke_tia_register('CURRENT_DATE()')),
    ('adhd', ref('fct_person_adhd_register'), calculate_adhd_register('CURRENT_DATE()')),
    ('anxiety', ref('fct_person_anxiety_register'), calculate_anxiety_register('CURRENT_DATE()')),
    ('autism', ref('fct_person_autism_register'), calculate_autism_register('CURRENT_DATE()')),
    ('cerebral_palsy', ref('fct_person_cerebral_palsy_register'), calculate_cerebral_palsy_register('CURRENT_DATE()')),
    ('chronic_liver_disease', ref('fct_person_chronic_liver_disease_register'), calculate_chronic_liver_disease_register('CURRENT_DATE()')),
    ('cyp_asthma', ref('fct_person_cyp_asthma_register'), calculate_cyp_asthma_register('CURRENT_DATE()')),
    ('familial_hypercholesterolaemia', ref('fct_person_familial_hypercholesterolaemia_register'), calculate_familial_hypercholesterolaemia_register('CURRENT_DATE()')),
    ('frailty', ref('fct_person_frailty_register'), calculate_frailty_register('CURRENT_DATE()')),
    ('gestational_diabetes', ref('fct_person_gestational_diabetes_register'), calculate_gestational_diabetes_register('CURRENT_DATE()')),
    ('hypothyroidism', ref('fct_person_hypothyroidism_register'), calculate_hypothyroidism_register('CURRENT_DATE()')),
    ('learning_disability_under_14', ref('fct_person_learning_disability_register_under_14'), calculate_learning_disability_under_14_register('CURRENT_DATE()')),
    ('mnd', ref('fct_person_mnd_register'), calculate_mnd_register('CURRENT_DATE()')),
    ('ms', ref('fct_person_ms_register'), calculate_ms_register('CURRENT_DATE()')),
    ('nafld', ref('fct_person_nafld_register'), calculate_nafld_register('CURRENT_DATE()')),
    ('ndh_clinical', ref('fct_person_ndh_register'), calculate_ndh_register('CURRENT_DATE()')),
    ('osteoarthritis', ref('fct_person_osteoarthritis_register'), calculate_osteoarthritis_register('CURRENT_DATE()')),
    ('parkinsons', ref('fct_person_parkinsons_register'), calculate_parkinsons_register('CURRENT_DATE()')),
    ('sickle_cell', ref('fct_person_sickle_cell_register'), calculate_sickle_cell_register('CURRENT_DATE()')),
    ('thalassaemia', ref('fct_person_thalassaemia_register'), calculate_thalassaemia_register('CURRENT_DATE()'))
] %}

{% set register_sources = {
    'asthma': [(ref('int_asthma_diagnoses_all'), 'clinical_effective_date'), (ref('int_asthma_medications_all'), 'order_date')],
    'atrial_fibrillation': [(ref('int_atrial_fibrillation_diagnoses_all'), 'clinical_effective_date')],
    'cancer': [(ref('int_cancer_diagnoses_all'), 'clinical_effective_date')],
    'chd': [(ref('int_chd_diagnoses_all'), 'clinical_effective_date')],
    'ckd': [(ref('int_ckd_diagnoses_all'), 'clinical_effective_date')],
    'copd': [(ref('int_copd_diagnoses_all'), 'clinical_effective_date'), (ref('int_spirometry_all'), 'clinical_effective_date')],
    'cvd': [(ref('int_chd_diagnoses_all'), 'clinical_effective_date'), (ref('int_stroke_tia_diagnoses_all'), 'clinical_effective_date')],
    'dementia': [(ref('int_dementia_diagnoses_all'), 'clinical_effective_date')],
    'depression': [(ref('int_depression_diagnoses_all'), 'clinical_effective_date')],
    'diabetes': [(ref('int_diabetes_diagnoses_all'), 'clinical_effective_date')],
    'epilepsy': [(ref('int_epilepsy_diagnoses_all'), 'clinical_effective_date'), (ref('int_epilepsy_medications_all'), 'order_date')],
    'heart_failure': [(ref('int_heart_failure_diagnoses_all'), 'clinical_effective_date')],
    'hypertension': [(ref('int_hypertension_diagnoses_all'), 'clinical_effective_date')],
    'learning_disability': [(ref('int_learning_disability_diagnoses_all'), 'clinical_effective_date')],
    'ndh': [(ref('int_diabetes_diagnoses_all'), 'clinical_effective_date'), (ref('int_gestational_diabetes_diagnoses_all'), 'clinical_effective_date'), (ref('int_ndh_diagnoses_all'), 'clinical_effective_date')],
    'obesity': [(ref('int_bmi_qof_all'), 'clinical_effective_date'), (ref('int_ethnicity_qof_all'), 'clinical_effective_date')],
    'obesity2': [(ref('int_diabetes_diagnoses_all'), 'clinical_effective_date'), (ref('int_hypertension_diagnoses_all'), 'clinical_effective_date'), (ref('int_obesity2_bmi_all'), 'clinical_effective_date'), (ref('int_obesity2_diagnoses_all'), 'clinical_effective_date'), (ref('int_obesity2_ethnicity_all'), 'clinical_effective_date'), (ref('int_obesity2_lipid_lowering_medications_all'), 'order_date'), (ref('int_obesity2_lipids_all'), 'clinical_effective_date')],
    'osteoporosis': [(ref('int_dxa_scans_all'), 'clinical_effective_date'), (ref('int_fragility_fractures_all'), 'clinical_effective_date'), (ref('int_osteoporosis_diagnoses_all'), 'clinical_effective_date')],
    'pad': [(ref('int_pad_diagnoses_all'), 'clinical_effective_date')],
    'palliative_care': [(ref('int_palliative_care_diagnoses_all'), 'clinical_effective_date')],
    'rheumatoid_arthritis': [(ref('int_rheumatoid_arthritis_diagnoses_all'), 'clinical_effective_date')],
    'smi': [(ref('int_smi_diagnoses_all'), 'clinical_effective_date')],
    'stroke_tia': [(ref('int_stroke_tia_diagnoses_all'), 'clinical_effective_date')],
    'adhd': [(ref('int_adhd_diagnoses_all'), 'clinical_effective_date')],
    'anxiety': [(ref('int_anxiety_diagnoses_all'), 'clinical_effective_date')],
    'autism': [(ref('int_autism_diagnoses_all'), 'clinical_effective_date')],
    'cerebral_palsy': [(ref('int_cerebral_palsy_diagnoses_all'), 'clinical_effective_date')],
    'chronic_liver_disease': [(ref('int_chronic_liver_disease_diagnoses_all'), 'clinical_effective_date')],
    'cyp_asthma': [(ref('int_asthma_diagnoses_all'), 'clinical_effective_date'), (ref('int_asthma_medications_all'), 'order_date')],
    'familial_hypercholesterolaemia': [(ref('int_familial_hypercholesterolaemia_diagnoses_all'), 'clinical_effective_date')],
    'frailty': [(ref('int_frailty_diagnoses_all'), 'clinical_effective_date')],
    'gestational_diabetes': [(ref('int_gestational_diabetes_diagnoses_all'), 'clinical_effective_date')],
    'hypothyroidism': [(ref('int_hypothyroidism_diagnoses_all'), 'clinical_effective_date')],
    'learning_disability_under_14': [(ref('int_learning_disability_diagnoses_all'), 'clinical_effective_date')],
    'mnd': [(ref('int_mnd_diagnoses_all'), 'clinical_effective_date')],
    'ms': [(ref('int_ms_diagnoses_all'), 'clinical_effective_date')],
    'nafld': [(ref('int_nafld_diagnoses_all'), 'clinical_effective_date')],
    'ndh_clinical': [(ref('int_diabetes_diagnoses_all'), 'clinical_effective_date'), (ref('int_ndh_diagnoses_all'), 'clinical_effective_date')],
    'osteoarthritis': [(ref('int_osteoarthritis_diagnoses_all'), 'clinical_effective_date')],
    'parkinsons': [(ref('int_parkinsons_diagnoses_all'), 'clinical_effective_date')],
    'sickle_cell': [(ref('int_sickle_cell_diagnoses_all'), 'clinical_effective_date')],
    'thalassaemia': [(ref('int_thalassaemia_diagnoses_all'), 'clinical_effective_date')]
} %}

{# Live facts filtered to currently registered patients; their PIT members are compared on the same population. #}
{% set current_patient_registers = ['familial_hypercholesterolaemia', 'gestational_diabetes', 'nafld'] %}

{# Live facts that inner join dim_person or dim_person_age, so they cover people in dim_person only. #}
{% set dim_person_registers = [
    'adhd',
    'anxiety',
    'asthma',
    'autism',
    'cerebral_palsy',
    'chronic_liver_disease',
    'ckd',
    'cyp_asthma',
    'depression',
    'diabetes',
    'epilepsy',
    'familial_hypercholesterolaemia',
    'frailty',
    'hypothyroidism',
    'learning_disability_under_14',
    'mnd',
    'ms',
    'ndh_clinical',
    'obesity',
    'osteoarthritis',
    'osteoporosis',
    'parkinsons',
    'rheumatoid_arthritis',
    'sickle_cell',
    'thalassaemia'
] %}

{#
The live facts include records dated or entered after today; PIT today does not.
A mismatch is expected only for a person with such a record in the register's own
sources (register_sources), so those people are left out of both directions. Every
other mismatch is drift.
#}

WITH mismatches AS (
{% for register_name, fact_model, pit_query in register_pairs %}
    {% if not loop.first %} UNION ALL {% endif %}

    SELECT
        '{{ register_name }}' AS register_name,
        mismatch.direction,
        mismatch.person_id,
        release.snapshot_date AS pcd_snapshot_date,
        release.release_version AS pcd_release_version,
        release.source_file AS pcd_source_file
    FROM (
        WITH pit_today AS (
            {{ pit_query }}
        ),

        not_yet_known AS (
            {% for source_model, date_column in register_sources[register_name] %}
            SELECT person_id
            FROM {{ source_model }}
            WHERE person_id IS NOT NULL
                AND (
                    CAST({{ date_column }} AS DATE) > CURRENT_DATE()
                    OR CAST(date_recorded AS DATE) > CURRENT_DATE()
                )
            {% if not loop.last %}UNION{% endif %}
            {% endfor %}
        ),

        live_only AS (
            SELECT live.person_id
            FROM {{ fact_model }} AS live
            LEFT JOIN pit_today AS pit
                ON live.person_id = pit.person_id
                AND pit.is_on_register = TRUE
            WHERE
                pit.person_id IS NULL
                AND live.person_id NOT IN (SELECT person_id FROM not_yet_known)
        ),

        pit_only AS (
            SELECT pit.person_id
            FROM pit_today AS pit
            LEFT JOIN {{ fact_model }} AS live
                ON pit.person_id = live.person_id
            WHERE
                pit.is_on_register = TRUE
                AND live.person_id IS NULL
                AND pit.person_id NOT IN (SELECT person_id FROM not_yet_known)
                {% if register_name in dim_person_registers %}
                AND pit.person_id IN (SELECT person_id FROM {{ ref('dim_person') }})
                {% endif %}
                {% if register_name in current_patient_registers %}
                AND pit.person_id IN (SELECT person_id FROM {{ ref('dim_person_active_patients') }})
                {% endif %}
        )

        SELECT 'live_not_pit' AS direction, person_id FROM live_only
        UNION ALL
        SELECT 'pit_not_live' AS direction, person_id FROM pit_only
    ) AS mismatch
    LEFT JOIN (
        SELECT DISTINCT snapshot_date, release_version, source_file
        FROM {{ ref('stg_reference_pcd_refset_latest') }}
    ) AS release ON TRUE
{% endfor %}

    UNION ALL

    SELECT
        'heart_failure_hfref' AS register_name,
        mismatch.direction,
        mismatch.person_id,
        release.snapshot_date AS pcd_snapshot_date,
        release.release_version AS pcd_release_version,
        release.source_file AS pcd_source_file
    FROM (
        WITH pit_today AS (
            {{ calculate_heart_failure_register('CURRENT_DATE()') }}
        ),

        live_hfref AS (
            SELECT
                person_id,
                latest_diagnosis_date,
                latest_reduced_ef_diagnosis_date
            FROM {{ ref('fct_person_heart_failure_register') }}
            WHERE is_on_hfref_register = TRUE
        ),

        pit_hfref AS (
            SELECT person_id
            FROM pit_today
            WHERE is_on_hfref_register = TRUE
        ),

        not_yet_known AS (
            SELECT person_id
            FROM {{ ref('int_heart_failure_diagnoses_all') }}
            WHERE person_id IS NOT NULL
                AND (
                    CAST(clinical_effective_date AS DATE) > CURRENT_DATE()
                    OR CAST(date_recorded AS DATE) > CURRENT_DATE()
                )
        )

        SELECT 'live_not_pit' AS direction, live.person_id
        FROM live_hfref AS live
        LEFT JOIN pit_hfref AS pit USING (person_id)
        WHERE pit.person_id IS NULL
            AND live.person_id NOT IN (SELECT person_id FROM not_yet_known)

        UNION ALL

        SELECT 'pit_not_live' AS direction, pit.person_id
        FROM pit_hfref AS pit
        LEFT JOIN live_hfref AS live USING (person_id)
        WHERE live.person_id IS NULL
            AND pit.person_id NOT IN (SELECT person_id FROM not_yet_known)
    ) AS mismatch
    LEFT JOIN (
        SELECT DISTINCT snapshot_date, release_version, source_file
        FROM {{ ref('stg_reference_pcd_refset_latest') }}
    ) AS release ON TRUE
)

SELECT
    register_name,
    direction,
    pcd_snapshot_date,
    pcd_release_version,
    pcd_source_file,
    COUNT(*) AS mismatch_count
FROM mismatches
GROUP BY
    register_name,
    direction,
    pcd_snapshot_date,
    pcd_release_version,
    pcd_source_file
