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
    ('stroke_tia', ref('fct_person_stroke_tia_register'), calculate_stroke_tia_register('CURRENT_DATE()'))
] %}

{% set future_evidence_columns = {
    'asthma': ['LATEST_DIAGNOSIS_DATE', 'LATEST_ASTHMA_MEDICATION_DATE'],
    'atrial_fibrillation': ['LATEST_DIAGNOSIS_DATE'],
    'cancer': ['LATEST_DIAGNOSIS_DATE'],
    'chd': ['LATEST_DIAGNOSIS_DATE'],
    'ckd': ['LATEST_DIAGNOSIS_DATE'],
    'copd': ['LATEST_DIAGNOSIS_DATE', 'QOF_RELEVANT_SPIROMETRY_DATE'],
    'cvd': ['EARLIEST_QUALIFYING_DIAGNOSIS_DATE'],
    'dementia': ['LATEST_DIAGNOSIS_DATE'],
    'depression': ['LATEST_DIAGNOSIS_DATE'],
    'diabetes': ['LATEST_DIAGNOSIS_DATE'],
    'epilepsy': ['LATEST_DIAGNOSIS_DATE', 'LATEST_EPILEPSY_MEDICATION_DATE'],
    'heart_failure': ['LATEST_DIAGNOSIS_DATE'],
    'hypertension': ['LATEST_DIAGNOSIS_DATE'],
    'learning_disability': ['LATEST_DIAGNOSIS_DATE'],
    'ndh': ['LATEST_DIAGNOSIS_DATE', 'LATEST_DIABETES_RESOLVED_DATE'],
    'obesity': ['LATEST_VALID_BMI_DATE', 'LATEST_BAME_DATE'],
    'obesity2': ['LATEST_BMI_35_DATE', 'LATEST_BMI_32_5_DATE', 'LATEST_LOWER_THRESHOLD_ETHNICITY_DATE', 'EARLIEST_ASCVD_DATE', 'LATEST_HYPERTENSION_DATE', 'LATEST_HYPERTENSION_RESOLVED_DATE', 'LATEST_LIPID_THERAPY_DATE', 'LATEST_LDL_DATE', 'LATEST_TRIGLYCERIDES_DATE', 'LATEST_HDL_DATE', 'EARLIEST_OBSTRUCTIVE_SLEEP_APNOEA_DATE', 'LATEST_TYPE2_DIABETES_DATE', 'LATEST_DIABETES_RESOLVED_DATE'],
    'osteoporosis': ['LATEST_DIAGNOSIS_DATE', 'LATEST_DXA_DATE', 'LATEST_DXA_T_SCORE_DATE', 'LATEST_FRAGILITY_FRACTURE_DATE'],
    'pad': ['LATEST_DIAGNOSIS_DATE'],
    'palliative_care': ['LATEST_DIAGNOSIS_DATE'],
    'rheumatoid_arthritis': ['LATEST_DIAGNOSIS_DATE'],
    'smi': ['LATEST_DIAGNOSIS_DATE'],
    'stroke_tia': ['LATEST_DIAGNOSIS_DATE']
} %}

{#
The live fact deliberately includes future-dated records; PIT today does not.
For live-only rows, only a future-valued membership evidence column explains
that intended difference. PIT-only rows are always drift.
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

        live_rows AS (
            SELECT
                live.person_id,
                OBJECT_CONSTRUCT_KEEP_NULL(*) AS row_values
            FROM {{ fact_model }} AS live
        ),

        live_scope AS (
            SELECT
                live.person_id,
                MAX(IFF(
                    field.key::VARCHAR IN (
                        {% for column_name in future_evidence_columns[register_name] %}
                        '{{ column_name }}'{% if not loop.last %},{% endif %}
                        {% endfor %}
                    )
                    AND TRY_TO_DATE(field.value::VARCHAR) > CURRENT_DATE(),
                    1,
                    0
                )) = 1 AS has_future_date
            FROM live_rows AS live,
                LATERAL FLATTEN(INPUT => live.row_values, OUTER => TRUE) AS field
            GROUP BY live.person_id
        ),

        live_only AS (
            SELECT live.person_id
            FROM live_scope AS live
            LEFT JOIN pit_today AS pit
                ON live.person_id = pit.person_id
                AND pit.is_on_register = TRUE
            WHERE
                pit.person_id IS NULL
                AND live.has_future_date = FALSE
        ),

        pit_only AS (
            SELECT pit.person_id
            FROM pit_today AS pit
            LEFT JOIN live_scope AS live
                ON pit.person_id = live.person_id
            WHERE
                pit.is_on_register = TRUE
                AND live.person_id IS NULL
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
        )

        SELECT 'live_not_pit' AS direction, live.person_id
        FROM live_hfref AS live
        LEFT JOIN pit_hfref AS pit USING (person_id)
        WHERE pit.person_id IS NULL
            AND live.latest_diagnosis_date <= CURRENT_DATE()
            AND live.latest_reduced_ef_diagnosis_date <= CURRENT_DATE()

        UNION ALL

        SELECT 'pit_not_live' AS direction, pit.person_id
        FROM pit_hfref AS pit
        LEFT JOIN live_hfref AS live USING (person_id)
        WHERE live.person_id IS NULL
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
