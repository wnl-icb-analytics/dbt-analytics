-- Synthetic NDH re-entries, separate practice records and unknown clinical days.
SELECT
    column1::VARCHAR AS id,
    column2::NUMBER AS person_id,
    column3::TIMESTAMP_NTZ AS clinical_effective_date,
    column3::TIMESTAMP_NTZ AS date_recorded,
    column4::VARCHAR AS concept_code,
    'Synthetic NDH'::VARCHAR AS concept_display,
    'NDH_COD'::VARCHAR AS source_cluster_id,
    column5::FLOAT AS result_value,
    IFF(column1='SYN_OTHER_RECORD','SYN_OTHER_RECORD','SYN_RECORD')::VARCHAR AS source_patient_id,
    TRUE::BOOLEAN AS is_ndh_diagnosis_code,
    FALSE::BOOLEAN AS is_igt_diagnosis_code,
    FALSE::BOOLEAN AS is_pre_diabetes_diagnosis_code,
    TRUE::BOOLEAN AS is_any_ndh_type_code,
    TRUE::BOOLEAN AS is_diagnosis_code
FROM VALUES
    ('SYN_A', -145441, '2022-01-01 09:00:00', 'SYN_CONCEPT', NULL),
    ('SYN_B', -145441, '2022-01-01 18:00:00', 'SYN_CONCEPT', NULL),
    ('SYN_C', -145441, '2022-01-01', 'SYN_OTHER_CONCEPT', NULL),
    ('SYN_D', -145441, '2022-01-02', 'SYN_CONCEPT', NULL),
    ('SYN_E', -145441, '2022-01-01', 'SYN_CONCEPT', 0),
    ('SYN_F', -145441, '2022-01-01', 'SYN_CONCEPT', 1),
    ('SYN_G', -145442, '2022-01-01', 'SYN_CONCEPT', NULL),
    ('SYN_OTHER_RECORD', -145441, '2022-01-01', 'SYN_CONCEPT', NULL),
    ('SYN_UNKNOWN_A', -145441, '1900-01-01', 'SYN_CONCEPT', NULL),
    ('SYN_UNKNOWN_B', -145441, '1900-01-01', 'SYN_CONCEPT', NULL),
    ('SYN_1899_A', -145441, '1899-12-31', 'SYN_CONCEPT', NULL),
    ('SYN_1899_B', -145441, '1899-12-31', 'SYN_CONCEPT', NULL)
