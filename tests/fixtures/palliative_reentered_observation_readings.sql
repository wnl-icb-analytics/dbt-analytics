-- Synthetic palliative evidence uses the source model's observation_id column.
SELECT
    column1::VARCHAR AS observation_id,
    column2::NUMBER AS person_id,
    column3::TIMESTAMP_NTZ AS clinical_effective_date,
    column3::TIMESTAMP_NTZ AS date_recorded,
    column4::VARCHAR AS concept_code,
    'Synthetic palliative evidence'::VARCHAR AS concept_display,
    column5::FLOAT AS result_value,
    IFF(column6, 'PALCARE_COD', 'PALCARENI_COD')::VARCHAR AS source_cluster_id,
    column6::BOOLEAN AS is_palliative_care_code,
    NOT column6::BOOLEAN AS is_palliative_care_not_indicated_code,
    column6::BOOLEAN AS is_diagnosis_code,
    NOT column6::BOOLEAN AS is_resolved_code
FROM VALUES
    ('SYN_A', -145441, '2022-01-01 09:00:00', 'SYN_CONCEPT', NULL, TRUE),
    ('SYN_B', -145441, '2022-01-01 18:00:00', 'SYN_CONCEPT', NULL, TRUE),
    ('SYN_C', -145441, '2022-01-01', 'SYN_OTHER_CONCEPT', NULL, TRUE),
    ('SYN_D', -145441, '2022-01-02', 'SYN_CONCEPT', NULL, TRUE),
    ('SYN_E', -145441, '2022-01-01', 'SYN_CONCEPT', 0, TRUE),
    ('SYN_F', -145441, '2022-01-01', 'SYN_CONCEPT', 1, TRUE),
    ('SYN_G', -145442, '2022-01-01', 'SYN_CONCEPT', NULL, TRUE),
    ('SYN_UNKNOWN_A', -145441, '1900-01-01', 'SYN_CONCEPT', NULL, FALSE),
    ('SYN_UNKNOWN_B', -145441, '1900-01-01', 'SYN_CONCEPT', NULL, FALSE),
    ('SYN_1899_A', -145441, '1899-12-31', 'SYN_CONCEPT', NULL, FALSE),
    ('SYN_1899_B', -145441, '1899-12-31', 'SYN_CONCEPT', NULL, FALSE)
