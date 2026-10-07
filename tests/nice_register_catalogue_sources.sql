WITH expected AS (
    SELECT column1::VARCHAR AS indicator_id, column2::VARCHAR AS source_model
    FROM VALUES
        ('IND185', 'fct_person_nice_atrial_fibrillation_register'),
        ('IND205', 'fct_person_nice_multimorbidity_register'),
        ('IND206', 'fct_person_nice_frailty_register'),
        ('IND256', 'fct_person_nice_smi_register')
), failures AS (
    SELECT expected.indicator_id
    FROM expected
    LEFT JOIN {{ ref('def_indicator') }} AS actual
        ON expected.indicator_id = actual.indicator_id
    WHERE actual.indicator_id IS NULL
        OR actual.source_model <> UPPER(expected.source_model)
        OR actual.source_column <> 'IS_ON_REGISTER'
        OR actual.indicator_type <> 'NICE_REGISTER'
)
SELECT COUNT(*) AS failure_count FROM failures HAVING COUNT(*) > 0
