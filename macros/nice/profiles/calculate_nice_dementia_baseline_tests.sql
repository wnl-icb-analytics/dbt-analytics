{% macro calculate_nice_dementia_baseline_tests(reference='current') %}
WITH population AS (
    -- The live register uses clinical dates only. Anchor tests to the first diagnosis known at R.
    SELECT population.person_id, population.reporting_date,
        MIN(diagnosis.clinical_effective_date)::DATE AS diagnosis_date
    FROM ({{ nice_register('DEM', reference) }}) AS register
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON register.person_id = population.person_id
        AND register.reporting_date = population.reporting_date
    INNER JOIN {{ ref('int_dementia_diagnoses_all') }} AS diagnosis
        ON population.person_id = diagnosis.person_id
        AND {{ ltc_register_known_by('diagnosis.clinical_effective_date', 'diagnosis.date_recorded', 'population.reporting_date') }}
        AND diagnosis.is_diagnosis_code
    GROUP BY population.person_id, population.reporting_date
), anchors AS (
    SELECT population.*, test.column1::VARCHAR AS test_type, rule_window.column1::VARCHAR AS window_name,
        IFF(rule_window.column1 = 'ind80', DATEADD(month, -6, diagnosis_date), DATEADD(month, -12, diagnosis_date)) AS window_start,
        IFF(rule_window.column1 = 'ind80', LEAST(DATEADD(month, 6, diagnosis_date), reporting_date), diagnosis_date) AS window_end
    FROM population
    CROSS JOIN (VALUES ('fbc'), ('calcium'), ('glucose'), ('renal'), ('liver'), ('thyroid'), ('b12'), ('folate')) AS test
    CROSS JOIN (VALUES ('ind80'), ('ind118')) AS rule_window
), daily AS (
    SELECT person_id, event_date, test_type, id
    FROM {{ ref('int_nice_dementia_baseline_tests_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date, test_type ORDER BY id DESC) = 1
), selected AS (
    SELECT anchors.person_id, anchors.reporting_date, anchors.diagnosis_date, anchors.window_name, anchors.test_type,
        IFF(daily.event_date >= anchors.window_start, daily.event_date, NULL) AS qualifying_date
    FROM anchors
    ASOF JOIN daily
        MATCH_CONDITION (anchors.window_end >= daily.event_date)
        ON anchors.person_id = daily.person_id AND anchors.test_type = daily.test_type
)
SELECT person_id, reporting_date, diagnosis_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'fbc', qualifying_date, NULL)) AS ind80_fbc_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'calcium', qualifying_date, NULL)) AS ind80_calcium_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'glucose', qualifying_date, NULL)) AS ind80_glucose_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'renal', qualifying_date, NULL)) AS ind80_renal_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'liver', qualifying_date, NULL)) AS ind80_liver_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'thyroid', qualifying_date, NULL)) AS ind80_thyroid_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'b12', qualifying_date, NULL)) AS ind80_b12_date,
    MAX(IFF(window_name = 'ind80' AND test_type = 'folate', qualifying_date, NULL)) AS ind80_folate_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'fbc', qualifying_date, NULL)) AS ind118_fbc_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'calcium', qualifying_date, NULL)) AS ind118_calcium_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'glucose', qualifying_date, NULL)) AS ind118_glucose_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'renal', qualifying_date, NULL)) AS ind118_renal_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'liver', qualifying_date, NULL)) AS ind118_liver_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'thyroid', qualifying_date, NULL)) AS ind118_thyroid_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'b12', qualifying_date, NULL)) AS ind118_b12_date,
    MAX(IFF(window_name = 'ind118' AND test_type = 'folate', qualifying_date, NULL)) AS ind118_folate_date,
    COALESCE(COUNT_IF(window_name = 'ind80' AND qualifying_date IS NOT NULL), 0) = 8 AS has_ind80_all_tests,
    COALESCE(COUNT_IF(window_name = 'ind118' AND qualifying_date IS NOT NULL), 0) = 8 AS has_ind118_all_tests,
    MAX(IFF(window_name = 'ind80', qualifying_date, NULL)) AS ind80_latest_record_date,
    MAX(IFF(window_name = 'ind118', qualifying_date, NULL)) AS ind118_latest_record_date
FROM selected
GROUP BY person_id, reporting_date, diagnosis_date
{% endmacro %}
