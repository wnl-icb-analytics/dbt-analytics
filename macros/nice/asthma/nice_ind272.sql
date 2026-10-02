{% macro nice_ind272(reference='current') %}
WITH population AS (
    {{ nice_asthma_diagnosis_population(reference) }}
), known_events AS (
    SELECT p.person_id, p.reporting_date, d.clinical_effective_date::DATE AS event_date,
        d.is_diagnosis_code, d.is_resolved_code
    FROM population p
    INNER JOIN {{ ref('int_asthma_diagnoses_all') }} d
        ON p.person_id = d.person_id
        AND {{ ltc_known_date('d.clinical_effective_date', 'd.date_recorded') }} <= p.reporting_date
), resolutions AS (
    SELECT person_id, reporting_date, MAX(IFF(is_resolved_code, event_date, NULL)) AS resolved_date
    FROM known_events
    GROUP BY person_id, reporting_date
), anchors AS (
    -- QOF's earliest unresolved date excludes diagnoses on or before the latest resolution.
    SELECT d.person_id, d.reporting_date, MIN(d.event_date) AS diagnosis_date
    FROM known_events d
    INNER JOIN resolutions r ON d.person_id = r.person_id AND d.reporting_date = r.reporting_date
    WHERE d.is_diagnosis_code AND (r.resolved_date IS NULL OR d.event_date > r.resolved_date)
    GROUP BY d.person_id, d.reporting_date
), cohort AS (
    SELECT p.*, a.diagnosis_date
    FROM population p
    INNER JOIN anchors a ON p.person_id = a.person_id AND p.reporting_date = a.reporting_date
    WHERE a.diagnosis_date > DATEADD(month, -15, p.reporting_date)
        AND a.diagnosis_date <= DATEADD(month, -3, p.reporting_date)
), evidence AS (
    SELECT p.person_id, p.reporting_date,
        MAX(IFF(e.test_type = 'FENO_COD', e.event_date, NULL)) AS feno_date,
        MAX(IFF(e.test_type = 'ASTSPIR_COD', e.event_date, NULL)) AS reversibility_date,
        MAX(IFF(e.test_type = 'PEFRVAR_COD', e.event_date, NULL)) AS pefr_variability_date,
        MAX(IFF(e.test_type = 'BRONCCHALENG_COD', e.event_date, NULL)) AS bronchial_challenge_date,
        MAX(IFF(e.test_type = 'SKINTEST_COD', e.event_date, NULL)) AS skin_prick_date,
        MAX(IFF(e.test_type = 'IGE_COD', e.event_date, NULL)) AS ige_date,
        MAX(IFF(e.test_type IN ('EOS_COUNT', 'FBC_COD'), e.event_date, NULL)) AS eosinophil_or_fbc_date
    FROM cohort p
    LEFT JOIN {{ ref('int_nice_asthma_objective_tests_all') }} e
        ON p.person_id = e.person_id
        AND e.event_date BETWEEN DATEADD(month, -3, p.diagnosis_date)
            AND LEAST(DATEADD(month, 3, p.diagnosis_date), p.reporting_date)
    GROUP BY p.person_id, p.reporting_date
), assessed AS (
    SELECT p.*, e.* EXCLUDE (person_id, reporting_date),
        e.feno_date IS NOT NULL OR e.reversibility_date IS NOT NULL
        OR e.pefr_variability_date IS NOT NULL OR e.bronchial_challenge_date IS NOT NULL
        OR (p.age > 16 AND e.eosinophil_or_fbc_date IS NOT NULL)
        OR (p.age <= 16 AND (e.skin_prick_date IS NOT NULL
            OR (e.ige_date IS NOT NULL AND e.eosinophil_or_fbc_date IS NOT NULL))) AS is_in_numerator,
        GREATEST_IGNORE_NULLS(e.feno_date, e.reversibility_date, e.pefr_variability_date,
            e.bronchial_challenge_date,
            IFF(p.age > 16, e.eosinophil_or_fbc_date, NULL),
            IFF(p.age <= 16, e.skin_prick_date, NULL),
            IFF(p.age <= 16 AND e.ige_date IS NOT NULL AND e.eosinophil_or_fbc_date IS NOT NULL,
                GREATEST(e.ige_date, e.eosinophil_or_fbc_date), NULL)) AS latest_record_date
    FROM cohort p
    INNER JOIN evidence e ON p.person_id = e.person_id AND p.reporting_date = e.reporting_date
)
SELECT a.person_id, 'IND272' AS indicator_id, 'Asthma: objective tests' AS indicator_name,
    a.reporting_date, DATEADD(month, -15, a.reporting_date) AS measurement_period_start,
    a.age, 'Asthma (new unresolved diagnosis, aged 5 and over)' AS condition_name,
    {{ nice_practice_columns('a', reference) }},
    a.diagnosis_date, a.feno_date, a.reversibility_date, a.pefr_variability_date,
    a.bronchial_challenge_date, a.skin_prick_date, a.ige_date, a.eosinophil_or_fbc_date,
    a.latest_record_date, TRUE AS is_in_denominator, a.is_in_numerator,
    IFF(a.is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed a
{% endmacro %}
