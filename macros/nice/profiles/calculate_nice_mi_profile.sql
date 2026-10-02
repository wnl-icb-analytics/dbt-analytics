{% macro calculate_nice_mi_profile(reference='current') %}
WITH population AS (
    {{ nice_reference_population(reference) }}
),
mi_dates AS (
    SELECT
        population.person_id,
        population.reporting_date,
        MIN(mi.clinical_effective_date::DATE) AS earliest_mi_date,
        MAX(mi.clinical_effective_date::DATE) AS latest_mi_date,
        MAX(IFF(mi.clinical_effective_date::DATE >= {{ nice_financial_year_start('population.reporting_date') }},
            mi.clinical_effective_date::DATE, NULL)) AS latest_mi_in_financial_year_date
    FROM population
    INNER JOIN {{ ref('int_myocardial_infarction_diagnoses_all') }} AS mi
        ON population.person_id = mi.person_id
        AND {{ ltc_register_known_by('mi.clinical_effective_date', 'mi.date_recorded', 'population.reporting_date') }}
    GROUP BY population.person_id, population.reporting_date
),
lvsd AS (
    SELECT mi.person_id, mi.reporting_date, MIN(hf.clinical_effective_date::DATE) AS first_lvsd_date
    FROM mi_dates AS mi
    INNER JOIN {{ ref('int_heart_failure_diagnoses_all') }} AS hf
        ON mi.person_id = hf.person_id
        AND {{ ltc_register_known_by('hf.clinical_effective_date', 'hf.date_recorded', 'mi.reporting_date') }}
    WHERE hf.is_hf_lvsd_code OR hf.is_reduced_ef_code
    GROUP BY mi.person_id, mi.reporting_date
),
intolerance_events AS (
    SELECT person_id, clinical_effective_date::DATE AS event_date, is_persisting, 'ACE_INHIBITOR' AS drug_class
    FROM {{ ref('int_ras_contraindication_all') }} WHERE drug_class = 'ACE_INHIBITOR'
    UNION ALL
    SELECT person_id, clinical_effective_date::DATE AS event_date, is_persisting, 'ASPIRIN' AS drug_class
    FROM {{ ref('int_antithrombotic_contraindication_all') }} WHERE drug_class = 'SALICYLATE'
),
intolerance AS (
    SELECT
        mi.person_id,
        mi.reporting_date,
        COUNT_IF(evidence.drug_class = 'ACE_INHIBITOR') > 0 AS has_ace_inhibitor_intolerance,
        COUNT_IF(evidence.drug_class = 'ASPIRIN') > 0 AS has_aspirin_intolerance
    FROM mi_dates AS mi
    LEFT JOIN intolerance_events AS evidence
        ON mi.person_id = evidence.person_id
        AND evidence.event_date <= mi.reporting_date
        AND (evidence.is_persisting OR evidence.event_date >= DATEADD(month, -12, mi.reporting_date))
    GROUP BY mi.person_id, mi.reporting_date
),
aspirin_daily AS (
    SELECT person_id, clinical_effective_date::DATE AS event_date
    FROM {{ ref('int_antithrombotic_treatment_all') }}
    WHERE source_cluster_id = 'OSAL_COD'
    GROUP BY person_id, clinical_effective_date::DATE
),
selected_aspirin AS (
    SELECT mi.person_id, mi.reporting_date, aspirin.event_date AS latest_aspirin_record_date
    FROM mi_dates AS mi
    ASOF JOIN aspirin_daily AS aspirin
        MATCH_CONDITION (mi.reporting_date >= aspirin.event_date)
        ON mi.person_id = aspirin.person_id
)
SELECT
    mi.person_id,
    mi.reporting_date,
    mi.earliest_mi_date,
    mi.latest_mi_date,
    mi.latest_mi_in_financial_year_date,
    lvsd.first_lvsd_date,
    lvsd.first_lvsd_date IS NOT NULL AS has_lvsd,
    intolerance.has_ace_inhibitor_intolerance,
    intolerance.has_aspirin_intolerance,
    aspirin.latest_aspirin_record_date,
    therapy.antiplatelet_chemical_count_in_period,
    {% for name in ['ace_inhibitor', 'arb', 'beta_blocker', 'aspirin', 'p2y12', 'clopidogrel', 'statin', 'antiplatelet', 'anticoagulant'] %}
    therapy.latest_{{ name }}_order_date{% if not loop.last %},{% endif %}
    {% endfor %}
FROM mi_dates AS mi
LEFT JOIN lvsd ON mi.person_id = lvsd.person_id AND mi.reporting_date = lvsd.reporting_date
LEFT JOIN intolerance ON mi.person_id = intolerance.person_id AND mi.reporting_date = intolerance.reporting_date
LEFT JOIN selected_aspirin AS aspirin ON mi.person_id = aspirin.person_id AND mi.reporting_date = aspirin.reporting_date
LEFT JOIN {{ nice_ref('int_nice_cardiac_therapy', reference) }} AS therapy
    ON mi.person_id = therapy.person_id AND mi.reporting_date = therapy.reporting_date
{% endmacro %}
