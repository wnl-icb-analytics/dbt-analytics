{% macro calculate_nice_multimorbidity_categories(reference='current') %}
{#-
    Calculate reviewed NICE IND207 category membership at each reference date.
    Args: reference is current or by_month; dates and register membership use adapters.
    Returns: person_id, category_code, existing/additional evidence flags and reporting_date.
    Grain is person/date/category. Measures apply active, non-test and age eligibility.
-#}
-- NICE IND207 categories, counted once per person and reference date.
WITH reference_dates AS (
    SELECT reporting_date AS reference_date
    FROM ({{ nice_reference_dates(reference) }})
),

register_categories AS (
    SELECT
        person_id,
        reporting_date,
        CASE
            WHEN condition_code = 'CAN' THEN 'CANCER'
            WHEN condition_code IN ('CHD', 'AF', 'HF', 'HTN', 'STIA', 'PAD') THEN 'CIRCULATORY'
            WHEN condition_code = 'DM' THEN 'DIABETES'
            WHEN condition_code = 'CLD' THEN 'DIGESTIVE'
            WHEN condition_code IN ('LD', 'LD_U14') THEN 'LEARNING_DISABILITY'
            WHEN condition_code IN ('ANX', 'DEP', 'DEM')
                OR (condition_code = 'SMI' AND earliest_diagnosis_date IS NOT NULL) THEN 'MENTAL_HEALTH'
            WHEN condition_code = 'RA' THEN 'MUSCULOSKELETAL'
            WHEN condition_code IN ('EP', 'MS', 'PD') THEN 'NEUROLOGICAL'
            WHEN condition_code = 'CKD' THEN 'RENAL'
            WHEN condition_code IN ('AST', 'CYP_AST', 'COPD') THEN 'RESPIRATORY'
        END AS category_code
    FROM ({{ nice_ltc_summary(reference) }})
),

alcohol_history AS (
    SELECT
        person_id,
        MIN({{ ltc_known_date('clinical_effective_date', 'date_recorded') }}) AS first_known_date
    -- Keep the reviewed alcohol event-age floor of 16 for multimorbidity.
    FROM {{ ref('int_alcohol_misuse_disorders') }}
    GROUP BY person_id
),

alcohol_categories AS (
    SELECT
        alcohol.person_id,
        dates.reference_date AS reporting_date,
        'MENTAL_HEALTH' AS category_code
    FROM alcohol_history AS alcohol
    -- The existing hub supplies alcohol flags only for people in dim_person.
    INNER JOIN {{ ref('dim_person') }} AS person
        ON alcohol.person_id = person.person_id
    INNER JOIN reference_dates AS dates
        ON alcohol.first_known_date <= dates.reference_date
),

diagnosis_history AS (
    SELECT
        person_id,
        category_code,
        MIN({{ ltc_known_date('clinical_effective_date', 'date_recorded') }}) AS first_known_date
    FROM {{ ref('int_nice_multimorbidity_diagnoses_all') }}
    GROUP BY person_id, category_code
),

diagnosis_categories AS (
    SELECT
        diagnosis.person_id,
        dates.reference_date AS reporting_date,
        diagnosis.category_code
    FROM diagnosis_history AS diagnosis
    INNER JOIN reference_dates AS dates
        ON diagnosis.first_known_date <= dates.reference_date
),

substance_records AS (
    SELECT
        person_id,
        -- Preserve timestamp order; QUALIFYING wins equal-timestamp ties, then id.
        TO_CHAR(clinical_effective_date, 'YYYYMMDDHH24MISS.FF9')
            || ':' || IFF(status = 'QUALIFYING', '1', '0')
            || ':' || id::VARCHAR AS record_key,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date,
        status
    FROM {{ ref('int_substance_misuse_all') }}
),

substance_keys AS (
    SELECT
        person_id,
        record_key,
        MIN(known_date) AS known_date
    FROM substance_records
    GROUP BY person_id, record_key
),

selected_substance_keys AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM substance_keys', 'reference_dates') }}
),

substance_payload AS (
    SELECT
        person_id,
        record_key,
        MAX(status) AS status
    FROM substance_records
    GROUP BY person_id, record_key
),

substance_categories AS (
    SELECT
        selected.person_id,
        selected.reference_date AS reporting_date,
        'MENTAL_HEALTH' AS category_code
    FROM selected_substance_keys AS selected
    INNER JOIN substance_payload AS payload
        ON selected.person_id = payload.person_id
        AND selected.record_key = payload.record_key
    WHERE payload.status = 'QUALIFYING'
),

epilepsy_history AS (
    SELECT
        person_id,
        MIN({{ ltc_known_date('clinical_effective_date', 'date_recorded') }}) AS first_known_date
    FROM {{ ref('int_epilepsy_diagnoses_all') }}
    -- Any known diagnosis excludes the antiepileptic route, including resolved epilepsy.
    WHERE is_diagnosis_code
    GROUP BY person_id
),

prescription_counts AS (
    SELECT
        prescribing.person_id,
        dates.reference_date AS reporting_date,
        COUNT(DISTINCT IFF(prescribing.conditionid = 5065, prescribing.medication_order_id, NULL)) AS analgesic_issues,
        COUNT(DISTINCT IFF(prescribing.conditionid = 5066, prescribing.medication_order_id, NULL)) AS antiepileptic_issues,
        COUNT(DISTINCT IFF(prescribing.conditionid = 5071, prescribing.medication_order_id, NULL)) AS laxative_issues
    FROM {{ ref('int_ccms_medication_orders_all') }} AS prescribing
    INNER JOIN reference_dates AS dates
        -- Inclusive 12-month window; count distinct issues rather than code-set matches.
        ON prescribing.order_date BETWEEN DATEADD(month, -12, dates.reference_date)
            AND dates.reference_date
    WHERE prescribing.conditionid IN (5065, 5066, 5071)
    GROUP BY prescribing.person_id, dates.reference_date
),

prescription_categories AS (
    SELECT
        prescribing.person_id,
        prescribing.reporting_date,
        'CHRONIC_PAIN' AS category_code
    FROM prescription_counts AS prescribing
    LEFT JOIN epilepsy_history AS epilepsy
        ON prescribing.person_id = epilepsy.person_id
        AND epilepsy.first_known_date <= prescribing.reporting_date
    WHERE prescribing.analgesic_issues >= 4
        OR (prescribing.antiepileptic_issues >= 4 AND epilepsy.person_id IS NULL)

    UNION ALL

    SELECT
        person_id,
        reporting_date,
        'DIGESTIVE' AS category_code
    FROM prescription_counts
    WHERE laxative_issues >= 4
),

category_evidence AS (
    SELECT
        person_id,
        reporting_date,
        category_code,
        TRUE AS has_existing_evidence,
        FALSE AS has_additional_evidence
    FROM register_categories
    WHERE category_code IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        reporting_date,
        category_code,
        TRUE,
        FALSE
    FROM alcohol_categories

    UNION ALL

    SELECT
        person_id,
        reporting_date,
        category_code,
        FALSE,
        TRUE
    FROM diagnosis_categories

    UNION ALL

    SELECT
        person_id,
        reporting_date,
        category_code,
        FALSE,
        TRUE
    FROM substance_categories

    UNION ALL

    SELECT
        person_id,
        reporting_date,
        category_code,
        FALSE,
        TRUE
    FROM prescription_categories
)
SELECT
    person_id,
    category_code,
    BOOLOR_AGG(has_existing_evidence) AS has_existing_evidence,
    BOOLOR_AGG(has_additional_evidence) AS has_additional_evidence,
    reporting_date
FROM category_evidence
WHERE person_id IS NOT NULL
GROUP BY person_id, reporting_date, category_code
{% endmacro %}
