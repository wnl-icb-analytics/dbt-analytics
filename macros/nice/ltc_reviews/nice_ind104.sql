{% macro nice_ind104(reference='current') %}
{#-
    Calculate NICE IND104 from known first/new diagnoses and capped review evidence.
    Args: reference is current or by_month.
    Returns: the IND104 detail projection, one eligible person per reporting_date.
-#}
-- NICE IND104: https://www.nice.org.uk/indicators/ind104
-- Depression review 10 to 35 days after a new diagnosis, with completed follow-up only.
WITH reference_dates AS (
    SELECT reporting_date AS reference_date
    FROM ({{ nice_reference_dates(reference) }})
),

keyed_diagnoses AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS diagnosis_date,
        TO_CHAR(clinical_effective_date, 'YYYYMMDDHH24MISS.FF9')
            || ':' || id::VARCHAR AS record_key,
        {{ ltc_known_date('clinical_effective_date', 'date_recorded') }} AS known_date
    FROM {{ ref('int_depression_diagnoses_all') }}
    WHERE is_diagnosis_code
        AND is_first_or_new_episode
),

diagnosis_keys AS (
    SELECT
        person_id,
        record_key,
        MIN(known_date) AS known_date
    FROM keyed_diagnoses
    GROUP BY person_id, record_key
),

selected_keys AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM diagnosis_keys', 'reference_dates') }}
),

diagnosis_payload AS (
    SELECT
        person_id,
        record_key,
        MIN(diagnosis_date) AS diagnosis_date
    FROM keyed_diagnoses
    GROUP BY person_id, record_key
),

new_depression AS (
    SELECT
        keys.person_id,
        keys.reference_date AS reporting_date,
        diagnosis.diagnosis_date
    FROM selected_keys AS keys
    INNER JOIN diagnosis_payload AS diagnosis
        ON keys.person_id = diagnosis.person_id
        AND keys.record_key = diagnosis.record_key
),

anchors AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        diagnosis.diagnosis_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN new_depression AS diagnosis
        ON population.person_id = diagnosis.person_id
        AND population.reporting_date = diagnosis.reporting_date
    WHERE population.age >= 18
        AND diagnosis.diagnosis_date > DATEADD(month, -12, DATEADD(day, -35, population.reporting_date))
        AND diagnosis.diagnosis_date <= DATEADD(day, -35, population.reporting_date)
),

follow_up AS (
    SELECT
        anchor.person_id,
        anchor.reporting_date,
        MIN(review.clinical_effective_date::DATE) AS latest_review_date
    FROM anchors AS anchor
    LEFT JOIN {{ ref('int_ltc_review_all') }} AS review
        ON anchor.person_id = review.person_id
        AND review.review_type = 'DEPRESSION_REVIEW'
        -- A qualifying review cannot occur after the reference date.
        AND review.clinical_effective_date::DATE
            BETWEEN DATEADD(day, 10, anchor.diagnosis_date)
                AND LEAST(DATEADD(day, 35, anchor.diagnosis_date), anchor.reporting_date)
    GROUP BY anchor.person_id, anchor.reporting_date
)

SELECT
    anchor.person_id,
    'IND104' AS indicator_id,
    'Depression and anxiety: review within 10 to 35 days' AS indicator_name,
    'The percentage of patients with a new diagnosis of depression in the preceding 1 April to 31 March who have been reviewed within 10 to 35 days of the date of diagnosis.' AS indicator_description,
    anchor.reporting_date,
    DATEADD(month, -12, DATEADD(day, -35, anchor.reporting_date)) AS measurement_period_start,
    anchor.age,
    'New depression diagnosis, aged 18 or over'::VARCHAR(57) AS denominator_description,
    {{ nice_practice_columns('anchor', reference) }},
    anchor.diagnosis_date,
    follow_up.latest_review_date,
    follow_up.latest_review_date AS latest_record_date,
    TRUE AS is_in_denominator,
    follow_up.latest_review_date IS NOT NULL AS is_in_numerator,
    IFF(follow_up.latest_review_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM anchors AS anchor
INNER JOIN follow_up
    ON anchor.person_id = follow_up.person_id
    AND anchor.reporting_date = follow_up.reporting_date
{% endmacro %}
