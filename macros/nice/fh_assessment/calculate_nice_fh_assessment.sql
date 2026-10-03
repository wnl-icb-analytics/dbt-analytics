{% macro calculate_nice_fh_assessment(reference='current') %}
{#-
    Select qualifying cholesterol readings and familial hypercholesterolaemia evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with reading and evidence dates.
-#}
WITH cholesterol_readings AS (
    SELECT person_id, id, clinical_effective_date::DATE AS reading_date, cholesterol_value
    FROM {{ ref('int_cholesterol_all') }}
    WHERE is_valid_cholesterol AND cholesterol_value > 7.5
),
cholesterol_candidates AS (
    SELECT DISTINCT person_id FROM cholesterol_readings
),
population AS (
    SELECT population.person_id, population.reporting_date, population.birth_date_approx
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN cholesterol_candidates AS candidate ON population.person_id = candidate.person_id
),
birth_dates AS (
    SELECT DISTINCT person_id, birth_date_approx FROM population
),
high_readings AS (
    SELECT
        reading.person_id,
        reading.id,
        reading.reading_date,
        reading.cholesterol_value,
        FLOOR(DATEDIFF(month, birth.birth_date_approx, reading.reading_date) / 12) AS age_at_reading
    FROM cholesterol_readings AS reading
    INNER JOIN birth_dates AS birth ON reading.person_id = birth.person_id
),
first_historical AS (
    SELECT person_id, reading_date, cholesterol_value, age_at_reading
    FROM high_readings
    WHERE age_at_reading BETWEEN 0 AND 29 OR (age_at_reading >= 30 AND cholesterol_value > 9.0)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY reading_date, id DESC) = 1
),
daily_high AS (
    SELECT person_id, reading_date, cholesterol_value
    FROM high_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, reading_date ORDER BY id DESC) = 1
),
first_high AS (
    SELECT person_id, MIN(reading_date) AS first_high_date FROM high_readings GROUP BY person_id
),
reading_candidates AS (
    SELECT population.person_id, population.reporting_date
    FROM population
    INNER JOIN first_high ON population.person_id = first_high.person_id AND first_high.first_high_date <= population.reporting_date
),
readings_at_reference AS (
    SELECT
        population.person_id,
        population.reporting_date,
        historical.reading_date AS earliest_qualifying_reading_date,
        historical.cholesterol_value AS earliest_qualifying_cholesterol_value,
        historical.age_at_reading AS age_at_earliest_qualifying_reading,
        latest.reading_date AS latest_high_reading_date,
        latest.cholesterol_value AS latest_high_cholesterol_value
    FROM reading_candidates AS population
    ASOF JOIN daily_high AS latest
        MATCH_CONDITION (population.reporting_date >= latest.reading_date)
        ON population.person_id = latest.person_id
    LEFT JOIN first_historical AS historical
        ON population.person_id = historical.person_id AND historical.reading_date <= population.reporting_date
    WHERE historical.reading_date IS NOT NULL
        OR latest.reading_date > DATEADD(month, -12, population.reporting_date)
),
candidate_keys AS (
    SELECT DISTINCT person_id FROM readings_at_reference
),
first_evidence AS (
    SELECT
        evidence.person_id,
        MIN(IFF(evidence.is_fh_assessment, evidence.clinical_effective_date::DATE, NULL)) AS first_fh_assessment_date,
        MIN(IFF(evidence.is_clinical_fh_diagnosis, evidence.clinical_effective_date::DATE, NULL)) AS first_clinical_fh_diagnosis_date,
        MIN(IFF(evidence.is_fh_referral, evidence.clinical_effective_date::DATE, NULL)) AS first_fh_referral_date,
        MIN(IFF(evidence.is_genetic_fh, evidence.clinical_effective_date::DATE, NULL)) AS first_genetic_fh_date,
        MIN(IFF(evidence.is_secondary_hyperlipidaemia_history, evidence.clinical_effective_date::DATE, NULL)) AS first_secondary_history_date
    FROM {{ ref('int_nice_fh_assessment_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate ON evidence.person_id = candidate.person_id
    GROUP BY evidence.person_id
),
daily_secondary AS (
    SELECT evidence.person_id, evidence.clinical_effective_date::DATE AS diagnosis_date
    FROM {{ ref('int_nice_fh_assessment_all') }} AS evidence
    INNER JOIN candidate_keys AS candidate ON evidence.person_id = candidate.person_id
    WHERE evidence.is_secondary_hyperlipidaemia
    GROUP BY evidence.person_id, evidence.clinical_effective_date::DATE
)
SELECT
    readings.person_id,
    readings.reporting_date,
    readings.earliest_qualifying_reading_date,
    readings.earliest_qualifying_cholesterol_value,
    readings.age_at_earliest_qualifying_reading,
    readings.latest_high_reading_date,
    readings.latest_high_cholesterol_value,
    IFF(evidence.first_fh_assessment_date <= readings.reporting_date, evidence.first_fh_assessment_date, NULL) AS first_fh_assessment_date,
    IFF(evidence.first_clinical_fh_diagnosis_date <= readings.reporting_date, evidence.first_clinical_fh_diagnosis_date, NULL) AS first_clinical_fh_diagnosis_date,
    IFF(evidence.first_fh_referral_date <= readings.reporting_date, evidence.first_fh_referral_date, NULL) AS first_fh_referral_date,
    IFF(evidence.first_genetic_fh_date <= readings.reporting_date, evidence.first_genetic_fh_date, NULL) AS first_genetic_fh_date,
    secondary.diagnosis_date AS latest_secondary_hyperlipidaemia_date,
    IFF(evidence.first_secondary_history_date <= readings.reporting_date, evidence.first_secondary_history_date, NULL) AS first_secondary_history_date
FROM readings_at_reference AS readings
ASOF JOIN daily_secondary AS secondary
    MATCH_CONDITION (readings.reporting_date >= secondary.diagnosis_date)
    ON readings.person_id = secondary.person_id
LEFT JOIN first_evidence AS evidence ON readings.person_id = evidence.person_id
{% endmacro %}
