{% macro nice_ind156(reference='current') %}
{#- Calculate IND156 at person/reporting-date grain from paired LTC and smoking inputs. -#}
-- NICE IND156: https://www.nice.org.uk/indicators/ind156
-- Smoking status recorded in 12 months for people with a listed LTC; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest listed diagnosis.
WITH indicator_population AS (
    SELECT
        profile.person_id,
        profile.birth_date_approx,
        profile.earliest_smoking_ltc_diagnosis_date,
        population.age,
        population.reporting_date AS evaluation_date,
        population.practice_code,
        population.practice_name,
        smoking.latest_smoking_status,
        smoking.latest_smoking_status_date,
        smoking.latest_never_smoked_date,
        smoking.latest_smoking_intervention_date
    FROM {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_smoking_evidence', reference) }} AS smoking
        ON profile.person_id = smoking.person_id
        AND profile.reporting_date = smoking.reporting_date
    WHERE (
            profile.has_chd OR profile.has_pad OR profile.has_stroke_tia OR profile.has_hypertension
            OR profile.has_diabetes OR profile.has_copd OR profile.has_ckd OR profile.has_asthma
        )
),

assessed AS (
    SELECT
        population.person_id,
        population.age,
        population.evaluation_date AS reporting_date,
        population.practice_code,
        population.practice_name,
        population.latest_smoking_status,
        population.latest_smoking_status_date,
        population.latest_never_smoked_date,
        population.latest_smoking_intervention_date,
        COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, population.evaluation_date), FALSE) AS is_status_recorded_in_period,
        COALESCE(
            DATEADD(year, 26, population.birth_date_approx)
                <= {{ nice_financial_year_end('population.evaluation_date') }}
            AND population.latest_smoking_status = 'Never Smoked'
            AND population.latest_never_smoked_date > DATEADD(year, 25, population.birth_date_approx)
            AND population.latest_never_smoked_date > population.earliest_smoking_ltc_diagnosis_date,
            FALSE
        ) AS is_never_smoker_covered,
        CASE WHEN COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, population.evaluation_date), FALSE)
            THEN population.latest_smoking_status_date END AS latest_record_date,
        COALESCE(population.latest_smoking_status_date >= DATEADD(month, -12, population.evaluation_date), FALSE)
        OR COALESCE(
            DATEADD(year, 26, population.birth_date_approx)
                <= {{ nice_financial_year_end('population.evaluation_date') }}
            AND population.latest_smoking_status = 'Never Smoked'
            AND population.latest_never_smoked_date > DATEADD(year, 25, population.birth_date_approx)
            AND population.latest_never_smoked_date > population.earliest_smoking_ltc_diagnosis_date,
            FALSE
        ) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND156' AS indicator_id,
    'Smoking: smoking status of people with long-term conditions' AS indicator_name,
    'The percentage of patients with any or any combination of the following conditions: CHD, PAD, stroke or TIA, hypertension, diabetes, COPD, CKD or asthma, whose notes record smoking status in the preceding 12 months.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Listed long-term conditions' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_smoking_status,
    latest_smoking_status_date,
    latest_never_smoked_date,
    latest_smoking_intervention_date,
    latest_record_date,
    is_status_recorded_in_period,
    is_never_smoker_covered,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed

{% endmacro %}
