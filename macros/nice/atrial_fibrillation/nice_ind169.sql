{% macro nice_ind169(reference='current') %}
{#- Calculate IND169 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND169: https://www.nice.org.uk/indicators/ind169
-- Anticoagulant medication review in 12 months for people over 18 on the AF register with an oral anticoagulant order in the preceding 6 months.
WITH indicator_population AS (
    SELECT
        profile.*,
        population.age,
        population.practice_code,
        population.practice_name
    FROM {{ nice_ref('int_atrial_fibrillation_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id
        AND profile.reporting_date = population.reporting_date
    WHERE population.age > 18
        AND COALESCE(profile.latest_anticoagulant_order_date >= DATEADD(month, -6, profile.reporting_date), FALSE)
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        population.latest_chadsvasc_score,
        population.latest_chadsvasc_date,
        population.latest_chads2_score,
        population.latest_anticoagulant_order_date,
        population.latest_anticoagulant_type,
        population.latest_doac_order_date,
        population.latest_vka_order_date,
        population.is_doac_ineligible,
        population.has_doac_exception,
        population.latest_anticoagulant_review_date,
        COALESCE(population.latest_anticoagulant_review_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND169' AS indicator_id,
    'Atrial fibrillation: review of anticoagulation' AS indicator_name,
    'The percentage of patients with atrial fibrillation, currently treated with an anticoagulant, who have had a review in the preceding 12 months which included: assessment of stroke/VTE risk assessment of bleeding risk assessment of renal function, creatinine clearance, FBC and LFTs as appropriate for their anticoagulation therapy any adverse effects related to anticoagulation assessment of compliance choice of anticoagulant.' AS indicator_description,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Atrial fibrillation on anticoagulation, aged over 18' AS denominator_description,
    {{ nice_practice_columns('assessed', reference) }},
    latest_chadsvasc_score,
    latest_chadsvasc_date,
    latest_chads2_score,
    latest_anticoagulant_order_date,
    latest_anticoagulant_type,
    latest_doac_order_date,
    latest_vka_order_date,
    is_doac_ineligible,
    has_doac_exception,
    latest_anticoagulant_review_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
