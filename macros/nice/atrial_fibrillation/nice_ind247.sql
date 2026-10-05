{% macro nice_ind247(reference='current') %}
{#- Calculate IND247 at eligible person/reporting-date grain in current or by_month mode. -#}
-- NICE IND247: https://www.nice.org.uk/indicators/ind247
-- DOAC order in 6 months, or a VKA order for valvular AF, antiphospholipid syndrome or a DOAC exception,
-- for people on the AF register with a latest CHA2DS2-VASc of 2 or more.
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
    WHERE profile.latest_chadsvasc_score >= 2
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
        COALESCE(population.latest_doac_order_date >= DATEADD(month, -6, population.reporting_date), FALSE) AS is_doac_in_period,
        COALESCE(population.latest_vka_order_date >= DATEADD(month, -6, population.reporting_date), FALSE) AS is_vka_in_period,
        -- NICE excludes valvular AF from DOAC success. Antiphospholipid syndrome allows either success.
        CASE
            WHEN population.is_doac_ineligible THEN is_vka_in_period
            ELSE is_doac_in_period OR (is_vka_in_period AND population.has_doac_exception)
        END AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND247' AS indicator_id,
    'Atrial fibrillation: DOACs and Vitamin K antagonists' AS indicator_name,
    reporting_date,
    DATEADD(month, -6, reporting_date) AS measurement_period_start,
    age,
    'Atrial fibrillation with CHA2DS2-VASc 2 or more' AS condition_name,
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
        WHEN is_doac_ineligible AND is_doac_in_period THEN 'DOAC_WHERE_VKA_INDICATED'
        WHEN is_vka_in_period THEN 'VKA_WITHOUT_DOAC_EXCEPTION'
        WHEN latest_anticoagulant_order_date IS NOT NULL THEN 'NOT_TREATED_IN_PERIOD'
        ELSE 'NEVER_TREATED'
    END AS indicator_status
FROM assessed
{% endmacro %}
