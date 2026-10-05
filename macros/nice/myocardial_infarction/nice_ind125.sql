{#-
    Calculate NICE IND125 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND125 detail columns, one person per reporting_date.
-#}
{% macro nice_ind125(reference='current') %}
-- NICE IND125: https://www.nice.org.uk/indicators/ind125
-- Required medication classes in six months for people with myocardial infarction in the financial year, with documented substitutions and LVSD-dependent beta blockers.
WITH assessed AS (
    SELECT
        profile.*,
        population.age,
        population.practice_code,
        population.practice_name,
        COALESCE(profile.latest_ace_inhibitor_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_ace_inhibitor_in_period,
        COALESCE(profile.latest_arb_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_arb_in_period,
        COALESCE(GREATEST_IGNORE_NULLS(profile.latest_aspirin_order_date, profile.latest_aspirin_record_date)::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_aspirin_in_period,
        COALESCE(profile.latest_p2y12_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_p2y12_in_period,
        COALESCE(profile.latest_clopidogrel_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_clopidogrel_in_period,
        COALESCE(profile.latest_antiplatelet_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_antiplatelet_in_period,
        COALESCE(profile.latest_anticoagulant_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_anticoagulant_in_period,
        COALESCE(profile.latest_statin_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_statin_in_period,
        COALESCE(profile.latest_beta_blocker_order_date::DATE BETWEEN DATEADD(month, -6, profile.reporting_date) AND profile.reporting_date, FALSE) AS is_beta_blocker_in_period
    FROM {{ nice_ref('int_nice_mi_profile', reference) }} AS profile
    INNER JOIN ({{ nice_reference_population(reference) }}) AS population
        ON profile.person_id = population.person_id AND profile.reporting_date = population.reporting_date
    WHERE profile.latest_mi_in_financial_year_date IS NOT NULL
),
result AS (
    SELECT *,
        (is_ace_inhibitor_in_period OR (is_arb_in_period AND has_ace_inhibitor_intolerance))
            AND is_aspirin_in_period AND is_p2y12_in_period AND is_statin_in_period
            AND (NOT has_lvsd OR is_beta_blocker_in_period) AS is_in_numerator,
        (is_ace_inhibitor_in_period OR is_arb_in_period)
            -- The lenient result accepts any two BNF chemicals, including OTC aspirin.
            AND (antiplatelet_chemical_count_in_period
                + IFF(is_aspirin_in_period AND NOT COALESCE(latest_aspirin_order_date::DATE
                    BETWEEN DATEADD(month, -6, reporting_date) AND reporting_date, FALSE), 1, 0) >= 2)
            AND is_statin_in_period
            AND (NOT has_lvsd OR is_beta_blocker_in_period) AS is_in_numerator_lenient
    FROM assessed
)
SELECT
    person_id,
    'IND125' AS indicator_id,
    'Myocardial infarction: medication for MI in preceding 12 months' AS indicator_name,
    reporting_date,
    {{ nice_financial_year_start('reporting_date') }} AS measurement_period_start,
    age,
    'Myocardial infarction in the financial year' AS condition_name,
    {{ nice_practice_columns('result', reference) }},
    earliest_mi_date,
    latest_mi_date,
    latest_mi_in_financial_year_date,
    first_lvsd_date,
    has_lvsd,
    has_ace_inhibitor_intolerance,
    has_aspirin_intolerance,
    latest_aspirin_record_date,
    antiplatelet_chemical_count_in_period,
    latest_ace_inhibitor_order_date,
    latest_arb_order_date,
    latest_aspirin_order_date,
    latest_p2y12_order_date,
    latest_clopidogrel_order_date,
    latest_antiplatelet_order_date,
    latest_anticoagulant_order_date,
    latest_statin_order_date,
    latest_beta_blocker_order_date,
    is_ace_inhibitor_in_period,
    is_arb_in_period,
    is_aspirin_in_period,
    is_p2y12_in_period,
    is_clopidogrel_in_period,
    is_antiplatelet_in_period,
    is_anticoagulant_in_period,
    is_statin_in_period,
    is_beta_blocker_in_period,
    is_in_numerator_lenient,
    GREATEST_IGNORE_NULLS(latest_ace_inhibitor_order_date, latest_arb_order_date, latest_aspirin_order_date,
        latest_aspirin_record_date, latest_p2y12_order_date, latest_anticoagulant_order_date,
        latest_statin_order_date, latest_beta_blocker_order_date)::DATE AS latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    IFF(is_in_numerator, 'ACHIEVED', 'NOT_TREATED_IN_PERIOD') AS indicator_status
FROM result
{% endmacro %}
