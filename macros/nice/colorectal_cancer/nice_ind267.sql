{#-
    Calculate NICE IND267 for its eligible population at each reference date.
    Args: reference is current or by_month.
    Returns: the IND267 detail columns, one person per reporting_date.
-#}
{% macro nice_ind267(reference='current') %}
-- NICE IND267: https://www.nice.org.uk/indicators/ind267
-- FIT recorded within 21 days before the latest urgent colorectal cancer referral in the preceding twelve months.
WITH population AS ({{ nice_reference_population(reference) }}),
daily_referral AS (
    SELECT person_id, event_date, id
    FROM {{ ref('int_nice_colorectal_referral_fit_all') }}
    WHERE source_cluster_id = 'GICANREF_COD'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY id DESC) = 1
), selected_referral AS (
    SELECT population.*, referral.event_date AS referral_date, referral.id AS referral_id
    FROM population
    ASOF JOIN daily_referral AS referral
        MATCH_CONDITION (population.reporting_date >= referral.event_date)
        ON population.person_id = referral.person_id
), eligible AS (
    SELECT * FROM selected_referral
    WHERE referral_date > DATEADD(month, -12, reporting_date)
), daily_fit AS (
    SELECT person_id, event_date, id
    FROM {{ ref('int_nice_colorectal_referral_fit_all') }}
    WHERE source_cluster_id = 'FAECIMM_COD'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date ORDER BY id DESC) = 1
), assessed AS (
    SELECT eligible.*,
        IFF(fit.event_date >= DATEADD(day, -21, eligible.referral_date), fit.event_date, NULL) AS latest_record_date
    FROM eligible
    ASOF JOIN daily_fit AS fit
        MATCH_CONDITION (eligible.referral_date >= fit.event_date)
        ON eligible.person_id = fit.person_id
)
SELECT person_id, 'IND267' AS indicator_id, 'Cancer: faecal immunochemical testing' AS indicator_name,
'The percentage of urgent suspected colorectal cancer referrals accompanied by a faecal immunochemical test (FIT) result, with the result recorded in the 21 days leading up to the referral.' AS indicator_description,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Urgent suspected colorectal cancer referral' AS denominator_description,
    {{ nice_practice_columns('result', reference) }},
    referral_date, referral_id, latest_record_date, TRUE AS is_in_denominator,
    latest_record_date IS NOT NULL AS is_in_numerator,
    IFF(latest_record_date IS NOT NULL, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed AS result
{% endmacro %}
