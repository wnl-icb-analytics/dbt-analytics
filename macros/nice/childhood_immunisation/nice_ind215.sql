{% macro nice_ind215(reference='current') %}
WITH profile AS (
{% if reference == 'current' %}
    SELECT person_id, CURRENT_DATE()::DATE AS reporting_date, birth_date_approx,
        dtap_doses_by_8_months, has_dtap_contraindication
    FROM {{ ref('int_childhood_immunisation_profile') }}
{% else %}
    SELECT person_id, reporting_date, birth_date_approx,
        dtap_doses_by_8_months, has_dtap_contraindication
    FROM {{ ref('int_childhood_immunisation_profile_by_month') }}
{% endif %}
),
assessed AS (
    SELECT p.person_id, p.reporting_date, p.age, p.practice_code, p.practice_name,
        profile.birth_date_approx, DATEADD(month, 8, profile.birth_date_approx) AS milestone_date,
        profile.dtap_doses_by_8_months AS doses_in_window,
        profile.dtap_doses_by_8_months >= 3 AS is_in_numerator
    FROM profile INNER JOIN ({{ nice_reference_population(reference) }}) p
        ON profile.person_id = p.person_id AND profile.reporting_date = p.reporting_date
    WHERE DATEADD(month, 8, profile.birth_date_approx) > DATEADD(month, -12, p.reporting_date)
        AND DATEADD(month, 8, profile.birth_date_approx) <= p.reporting_date
        -- Retain the current contraindication proxy for confirmed anaphylaxis.
        AND NOT profile.has_dtap_contraindication
)
SELECT person_id, 'IND215' AS indicator_id, 'Immunisation: DTaP (8 months)' AS indicator_name,
    reporting_date, DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age, 'Babies reaching 8 months in the preceding 12 months' AS condition_name,
    practice_code AS {{ 'current_practice_code' if reference == 'current' else 'practice_code' }},
    practice_name AS {{ 'current_practice_name' if reference == 'current' else 'practice_name' }},
    birth_date_approx, milestone_date, doses_in_window, TRUE AS is_in_denominator,
    is_in_numerator, IFF(is_in_numerator, 'ACHIEVED', 'NOT_RECORDED_IN_PERIOD') AS indicator_status
FROM assessed
{% endmacro %}
