{% macro calculate_nice_hba1c_evidence(reference='current') %}
{#-
    Select HbA1c, fructosamine and maximum-tolerated-treatment evidence through R.
    Args: reference is current or by_month.
    Returns: person_id, reporting_date, latest HbA1c id/date/value/validity,
             latest fructosamine date and latest DMMAX date.
-#}
-- NICE glycaemic evidence for DM, NDH, gestational diabetes and SMI members.
WITH register_people AS (
    {% for condition in ['DM', 'NDH', 'GESTDIAB', 'SMI'] %}
    SELECT
        person_id,
        reporting_date
    FROM ({{ nice_register(condition, reference) }})
    {% if not loop.last %}UNION{% endif %}
    {% endfor %}
),

population AS (
    SELECT
        population.person_id,
        population.reporting_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN register_people AS register
        ON population.person_id = register.person_id
        AND population.reporting_date = register.reporting_date
),

candidate_keys AS (
    SELECT DISTINCT person_id
    FROM population
),

hba1c_daily AS (
    SELECT
        result.person_id,
        result.id AS observation_id,
        result.clinical_effective_date::DATE AS event_date,
        result.hba1c_ifcc,
        result.is_valid_hba1c
    FROM {{ ref('int_hba1c_all') }} AS result
    INNER JOIN candidate_keys AS candidate
        ON result.person_id = candidate.person_id
    WHERE result.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    -- Keep invalid and value-free days. Valid companions win only on the same day.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY result.person_id, result.clinical_effective_date::DATE
        ORDER BY result.is_valid_hba1c DESC, result.id DESC
    ) = 1
),

{% for name, model in [('fructosamine', 'int_fructosamine_all'), ('dmmax', 'int_diabetes_max_tolerated_treatment_all')] %}
{{ name }}_daily AS (
    SELECT
        result.person_id,
        result.clinical_effective_date::DATE AS event_date
    FROM {{ ref(model) }} AS result
    INNER JOIN candidate_keys AS candidate
        ON result.person_id = candidate.person_id
    WHERE result.clinical_effective_date::DATE <= (SELECT MAX(reporting_date) FROM population)
    GROUP BY result.person_id, result.clinical_effective_date::DATE
),

{% endfor %}
selected_hba1c AS (
    SELECT
        population.person_id,
        population.reporting_date,
        result.observation_id,
        result.event_date,
        result.hba1c_ifcc,
        result.is_valid_hba1c
    FROM population
    ASOF JOIN hba1c_daily AS result
        MATCH_CONDITION (population.reporting_date >= result.event_date)
        ON population.person_id = result.person_id
)

SELECT
    hba1c.person_id,
    hba1c.reporting_date,
    hba1c.observation_id AS latest_hba1c_observation_id,
    hba1c.event_date AS latest_hba1c_date,
    hba1c.hba1c_ifcc AS latest_hba1c_value,
    COALESCE(hba1c.is_valid_hba1c, FALSE) AS is_latest_hba1c_valid,
    fructosamine.event_date AS latest_fructosamine_date,
    dmmax.event_date AS latest_dmmax_date
FROM selected_hba1c AS hba1c
ASOF JOIN fructosamine_daily AS fructosamine
    MATCH_CONDITION (hba1c.reporting_date >= fructosamine.event_date)
    ON hba1c.person_id = fructosamine.person_id
ASOF JOIN dmmax_daily AS dmmax
    MATCH_CONDITION (hba1c.reporting_date >= dmmax.event_date)
    ON hba1c.person_id = dmmax.person_id
{% endmacro %}
