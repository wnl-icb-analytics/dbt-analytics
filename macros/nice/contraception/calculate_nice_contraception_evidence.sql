{% macro calculate_nice_contraception_evidence(reference='current') %}
{#-
    Select contraception evidence for active, living, non-test women under 55.
    Args: reference is current or by_month.
    Returns: latest clinical and order dates, one person per reporting_date.
-#}
WITH population AS (
    SELECT person_id, reporting_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    WHERE gender = 'Female' AND age < 55
), exclusions AS (
    SELECT person_id, MIN(event_date) AS first_sterilisation_hysterectomy_date
    FROM {{ ref('int_nice_sterilisation_hysterectomy_all') }}
    GROUP BY person_id
), advice_daily AS (
    SELECT person_id, event_date::DATE AS event_date, id
    FROM {{ ref('int_nice_contraception_advice_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date::DATE ORDER BY id DESC) = 1
), larc_daily AS (
    SELECT person_id, event_date::DATE AS event_date, id
    FROM {{ ref('int_nice_larc_advice_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, event_date::DATE ORDER BY id DESC) = 1
), asm_daily AS (
    SELECT person_id, order_date::DATE AS event_date, medication_order_id
    FROM {{ ref('int_epilepsy_medications_all') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, order_date::DATE ORDER BY medication_order_id DESC) = 1
), oral_patch_daily AS (
    SELECT person_id, order_date::DATE AS event_date, medication_order_id
    FROM {{ ref('int_contraceptive_medications_all') }}
    WHERE is_practice_issued AND source_cluster_id IN ('CONTRACEP_COC_RX', 'CONTRACEP_POP_RX', 'CONTRACEP_PATCH_RX')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, order_date::DATE ORDER BY medication_order_id DESC) = 1
), ehc_daily AS (
    SELECT person_id, order_date::DATE AS event_date, medication_order_id
    FROM {{ ref('int_contraceptive_medications_all') }}
    WHERE is_practice_issued AND source_cluster_id = 'CONTRACEP_EHC_RX'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id, order_date::DATE ORDER BY medication_order_id DESC) = 1
)
SELECT population.person_id, population.reporting_date,
    advice.event_date AS latest_advice_date,
    larc.event_date AS latest_larc_date,
    asm.event_date AS latest_asm_date,
    oral_patch.event_date AS latest_oral_patch_date,
    ehc.event_date AS latest_ehc_date,
    exclusions.first_sterilisation_hysterectomy_date
FROM population
ASOF JOIN advice_daily AS advice
    MATCH_CONDITION (population.reporting_date >= advice.event_date)
    ON population.person_id = advice.person_id
ASOF JOIN larc_daily AS larc
    MATCH_CONDITION (population.reporting_date >= larc.event_date)
    ON population.person_id = larc.person_id
ASOF JOIN asm_daily AS asm
    MATCH_CONDITION (population.reporting_date >= asm.event_date)
    ON population.person_id = asm.person_id
ASOF JOIN oral_patch_daily AS oral_patch
    MATCH_CONDITION (population.reporting_date >= oral_patch.event_date)
    ON population.person_id = oral_patch.person_id
ASOF JOIN ehc_daily AS ehc
    MATCH_CONDITION (population.reporting_date >= ehc.event_date)
    ON population.person_id = ehc.person_id
LEFT JOIN exclusions ON population.person_id = exclusions.person_id
{% endmacro %}
