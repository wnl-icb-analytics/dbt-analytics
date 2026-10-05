{% macro nice_ind320(reference='current') %}
{#-
    Calculate adult NICE BMI recording with the reviewed LTC and dyslipidaemia rules.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date with the original IND320 detail.
-#}
-- NICE IND320: https://www.nice.org.uk/indicators/ind320
-- BMI recorded in 12 months for people with CHD, stroke/TIA, diabetes, non-diabetic hyperglycaemia, hypertension, PAD, heart failure, COPD, dyslipidaemia, learning disability, obstructive sleep apnoea or SMI.
WITH adults AS (
    SELECT
        person_id,
        reporting_date,
        age,
        gender,
        practice_code,
        practice_name
    FROM ({{ nice_reference_population(reference) }})
    -- NICE's rationale concerns adult weight management.
    WHERE age >= 18
),

adult_people AS (
    SELECT DISTINCT person_id
    FROM adults
),

lipid_treatment_daily AS (
    -- IND320 includes adults outside the shared therapy profile's candidate ages.
    SELECT DISTINCT
        orders.person_id,
        orders.order_date::DATE AS event_date
    FROM {{ ref('int_lipid_lowering_medications_all') }} AS orders
    INNER JOIN adult_people AS adult
        ON orders.person_id = adult.person_id
    WHERE orders.lipid_lowering_class IN (
        'STATIN', 'STATIN_COMBINATION', 'EZETIMIBE', 'BEMPEDOIC_ACID',
        'PCSK9_INHIBITOR', 'INCLISIRAN', 'FIBRATE', 'BILE_ACID_SEQUESTRANT'
    )
),

lipid_treatment_selected AS (
    SELECT
        adult.person_id,
        adult.reporting_date,
        orders.event_date
    FROM adults AS adult
    ASOF JOIN lipid_treatment_daily AS orders
        MATCH_CONDITION (adult.reporting_date >= orders.event_date)
        ON adult.person_id = orders.person_id
),

lipid_events AS (
    {% for model, value, validity, kind in [
        ('int_cholesterol_ldl_all', 'cholesterol_value', 'is_valid_cholesterol', 'LDL'),
        ('int_triglycerides_all', 'triglycerides_value', 'is_valid_triglycerides', 'TRIGLYCERIDES'),
        ('int_cholesterol_hdl_all', 'cholesterol_value', 'is_valid_cholesterol', 'HDL')
    ] %}
    SELECT
        lipid.person_id,
        lipid.id,
        lipid.clinical_effective_date,
        lipid.{{ value }} AS lipid_value,
        lipid.{{ validity }} AS is_valid,
        '{{ kind }}' AS lipid_kind
    FROM {{ ref(model) }} AS lipid
    INNER JOIN adult_people AS adult
        ON lipid.person_id = adult.person_id
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

lipid_daily AS (
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        lipid_value,
        is_valid,
        lipid_kind
    FROM lipid_events
    -- A later invalid timestamp prevents fallback. Valid evidence wins only at the same timestamp.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id, lipid_kind, clinical_effective_date::DATE
        ORDER BY clinical_effective_date DESC, is_valid DESC, id DESC
    ) = 1
),

lipid_candidates AS (
    SELECT
        adult.person_id,
        adult.reporting_date,
        adult.gender,
        kind.lipid_kind
    FROM adults AS adult
    CROSS JOIN (SELECT column1 AS lipid_kind FROM VALUES ('LDL'), ('TRIGLYCERIDES'), ('HDL')) AS kind
),

lipid_selected AS (
    SELECT
        candidate.person_id,
        candidate.reporting_date,
        candidate.gender,
        candidate.lipid_kind,
        lipid.event_date,
        lipid.lipid_value,
        lipid.is_valid
    FROM lipid_candidates AS candidate
    ASOF JOIN lipid_daily AS lipid
        MATCH_CONDITION (candidate.reporting_date >= lipid.event_date)
        ON candidate.person_id = lipid.person_id
        AND candidate.lipid_kind = lipid.lipid_kind
),

dyslipidaemia AS (
    -- NICE's HDL definition is incomplete. QOF v51.3 DYSLIP_FLG supplies the
    -- sex-specific HDL limits, triglyceride limit and treatment/result windows.
    SELECT
        person_id,
        reporting_date
    FROM lipid_treatment_selected
    WHERE event_date BETWEEN DATEADD(month, -6, reporting_date) AND reporting_date

    UNION

    SELECT
        person_id,
        reporting_date
    FROM lipid_selected
    WHERE event_date BETWEEN DATEADD(month, -12, reporting_date) AND reporting_date
        AND is_valid
        AND (
            (lipid_kind = 'LDL' AND lipid_value >= 4.1)
            OR (lipid_kind = 'TRIGLYCERIDES' AND lipid_value >= 1.7)
            OR (lipid_kind = 'HDL' AND gender = 'Male' AND lipid_value < 1.0)
            OR (lipid_kind = 'HDL' AND gender = 'Female' AND lipid_value < 1.3)
        )
),

indicator_population AS (
    SELECT
        adult.person_id,
        adult.reporting_date,
        adult.age,
        adult.practice_code,
        adult.practice_name
    FROM adults AS adult
    LEFT JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON adult.person_id = profile.person_id
        AND adult.reporting_date = profile.reporting_date
    LEFT JOIN dyslipidaemia
        ON adult.person_id = dyslipidaemia.person_id
        AND adult.reporting_date = dyslipidaemia.reporting_date
    WHERE profile.has_chd OR profile.has_stroke_tia OR profile.has_diabetes OR profile.has_ndh
        OR profile.has_hypertension OR profile.has_pad OR profile.has_heart_failure OR profile.has_copd
        OR dyslipidaemia.person_id IS NOT NULL OR profile.has_learning_disability
        OR profile.has_obstructive_sleep_apnoea OR profile.has_smi
),

eligible_people AS (
    SELECT DISTINCT person_id
    FROM indicator_population
),

bmi_events AS (
    -- Recorded BMI has no current-age restriction. BMI30_COD alone is not a measurement.
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        bmi_value BETWEEN 10 AND 150 AS is_valid_bmi
    FROM {{ ref('int_bmi_qof_all') }}
    WHERE source_cluster_id = 'BMIVAL_COD'

    UNION ALL

    -- Retain the existing adult height/weight calculation and its weight date.
    SELECT
        person_id,
        clinical_effective_date::DATE AS event_date,
        is_valid_bmi
    FROM {{ ref('int_bmi_all') }}
    WHERE bmi_source = 'calculated'
),

bmi_daily AS (
    SELECT
        bmi.person_id,
        bmi.event_date,
        BOOLOR_AGG(bmi.is_valid_bmi) AS has_valid_bmi
    FROM bmi_events AS bmi
    INNER JOIN eligible_people AS eligible
        ON bmi.person_id = eligible.person_id
    GROUP BY bmi.person_id, bmi.event_date
),

valid_bmi_daily AS (
    SELECT
        person_id,
        event_date
    FROM bmi_daily
    WHERE has_valid_bmi
),

latest_bmi AS (
    SELECT
        population.person_id,
        population.reporting_date,
        bmi.event_date AS latest_bmi_in_period_date
    FROM indicator_population AS population
    ASOF JOIN bmi_daily AS bmi
        MATCH_CONDITION (population.reporting_date >= bmi.event_date)
        ON population.person_id = bmi.person_id
),

latest_valid_bmi AS (
    -- Any valid BMI qualifies even after a later invalid result.
    SELECT
        population.person_id,
        population.reporting_date,
        bmi.event_date AS latest_bmi_date
    FROM indicator_population AS population
    ASOF JOIN valid_bmi_daily AS bmi
        MATCH_CONDITION (population.reporting_date >= bmi.event_date)
        ON population.person_id = bmi.person_id
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        valid.latest_bmi_date,
        CASE WHEN bmi.latest_bmi_in_period_date >= DATEADD(month, -12, population.reporting_date)
            THEN bmi.latest_bmi_in_period_date END AS latest_bmi_in_period_date,
        CASE WHEN valid.latest_bmi_date >= DATEADD(month, -12, population.reporting_date)
            THEN valid.latest_bmi_date END AS latest_record_date,
        COALESCE(valid.latest_bmi_date >= DATEADD(month, -12, population.reporting_date), FALSE) AS is_in_numerator
    FROM indicator_population AS population
    LEFT JOIN latest_bmi AS bmi
        ON population.person_id = bmi.person_id
        AND population.reporting_date = bmi.reporting_date
    LEFT JOIN latest_valid_bmi AS valid
        ON population.person_id = valid.person_id
        AND population.reporting_date = valid.reporting_date
)

SELECT
    person_id,
    'IND320' AS indicator_id,
    'Weight management: BMI recording (long term conditions)' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Long-term condition on the NICE BMI recording list' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    latest_bmi_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        WHEN latest_bmi_in_period_date IS NOT NULL THEN 'NOT_ASSESSABLE'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
