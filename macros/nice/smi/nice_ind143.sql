{% macro nice_ind143(reference='current') %}
{#-
    Calculate NICE IND143 from the paired population and evidence.
    Args: reference is current or by_month.
    Returns: one eligible person per reporting_date, with indicator detail.
-#}
-- NICE IND143: https://www.nice.org.uk/indicators/ind143
-- Mental health care plan recorded in 12 months and on or after the relapse (or first diagnosis) for people with an active SMI diagnosis.
WITH indicator_population AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        profile.earliest_smi_diagnosis_date,
        profile.latest_smi_diagnosis_date,
        profile.latest_smi_remission_date,
        evidence.latest_smi_care_plan_date
    FROM ({{ nice_reference_population(reference) }}) AS population
    INNER JOIN {{ nice_ref('int_nice_ltc_population', reference) }} AS profile
        ON population.person_id = profile.person_id
        AND population.reporting_date = profile.reporting_date
    LEFT JOIN {{ nice_ref('int_nice_review_evidence', reference) }} AS evidence
        ON population.person_id = evidence.person_id
        AND population.reporting_date = evidence.reporting_date
    WHERE profile.has_active_smi_diagnosis
),

assessed AS (
    SELECT
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name,
        -- A plan must postdate the relapse (the latest diagnosis after any remission) or the first diagnosis, as QOF MH002 reads NICE
        CASE
            WHEN population.latest_smi_remission_date IS NOT NULL
                THEN population.latest_smi_diagnosis_date
            ELSE population.earliest_smi_diagnosis_date
        END AS plan_anchor_date,
        CASE
            WHEN population.latest_smi_care_plan_date >= DATEADD(month, -12, population.reporting_date)
                THEN population.latest_smi_care_plan_date
        END AS latest_record_date,
        COALESCE(population.latest_smi_care_plan_date >= DATEADD(month, -12, population.reporting_date)
            AND population.latest_smi_care_plan_date >= CASE
                WHEN population.latest_smi_remission_date IS NOT NULL
                    THEN population.latest_smi_diagnosis_date
                ELSE population.earliest_smi_diagnosis_date
            END, FALSE) AS is_in_numerator
    FROM indicator_population AS population
)

SELECT
    person_id,
    'IND143' AS indicator_id,
    'Bipolar, schizophrenia and other psychoses: care planning' AS indicator_name,
    reporting_date,
    DATEADD(month, -12, reporting_date) AS measurement_period_start,
    age,
    'Active severe mental illness' AS condition_name,
    {{ nice_practice_columns('assessed', reference) }},
    plan_anchor_date,
    latest_record_date,
    TRUE AS is_in_denominator,
    is_in_numerator,
    CASE
        WHEN is_in_numerator THEN 'ACHIEVED'
        ELSE 'NOT_RECORDED_IN_PERIOD'
    END AS indicator_status
FROM assessed
{% endmacro %}
