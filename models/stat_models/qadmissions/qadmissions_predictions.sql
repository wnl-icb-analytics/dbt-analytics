{{
    config(
        materialized='table',
        tags=['qadmissions', 'risk_scores', 'monthly-full'],
        meta={
            'custom_message': 'QAdmissions scores computed by calling the model registered in the Snowflake Model Registry. Displaying any score produced from QAdmissions requires the ClinRisk disclaimer (see qadmissions_input_features). A missing BMI is imputed before scoring (see is_bmi_imputed). Includes OLIDS data - for secondary use, INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED ON person_id to apply National Data Opt-Out and Type 1 opt-out filtering.'
        }
    )
}}

/*
    qadmissions_predictions
    -----------------------
    QAdmissions risk scores for everyone in qadmissions_input_features. It
    calls the model registered in the Snowflake Model Registry from SQL,
    through the qadmissions_predict macro. The model is registered from the
    qadmission_review repository.

    Grain
      One row per person in qadmissions_input_features, scored on the build
      date. Rows the model rejects (for example a bmi outside 20-40, recorded
      or imputed, or a NULL or out-of-range town) are kept with
      is_scored = FALSE and the model's error text. A missing BMI is imputed
      in qadmissions_input_features; is_bmi_imputed marks those rows.

    The inputs are carried alongside the score so each history row records
    exactly what was scored.

    Tagged monthly-full, so the daily and weekly builds skip it. It is built
    by the monthly full refresh on the 1st, and by any deploy that changes it
    or a model upstream of it, so this table can be newer than the latest
    month in qadmissions_predictions_history.

    The not_null_proportion test on score_percent is the failure guard: when
    too few rows are scored, dbt build skips qadmissions_predictions_history
    so a bad run never reaches the history.
*/

WITH scored AS (
    SELECT
        f.person_id,
        f.sk_patient_id,
        {%- for input in qadmissions_model_inputs() %}
        f.{{ input }},
        {%- endfor %}
        f.bmi_recorded,
        f.is_bmi_imputed,
        f.bmi_imputation_group,
        {{ qadmissions_predict('f') }} AS prediction
    FROM {{ ref('qadmissions_input_features') }} f
)

SELECT
    person_id,
    sk_patient_id,
    DATE_TRUNC('month', CURRENT_DATE())::DATE       AS score_month,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS scored_at,
    '{{ qadmissions_model_version() }}'             AS model_version,
    prediction:"ok"::BOOLEAN                        AS is_scored,
    prediction:"score_percent"::FLOAT               AS score_percent,
    prediction:"errors"::VARCHAR                    AS scoring_errors,
    {%- for input in qadmissions_model_inputs() %}
    {{ input }},
    {%- endfor %}
    -- How bmi was filled in. Added after the inputs so the history table
    -- appends them as new columns; earlier months hold NULL.
    bmi_recorded,
    is_bmi_imputed,
    bmi_imputation_group,
    '{{ qadmissions_bmi_imputation_method() }}'     AS bmi_imputation_method
FROM scored
