{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['qadmissions', 'risk_scores', 'monthly-full'],
        meta={
            'custom_message': 'QAdmissions 2-year scores at historical index dates, for validation against qadmissions_outcomes_validation, computed by calling the model registered in the Snowflake Model Registry. Displaying any score produced from QAdmissions requires the ClinRisk disclaimer (see qadmissions_input_features). Includes OLIDS data - for secondary use, INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED ON person_id to apply National Data Opt-Out and Type 1 opt-out filtering.'
        }
    )
}}

/*
    qadmissions_predictions_validation
    ----------------------------------
    QAdmissions risk scores for every row of
    qadmissions_input_features_validation, from the model registered in the
    Snowflake Model Registry, called through the qadmissions_predict macro as
    qadmissions_predictions does.

    Grain
      One row per person and index date, as in
      qadmissions_input_features_validation. Rows the model rejects (for
      example a bmi outside 20-40 or a NULL or out-of-range town) are kept
      with is_scored = FALSE and the model's error text.

    Horizon
      The features carry surv = 2 (qadmissions_history_horizon_years()), so
      score_percent is 2-year risk, matching the follow-up in
      qadmissions_outcomes_validation. Live scores in qadmissions_predictions
      are 1-year risk, so the two are not directly comparable.

    The inputs are not repeated here: join to
    qadmissions_input_features_validation on (end_date, person_id). Both are
    tagged monthly-full and rebuilt in the same run; scored_at shows when
    these scores were made.
*/

WITH scored AS (
    SELECT
        f.end_date,
        f.person_id,
        f.sk_patient_id,
        f.surv,
        {{ qadmissions_predict('f') }} AS prediction
    FROM {{ ref('qadmissions_input_features_validation') }} AS f
)

SELECT
    end_date,
    person_id,
    sk_patient_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ              AS scored_at,
    '{{ qadmissions_model_version() }}'             AS model_version,
    surv,
    prediction:"ok"::BOOLEAN                        AS is_scored,
    prediction:"score_percent"::FLOAT               AS score_percent,
    prediction:"errors"::VARCHAR                    AS scoring_errors
FROM scored
