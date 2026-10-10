{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        full_refresh=false,
        on_schema_change='append_new_columns',
        cluster_by=['score_month'],
        tags=['qadmissions', 'risk_scores', 'monthly-full'],
        meta={
            'custom_message': 'QAdmissions scores computed by calling the model registered in the Snowflake Model Registry. Displaying any score produced from QAdmissions requires the ClinRisk disclaimer (see qadmissions_input_features). Includes OLIDS data - for secondary use, INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED ON person_id to apply National Data Opt-Out and Type 1 opt-out filtering.'
        }
    )
}}

/*
    qadmissions_predictions_history
    -------------------------------
    Append-only monthly history of qadmissions_predictions: one set of rows
    per calendar month scored. Same mechanism as
    cltcs_population_monthly_capture.

    - First build: creates the table with the current month's scores.
    - Further builds in the same month: the guard below adds nothing, so a
      rerun cannot duplicate a month. To rescore a month (for example after
      an upstream fix), delete that month's rows first.
    - full_refresh=false: the monthly build on the 1st runs with
      --full-refresh, which would otherwise rebuild this table from scratch
      and lose the history.

    dbt build runs the failure-guard test on qadmissions_predictions first;
    if it fails, this model is skipped and the previous months stand.

    Do not rename or remove this model without first copying its table. The
    history exists only in this table: under a new name dbt starts an empty
    one, and the old table is no longer managed by dbt.
*/

SELECT *
FROM {{ ref('qadmissions_predictions') }}
{% if is_incremental() %}
-- Skip a month that is already captured, so reruns within the month are a no-op.
WHERE score_month NOT IN (SELECT DISTINCT score_month FROM {{ this }})
{% endif %}
