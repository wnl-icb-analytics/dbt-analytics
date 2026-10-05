-- QAdmissions features: history model at its latest index date vs the live model.
--
-- Sanity check for qadmissions_input_features_history. Run at the latest
-- month-end, the history logic should reproduce qadmissions_input_features
-- apart from the known, explained differences listed under EXPECTED below.
-- One query; every row is an aggregate count.
--
-- Usage: compile with dbt, then run the compiled query in Snowflake.
--
-- SET UP FIRST
--   1. Make sure the latest completed month-end (the spine's newest month,
--      LAST_DAY of last month) is in qadmissions_history_index_dates()
--      (macros/config/qadmissions_index_dates.sql). The check uses
--      MAX(end_date), so the older dates can stay.
--   2. Build both models from the same DEV inputs in ONE run, so source
--      refreshes between builds don't show up as differences:
--        dbt build -s qadmissions_input_features qadmissions_input_features_history
--      If int_date_spine, int_segmentation_person_month_spine or
--      int_segmentation_acute_activity_history were last built before the
--      month-end, rebuild them in the same run, or the history side will be
--      empty. The spine takes its month-ends from int_date_spine. If the history build fails on
--      DATE_RECORDED, another build has replaced an input without that
--      column; rebuild the lab, *_COD_Q, smoking and alcohol _all inputs
--      it reads in the same run.
--   3. Check that check_date is the month-end you added and that the
--      population row has millions of people in both models.
--
-- COLUMNS
--   section          population, boolean, categorical or numeric
--   feature, value   the feature, and for categoricals one of its values
--   live_n           live model: people (population), people with the flag
--                    TRUE, with this value, or with a non-NULL value
--   history_n        the same for the history model at check_date
--   both_n           people counted on both sides: in both models, TRUE in
--                    both, this value in both, or the same non-NULL value
--   live_only_n      population: people in live only. Otherwise people in
--                    both models counted on the live side only (TRUE only in
--                    live, this value only in live, non-NULL only in live)
--   history_only_n   the mirror of live_only_n
--   changed_n        numeric only: non-NULL in both models but different
--   pct_disagreeing  (live_only_n + history_only_n + changed_n) as a % of
--                    everyone counted on either side
--   mean_abs_diff,   numeric only: mean and largest absolute difference
--   max_abs_diff     where changed
--   live_n and history_n cover each model's own population; the other
--   counts cover people in both, so a flag can differ in count because of
--   the population alone.
--
-- EXPECTED (reasons the two can legitimately differ)
--   all       The live model is built after the month-end, so evidence
--             dated or entered between the month-end and the build date
--             counts in live only.
--   population Live-only people mostly joined or turned 18 after the
--             month-end; history-only people died, left or changed gender
--             record since.
--   registers Live registers keep future-dated records; the macros don't.
--             Expect a few more people in live only.
--   *_COD_Q   Live counts every code ever held, including ones entered
--             after the month-end; history filters on date_recorded.
--   labs      History keeps extreme outliers; live drops them. c_hb,
--             high_platelet and high_lft can differ in both directions.
--   meds      Live's 180-day window (is_recent_6m) is fixed when each
--             medication _all table is built; history's ends on the
--             month-end. Stale _all tables give live-only differences.
--   hes       Different sources: fct_person_sus_apc_recent (spells, joined
--             on sk_patient_id) vs int_segmentation_acute_activity_history
--             (encounters, no missing-date imputation).
--   smoke     Same-day ties: history breaks them with id DESC, live is
--             arbitrary.
--   town,     Both read the same current-value model, so expect 0
--   ethrisk   disagreements. Any difference here is a bug.
--   age       Live is age today, history is age at the month-end. Only
--             birthdays in between differ.

{%- set boolean_features = [
    'b_af', 'b_ccf', 'b_anycancer', 'b_asthmacopd', 'b_epilepsy', 'b_renal',
    'b_manicschiz', 'b_cvd', 'b_type1', 'b_type2',
    'b_anticoagulant', 'b_antidepressant', 'b_antipsychotic',
    'b_corticosteroids', 'b_nsaid',
    'c_hb', 'high_lft', 'high_platelet',
    'b_falls', 'b_malabsorption', 'b_vte', 'b_liverpancreas'
] -%}
{%- set categorical_features = ['sex', 'smoke_cat', 'hes_admitprior_cat', 'alcohol_cat6', 'ethrisk'] -%}
{%- set numeric_features = ['age', 'bmi', 'town'] %}

WITH check_date AS (
    SELECT MAX(end_date) AS check_date
    FROM {{ ref('qadmissions_input_features_history') }}
),

history AS (
    SELECT h.*
    FROM {{ ref('qadmissions_input_features_history') }} AS h
    INNER JOIN check_date AS c
        ON h.end_date = c.check_date
),

-- One row per person in either model, with each feature from both sides.
paired AS (
    SELECT
        l.person_id IS NOT NULL AS in_live,
        h.person_id IS NOT NULL AS in_history,
        {%- for f in boolean_features + numeric_features %}
        l.{{ f }} AS live_{{ f }},
        h.{{ f }} AS history_{{ f }},
        {%- endfor %}
        {%- for f in categorical_features %}
        l.{{ f }}::VARCHAR AS live_{{ f }},
        h.{{ f }}::VARCHAR AS history_{{ f }}{{ ',' if not loop.last }}
        {%- endfor %}
    FROM {{ ref('qadmissions_input_features') }} AS l
    FULL OUTER JOIN history AS h
        ON l.person_id = h.person_id
),

-- Categorical features in long form: one row per person, feature and side,
-- so each value can be counted for both models in one GROUP BY.
categorical_long AS (
{%- for f in categorical_features %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT {{ loop.index }} AS feature_order, '{{ f }}' AS feature, 'live' AS side,
        live_{{ f }} AS value, in_history AS in_other,
        EQUAL_NULL(live_{{ f }}, history_{{ f }}) AS matches
    FROM paired WHERE in_live
    UNION ALL
    SELECT {{ loop.index }} AS feature_order, '{{ f }}' AS feature, 'history' AS side,
        history_{{ f }} AS value, in_live AS in_other,
        EQUAL_NULL(live_{{ f }}, history_{{ f }}) AS matches
    FROM paired WHERE in_history
{%- endfor %}
),

results AS (
    SELECT
        1                                           AS section_order,
        'population'                                AS section,
        0                                           AS feature_order,
        'people'                                    AS feature,
        NULL::VARCHAR                               AS value,
        COUNT_IF(in_live)                           AS live_n,
        COUNT_IF(in_history)                        AS history_n,
        COUNT_IF(in_live AND in_history)            AS both_n,
        COUNT_IF(in_live AND NOT in_history)        AS live_only_n,
        COUNT_IF(in_history AND NOT in_live)        AS history_only_n,
        NULL::NUMBER                                AS changed_n,
        NULL::FLOAT                                 AS mean_abs_diff,
        NULL::FLOAT                                 AS max_abs_diff
    FROM paired

    {% for f in boolean_features %}
    UNION ALL
    SELECT
        2, 'boolean', {{ loop.index }}, '{{ f }}', NULL,
        COUNT_IF(in_live AND live_{{ f }}),
        COUNT_IF(in_history AND history_{{ f }}),
        COUNT_IF(in_live AND in_history AND live_{{ f }} AND history_{{ f }}),
        COUNT_IF(in_live AND in_history AND live_{{ f }} AND NOT history_{{ f }}),
        COUNT_IF(in_live AND in_history AND history_{{ f }} AND NOT live_{{ f }}),
        NULL, NULL, NULL
    FROM paired
    {% endfor %}

    UNION ALL
    SELECT
        3, 'categorical', feature_order, feature, COALESCE(value, '(null)'),
        COUNT_IF(side = 'live'),
        COUNT_IF(side = 'history'),
        COUNT_IF(side = 'live' AND in_other AND matches),
        COUNT_IF(side = 'live' AND in_other AND NOT matches),
        COUNT_IF(side = 'history' AND in_other AND NOT matches),
        NULL, NULL, NULL
    FROM categorical_long
    GROUP BY feature_order, feature, value

    {% for f in numeric_features %}
    UNION ALL
    SELECT
        4, 'numeric', {{ loop.index }}, '{{ f }}', NULL,
        COUNT_IF(in_live AND live_{{ f }} IS NOT NULL),
        COUNT_IF(in_history AND history_{{ f }} IS NOT NULL),
        COUNT_IF(in_live AND in_history AND live_{{ f }} = history_{{ f }}),
        COUNT_IF(in_live AND in_history AND live_{{ f }} IS NOT NULL AND history_{{ f }} IS NULL),
        COUNT_IF(in_live AND in_history AND history_{{ f }} IS NOT NULL AND live_{{ f }} IS NULL),
        COUNT_IF(in_live AND in_history AND live_{{ f }} <> history_{{ f }}),
        AVG(CASE WHEN in_live AND in_history AND live_{{ f }} <> history_{{ f }}
            THEN ABS(live_{{ f }} - history_{{ f }}) END),
        MAX(CASE WHEN in_live AND in_history
            THEN ABS(live_{{ f }} - history_{{ f }}) END)
    FROM paired
    {% endfor %}
)

SELECT
    c.check_date,
    r.section,
    r.feature,
    r.value,
    r.live_n,
    r.history_n,
    r.both_n,
    r.live_only_n,
    r.history_only_n,
    r.changed_n,
    ROUND(100 * (r.live_only_n + r.history_only_n + COALESCE(r.changed_n, 0))
        / NULLIF(r.both_n + r.live_only_n + r.history_only_n + COALESCE(r.changed_n, 0), 0), 2)
                                                    AS pct_disagreeing,
    ROUND(r.mean_abs_diff, 2)                       AS mean_abs_diff,
    ROUND(r.max_abs_diff, 2)                        AS max_abs_diff
FROM results AS r
CROSS JOIN check_date AS c
ORDER BY r.section_order, r.feature_order, r.value
