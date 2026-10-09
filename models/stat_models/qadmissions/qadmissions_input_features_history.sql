{{
    config(
        materialized='table',
        cluster_by=['end_date', 'person_id'],
        tags=['qadmissions', 'risk_scores', 'monthly-full'],
        meta={
            'custom_message': 'QAdmissions feature set for Snowflake model registry. Derived from the QAdmissions 2013 v4.1 reference implementation (AGPL-3.0, ClinRisk Ltd). Displaying any score produced from these features requires the ClinRisk disclaimer.'
        }
    )
}}

/*
    qadmissions_input_features_history
    ----------------------------------
    Rebuilds the QAdmissions input features as they stood at historical index
    dates, so the model registered in Snowflake's model registry can be
    validated against later emergency admissions. The historical counterpart
    of qadmissions_input_features. This model does not compute the risk score
    itself.

    Grain
      One row per person per index date. Index dates are every month-end
      from January 2023 to December 2024, from
      qadmissions_history_index_dates(). Each needs a full outcome horizon of
      complete SUS follow-up before it is used for validation.

    Population at each index date
      Registered and alive at the month-end, recorded as Male or Female, and
      aged 18-100 at the month-end, from int_segmentation_person_month_spine.
      People who later died or deregistered are kept.

    Time basis
      Every feature uses only evidence with a clinical date on or before the
      index date and, where a recording date is held, entered on or before
      it, so retrospectively entered codes are not visible at earlier index
      dates. Disease register flags come from the monthly register histories
      (fct_person_*_register_by_month), built with the QOF register macros
      (macros/qof_registers), the as-at pairs of the live
      fct_person_*_register models behind dim_person_conditions.
      Two features are exceptions and take today's value at every index
      date: town (current LSOA, as address history is not held) and ethrisk
      (latest recorded ethnicity, as the QAdmissions paper did).

    Differences from qadmissions_input_features
      Lab features keep extreme outliers; the live model reads the
      int_*_latest models, which exclude them. c_hb, high_platelet and
      high_lft can therefore differ even at the same date.
      Prior admissions come from int_segmentation_acute_activity_history,
      built from the SUS APC encounter model because the live mart's spell
      source holds only two years. It does not impute missing admission
      dates as the mart does, which affects under 0.1% of spells. SUS has no
      recording date, so admissions submitted after an index date are still
      counted at it.

    AGPL / ClinRisk attribution
      QAdmissions is published by ClinRisk Ltd under the GNU Affero General
      Public License v3. The ClinRisk additional terms require that the
      following disclaimer be displayed alongside any score produced from a
      derivative implementation:

      "The initial version of this file, to be found at http://qadmissions.org,
       faithfully implements QAdmissions. We have released this code under the
       GNU Affero General Public License to enable others to implement the
       algorithm faithfully. However, the nature of the GNU Affero General
       Public License is such that we cannot prevent, for example, someone
       accidentally altering the coefficients, getting the inputs wrong, or
       just poor programming. We stress, therefore, that it is the
       responsibility of the end user to check that the source that they
       receive produces the same results as the original code posted at
       http://qadmissions.org. Inaccurate implementations of risk scores can
       lead to wrong patients being given the wrong treatment."
*/

{#- Condition codes of the registers behind the dim_person_conditions flags
    used by the live model. Each monthly register history is looked up by code
    in ltc_register_history_models(). Diabetes is read separately for its
    type. CVD is not used: the QOF CVD register is CHD + stroke/TIA only, and
    b_cvd also includes PAD. -#}
{%- set qadmissions_register_codes = ['AF', 'HF', 'CAN', 'AST', 'COPD', 'EP', 'CKD', 'SMI', 'CHD', 'STIA', 'PAD'] -%}
{%- set qadmissions_registers = [] -%}
{%- for register_code, register_model in ltc_register_history_models() if register_code in qadmissions_register_codes -%}
    {%- do qadmissions_registers.append((register_code, register_model)) -%}
{%- endfor -%}
{%- if qadmissions_registers | length != qadmissions_register_codes | length -%}
    {{ exceptions.raise_compiler_error("qadmissions_input_features_history: not every QAdmissions register code was found in ltc_register_history_models()") }}
{%- endif %}

-- Index month-ends (January 2023 to December 2024), from
-- macros/config/qadmissions_index_dates.sql, which the index-dates test also
-- reads.
WITH index_dates AS (
    SELECT reference_date AS end_date
    FROM (
        {{ qadmissions_history_index_dates() }}
    )
),

-- Eligible population at each index date: registered and alive at the
-- month-end, recorded as Male or Female, aged 18-100 at the month-end. Taken
-- from the person-month spine rather than dim_person_demographics so that
-- people who later died or deregistered are kept. Index dates must be inside
-- the spine's rolling 60-month window, or they match no rows.
base_spine AS (
    SELECT
        s.month_end_date AS end_date,
        s.person_id,
        s.sk_patient_id,
        s.age,
        s.gender
    FROM {{ ref('int_segmentation_person_month_spine') }} AS s
    INNER JOIN index_dates AS i
        ON s.month_end_date = i.end_date
    WHERE s.is_active
      AND s.gender IN ('Male', 'Female')
      AND s.age BETWEEN 18 AND 100
),

-- Register membership at each index date: one row per person, index date and
-- register they are on. The monthly register histories hold only members, for
-- people registered and alive at the month-end; their QOF register macros
-- apply both clinical_effective_date and date_recorded cut-offs and derive age
-- at the month-end. Each history covers 60 months, so only the index months
-- are read.
qof_register_history AS (
{%- for register_code, register_model in qadmissions_registers %}
    {% if not loop.first %}UNION ALL{% endif %}
    SELECT
        reg.month_end_date AS end_date,
        reg.person_id,
        '{{ register_code }}' AS register_code
    FROM {{ ref(register_model) }} AS reg
    INNER JOIN index_dates AS i
        ON reg.month_end_date = i.end_date
{%- endfor %}
),

-- As-at equivalent of the dim_person_conditions columns read by the live
-- model: one row per person and index date on at least one register.
conditions AS (
    SELECT
        end_date,
        person_id,
        BOOLOR_AGG(register_code = 'AF')   AS has_atrial_fibrillation,
        BOOLOR_AGG(register_code = 'HF')   AS has_heart_failure,
        BOOLOR_AGG(register_code = 'CAN')  AS has_cancer,
        BOOLOR_AGG(register_code = 'AST')  AS has_asthma,
        BOOLOR_AGG(register_code = 'COPD') AS has_copd,
        BOOLOR_AGG(register_code = 'EP')   AS has_epilepsy,
        BOOLOR_AGG(register_code = 'CKD')  AS has_chronic_kidney_disease,
        BOOLOR_AGG(register_code = 'SMI')  AS has_severe_mental_illness,
        BOOLOR_AGG(register_code = 'CHD')  AS has_coronary_heart_disease,
        BOOLOR_AGG(register_code = 'STIA') AS has_stroke_tia,
        BOOLOR_AGG(register_code = 'PAD')  AS has_peripheral_arterial_disease
    FROM qof_register_history
    GROUP BY end_date, person_id
),

-- Diabetes register membership and type at each index date, from the monthly
-- history of fct_person_diabetes_register. One row per person on the register.
diabetes AS (
    SELECT
        reg.month_end_date AS end_date,
        reg.person_id,
        reg.diabetes_type
    FROM {{ ref('fct_person_diabetes_register_by_month') }} AS reg
    INNER JOIN index_dates AS i
        ON reg.month_end_date = i.end_date
),

-- Medication orders across the five QAdmissions classes. Each row is one
-- order; the med_class literal identifies the class. The 6-month window is
-- applied per index date in medication_flags. The medication models carry no
-- date_recorded, so orders entered after an index date cannot be excluded.
medication_orders AS (
    SELECT person_id, order_date, 'anticoagulant'  AS med_class
    FROM {{ ref('int_anticoagulant_medications_all') }}
    UNION ALL
    SELECT person_id, order_date, 'antidepressant' AS med_class
    FROM {{ ref('int_antidepressant_medications_all') }}
    UNION ALL
    SELECT person_id, order_date, 'antipsychotic'  AS med_class
    FROM {{ ref('int_antipsychotic_medications_all') }}
    UNION ALL
    SELECT person_id, order_date, 'corticosteroid' AS med_class
    FROM {{ ref('int_systemic_corticosteroid_medications_all') }}
    UNION ALL
    SELECT person_id, order_date, 'nsaid'          AS med_class
    FROM {{ ref('int_nsaid_medications_all') }}
),

-- One or more orders in the 180 days up to and including each index date:
-- the live is_recent_6m rule, measured back from the index date instead of
-- today. One row per person and index date with any qualifying order.
medication_flags AS (
    SELECT
        i.end_date,
        m.person_id,
        COUNT(CASE WHEN m.med_class = 'anticoagulant'  THEN 1 END) >= 1 AS b_anticoagulant,
        COUNT(CASE WHEN m.med_class = 'antidepressant' THEN 1 END) >= 1 AS b_antidepressant,
        COUNT(CASE WHEN m.med_class = 'antipsychotic'  THEN 1 END) >= 1 AS b_antipsychotic,
        COUNT(CASE WHEN m.med_class = 'corticosteroid' THEN 1 END) >= 1 AS b_corticosteroids,
        COUNT(CASE WHEN m.med_class = 'nsaid'          THEN 1 END) >= 1 AS b_nsaid
    FROM index_dates AS i
    INNER JOIN medication_orders AS m
        ON m.order_date <= i.end_date
        AND m.order_date >= DATEADD(day, -180, i.end_date)
    GROUP BY i.end_date, m.person_id
),

-- Non-elective admissions in the 12 months up to each index date, capped at 3
-- to give the QAdmissions hes_admitprior_cat bucket (0 / 1 / 2 / 3+). The
-- as-at pair of fct_person_sus_apc_recent.apc_nel_12mo, read by the live
-- model: admission methods starting 2 except 2C, with the mart's spell
-- deduplication. One row per person and index date with any activity.
emergency_admissions AS (
    SELECT
        a.end_date,
        a.person_id,
        LEAST(a.nel_admissions_12mo, 3) AS hes_admitprior_cat
    FROM {{ ref('int_segmentation_acute_activity_history') }} AS a
    INNER JOIN index_dates AS i
        ON a.end_date = i.end_date
),

-- Lab thresholds from the qadmissions_lab_thresholds seed, pivoted to one row.
-- The live model reads the ALT, GGT and bilirubin thresholds through
-- int_lft_latest; all five come from the same seed.
lab_thresholds AS (
    SELECT
        MAX(CASE WHEN measurement = 'haemoglobin' AND direction = 'low'  THEN threshold END) AS hb_threshold,
        MAX(CASE WHEN measurement = 'platelets'   AND direction = 'high' THEN threshold END) AS platelet_threshold,
        MAX(CASE WHEN measurement = 'alt'         AND direction = 'high' THEN threshold END) AS alt_uln,
        MAX(CASE WHEN measurement = 'ggt'         AND direction = 'high' THEN threshold END) AS ggt_uln,
        MAX(CASE WHEN measurement = 'bilirubin'   AND direction = 'high' THEN threshold END) AS bilirubin_uln
    FROM {{ ref('qadmissions_lab_thresholds') }}
),

-- Valid results for the five QAdmissions lab measurements, narrowed to the
-- columns needed before the index-date join. Valid means a standardised value
-- that is not negative. Unlike the live int_*_latest models, extreme outliers
-- are kept.
lab_results AS (
    SELECT 'haemoglobin' AS measurement, id, person_id, clinical_effective_date, date_recorded, inferred_value
    FROM {{ ref('int_haemoglobin_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'platelets'   AS measurement, id, person_id, clinical_effective_date, date_recorded, inferred_value
    FROM {{ ref('int_platelets_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'alt'         AS measurement, id, person_id, clinical_effective_date, date_recorded, inferred_value
    FROM {{ ref('int_alt_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'ggt'         AS measurement, id, person_id, clinical_effective_date, date_recorded, inferred_value
    FROM {{ ref('int_ggt_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'bilirubin'   AS measurement, id, person_id, clinical_effective_date, date_recorded, inferred_value
    FROM {{ ref('int_bilirubin_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
),

-- Latest valid result of each measurement per person at each index date,
-- restricted to results with a clinical date on or before the index date and
-- entered on or before it. Ordered as in the live int_*_latest models. One
-- window covers all five measurements.
lab_latest AS (
    SELECT
        i.end_date,
        l.person_id,
        l.measurement,
        l.inferred_value
    FROM index_dates AS i
    INNER JOIN lab_results AS l
        ON l.clinical_effective_date <= i.end_date
        AND (l.date_recorded IS NULL OR CAST(l.date_recorded AS DATE) <= i.end_date)
-- get the most recent measurement within an end_date period
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY i.end_date, l.person_id, l.measurement
        ORDER BY l.clinical_effective_date DESC, l.id DESC
    ) = 1
),

-- Lab flags per person and index date with any valid result. high_lft is TRUE
-- when the latest ALT, GGT or bilirubin is above its threshold, as in
-- int_lft_latest.
lab_flags AS (
    SELECT
        ll.end_date,
        ll.person_id,
        BOOLOR_AGG(ll.measurement = 'haemoglobin' AND ll.inferred_value < t.hb_threshold)       AS c_hb,
        BOOLOR_AGG(ll.measurement = 'platelets'   AND ll.inferred_value > t.platelet_threshold) AS high_platelet,
        BOOLOR_AGG(
            (ll.measurement = 'alt'          AND ll.inferred_value > t.alt_uln)
            OR (ll.measurement = 'ggt'       AND ll.inferred_value > t.ggt_uln)
            OR (ll.measurement = 'bilirubin' AND ll.inferred_value > t.bilirubin_uln)
        )                                                                                       AS high_lft
    FROM lab_latest AS ll
    CROSS JOIN lab_thresholds AS t
    GROUP BY ll.end_date, ll.person_id
),

-- Latest valid BMI per person at each index date. For calculated BMI the recording date is the
-- weight's; the height's is not held, so a height entered after the index
-- date but dated before it can still contribute.
bmi_latest AS (
    SELECT
        i.end_date,
        b.person_id,
        b.bmi_value
    FROM index_dates AS i
    INNER JOIN {{ ref('int_bmi_all') }} AS b
        ON b.clinical_effective_date <= i.end_date
        AND (b.date_recorded IS NULL OR CAST(b.date_recorded AS DATE) <= i.end_date)
    WHERE b.is_valid_bmi
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY i.end_date, b.person_id
        ORDER BY b.clinical_effective_date DESC, b.date_recorded DESC NULLS LAST, b.bmi_value DESC, b.id DESC
    ) = 1
),

-- Latest recorded smoking status per person at each index date,
-- smoking_cat defined in final SELECT
smoking_latest AS (
    SELECT
        i.end_date,
        s.person_id,
        s.is_current_smoker,
        s.is_ex_smoker
    FROM index_dates AS i
    INNER JOIN {{ ref('int_smoking_status_all') }} AS s
        ON s.clinical_effective_date <= i.end_date
        AND (s.date_recorded IS NULL OR CAST(s.date_recorded AS DATE) <= i.end_date)
    -- Same selection as int_smoking_status_latest: calendar date, then specific status, then id.
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY i.end_date, s.person_id
        ORDER BY s.clinical_effective_date::DATE DESC,
            CASE s.source_cluster_id
                WHEN 'LSMOK_COD' THEN 1
                WHEN 'EXSMOK_COD' THEN 2
                WHEN 'NSMOK_COD' THEN 3
                ELSE 4
            END,
            s.id DESC
    ) = 1
),

-- Highest QAdmissions alcohol category (0-5) from any valid Full AUDIT score
-- per person at each index date. AUDIT-C scores are not used.
alcohol_highest AS (
    SELECT
        i.end_date,
        a.person_id,
        MAX({{ qadmissions_alcohol_cat6('a.audit_score') }}) AS alcohol_cat6
    FROM index_dates AS i
    INNER JOIN {{ ref('int_alcohol_audit_scores') }} AS a
        ON a.clinical_effective_date <= i.end_date
        AND (a.date_recorded IS NULL OR CAST(a.date_recorded AS DATE) <= i.end_date)
    WHERE a.audit_type = 'Full AUDIT'
      AND a.is_valid_score
    GROUP BY i.end_date, a.person_id
),

-- Person-level flags from the four QAdmissions paper-faithful diagnosis /
-- observation intermediates, with TRUE meaning "ever coded" by the index date.
-- A code counts only when both its clinical date and the date it was entered
-- are on or before the index date. 
falls_flags AS (
    SELECT i.end_date, f.person_id, TRUE AS has_falls
    FROM index_dates AS i
    INNER JOIN {{ ref('int_falls_observations_all') }} AS f
        ON f.clinical_effective_date <= i.end_date
        AND (f.date_recorded IS NULL OR CAST(f.date_recorded AS DATE) <= i.end_date)
    GROUP BY i.end_date, f.person_id
),

malabsorption_flags AS (
    SELECT i.end_date, m.person_id, TRUE AS has_malabsorption
    FROM index_dates AS i
    INNER JOIN {{ ref('int_malabsorption_diagnoses_all') }} AS m
        ON m.clinical_effective_date <= i.end_date
        AND (m.date_recorded IS NULL OR CAST(m.date_recorded AS DATE) <= i.end_date)
    GROUP BY i.end_date, m.person_id
),

vte_flags AS (
    SELECT i.end_date, v.person_id, TRUE AS has_vte
    FROM index_dates AS i
    INNER JOIN {{ ref('int_vte_diagnoses_all') }} AS v
        ON v.clinical_effective_date <= i.end_date
        AND (v.date_recorded IS NULL OR CAST(v.date_recorded AS DATE) <= i.end_date)
    GROUP BY i.end_date, v.person_id
),

liver_pancreatitis_flags AS (
    SELECT i.end_date, lp.person_id, TRUE AS has_liver_pancreatitis
    FROM index_dates AS i
    INNER JOIN {{ ref('int_liver_pancreatitis_diagnoses_all') }} AS lp
        ON lp.clinical_effective_date <= i.end_date
        AND (lp.date_recorded IS NULL OR CAST(lp.date_recorded AS DATE) <= i.end_date)
    GROUP BY i.end_date, lp.person_id
),

-- Townsend score from the person's current 2011 LSOA, the same value at every
-- index date. Address history is not held, so people who have moved since an
-- index date get their current area's score. NULL where there is no 2011 LSOA
-- or it has no Townsend score. One row per person.
townsend AS (
    SELECT
        person_id,
        townsend_score
    FROM {{ ref('int_person_geography') }}
),

-- Ethnicity risk group (1-9) from the person's latest recorded ethnicity, the
-- same value at every index date. The QAdmissions paper also used the latest
-- recorded ethnicity, including records made after baseline. One row per
-- person.
ethrisk_lookup AS (
    SELECT
        person_id,
        ethrisk
    FROM {{ ref('qadmissions_ethrisk') }}
)

-- One row per eligible person per index date. The as-at feature CTEs are
-- keyed on (end_date, person_id) and the current-value CTEs (townsend,
-- ethrisk_lookup) on person_id, so each LEFT JOIN adds at most one row per
-- spine row. People absent from a feature CTE had no qualifying evidence by
-- the index date, so their flags default to FALSE.
SELECT
    -- Identifiers and demographics at the index date.
    base.end_date,
    base.person_id,
    base.sk_patient_id,
    base.age,
    CASE WHEN base.gender = 'Male' THEN 'male' ELSE 'female' END           AS sex,
    bmi.bmi_value                                                          AS bmi,

    -- Constants, as in the live model: Strategic Health Authority 5 (London)
    -- and the prediction horizon in years.
    5                                                                      AS sha1,
    {{ var('qadmissions_horizon_years', 1) }}                              AS surv,

    -- Disease booleans from the as-at QOF registers.
    COALESCE(cond.has_atrial_fibrillation, FALSE)                          AS b_af,
    COALESCE(cond.has_heart_failure, FALSE)                                AS b_ccf,
    COALESCE(cond.has_cancer, FALSE)                                       AS b_anycancer,
    COALESCE(cond.has_asthma, FALSE)
        OR COALESCE(cond.has_copd, FALSE)                                  AS b_asthmacopd,
    COALESCE(cond.has_epilepsy, FALSE)                                     AS b_epilepsy,
    COALESCE(cond.has_chronic_kidney_disease, FALSE)                       AS b_renal,
    COALESCE(cond.has_severe_mental_illness, FALSE)                        AS b_manicschiz,
    COALESCE(cond.has_coronary_heart_disease, FALSE)
        OR COALESCE(cond.has_stroke_tia, FALSE)
        OR COALESCE(cond.has_peripheral_arterial_disease, FALSE)           AS b_cvd,

    -- Diabetes type split. 'Unknown' maps to FALSE for both.
    COALESCE(diab.diabetes_type = 'Type 1', FALSE)                         AS b_type1,
    COALESCE(diab.diabetes_type = 'Type 2', FALSE)                         AS b_type2,

    -- Smoking category from the latest status code at the index date. No
    -- cigarettes-per-day data, so current smokers map to 3, as in the live
    -- model. Never smoked and no record both map to 0.
    CASE
        WHEN smk.is_current_smoker THEN 3
        WHEN smk.is_ex_smoker      THEN 1
        ELSE                            0
    END                                                                    AS smoke_cat,

    -- Medication booleans: one or more orders in the 6 months to the index date.
    COALESCE(med.b_anticoagulant,   FALSE)                                 AS b_anticoagulant,
    COALESCE(med.b_antidepressant,  FALSE)                                 AS b_antidepressant,
    COALESCE(med.b_antipsychotic,   FALSE)                                 AS b_antipsychotic,
    COALESCE(med.b_corticosteroids, FALSE)                                 AS b_corticosteroids,
    COALESCE(med.b_nsaid,           FALSE)                                 AS b_nsaid,

    -- Prior non-elective admissions (0 / 1 / 2 / 3+) in the 12 months to the
    -- index date.
    COALESCE(adm.hes_admitprior_cat, 0)                                    AS hes_admitprior_cat,

    -- Lab flags from the latest valid result at the index date.
    -- Thresholds sourced from qadmissions_lab_thresholds seed.
    COALESCE(lab.c_hb, FALSE)                                              AS c_hb,
    COALESCE(lab.high_lft, FALSE)                                          AS high_lft,
    COALESCE(lab.high_platelet, FALSE)                                     AS high_platelet,

    -- Diagnosis / observation flags from QAdmissions paper-faithful clusters.
    COALESCE(fls.has_falls, FALSE)                                         AS b_falls,
    COALESCE(mal.has_malabsorption, FALSE)                                 AS b_malabsorption,
    COALESCE(vte.has_vte, FALSE)                                           AS b_vte,
    COALESCE(lp.has_liver_pancreatitis, FALSE)                             AS b_liverpancreas,

    -- Townsend score from the current LSOA; NULL passed through, as live.
    twn.townsend_score                                                     AS town,

    -- Alcohol category. 0 (NonDrinker) when there is no Full AUDIT score by
    -- the index date, as in the live model.
    COALESCE(alc.alcohol_cat6, 0)                                          AS alcohol_cat6,

    -- Ethnicity risk group 1-9 from the latest recorded ethnicity. No match
    -- defaults to 1 (White / NotRecorded), as in the live model.
    COALESCE(eth.ethrisk, 1)                                               AS ethrisk

FROM base_spine                       base
LEFT JOIN bmi_latest                  bmi
    ON base.person_id = bmi.person_id  AND base.end_date = bmi.end_date
LEFT JOIN conditions                  cond
    ON base.person_id = cond.person_id AND base.end_date = cond.end_date
LEFT JOIN diabetes                    diab
    ON base.person_id = diab.person_id AND base.end_date = diab.end_date
LEFT JOIN smoking_latest              smk
    ON base.person_id = smk.person_id  AND base.end_date = smk.end_date
LEFT JOIN medication_flags           med
    ON base.person_id = med.person_id  AND base.end_date = med.end_date
LEFT JOIN emergency_admissions        adm
    ON base.person_id = adm.person_id  AND base.end_date = adm.end_date
LEFT JOIN lab_flags                   lab
    ON base.person_id = lab.person_id  AND base.end_date = lab.end_date
LEFT JOIN falls_flags                fls
    ON base.person_id = fls.person_id  AND base.end_date = fls.end_date
LEFT JOIN malabsorption_flags         mal
    ON base.person_id = mal.person_id  AND base.end_date = mal.end_date
LEFT JOIN vte_flags                   vte
    ON base.person_id = vte.person_id  AND base.end_date = vte.end_date
LEFT JOIN liver_pancreatitis_flags    lp
    ON base.person_id = lp.person_id   AND base.end_date = lp.end_date
LEFT JOIN alcohol_highest             alc
    ON base.person_id = alc.person_id  AND base.end_date = alc.end_date
LEFT JOIN townsend                    twn
    ON base.person_id = twn.person_id
LEFT JOIN ethrisk_lookup              eth
    ON base.person_id = eth.person_id
