-- LTC LCS: Person-level risk stratification summary
-- One row per person who appears in any condition-level rs model.
-- One column per condition holding the highest risk group the person is in for that condition.
-- overall_risk_group is the highest risk group across all conditions.
--
-- Risk group rank (lower = higher risk):
--   1 = HRC, 2 = HR, 3 = MR, 4 = MRa/MRb, 5 = LR

with all_rs as (
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_acyp_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_acyp_pg4_lr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_af_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_af_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ast_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ast_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ast_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ast_pg4_lr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_chd_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_chd_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_chd_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ckd_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_ckd_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_copd_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_copd_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_copd_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg3a_mra') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg3b_mrb') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_dm_pg4_lr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_hf_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_hf_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_htn_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_htn_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_htn_pg3a_mra') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_htn_pg3b_mrb') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_htn_pg4_lr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_nafld_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_pad_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_pad_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_pad_pg3_mr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_stroke_pg1_hrc') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_stroke_pg2_hr') }}
    union all
    select person_id, condition, risk_group from {{ ref('int_ltc_lcs_rs_stroke_pg3_mr') }}
),

ranked as (
    select
        person_id,
        condition,
        risk_group,
        case upper(risk_group)
            when 'HRC' then 1
            when 'HR'  then 2
            when 'MR'  then 3
            when 'MRA' then 4
            when 'MRB' then 4
            when 'LR'  then 5
            -- Defensive fallback: any unexpected value treated as LR so the overall
            -- rollup stays non-null. Per-condition accepted_values tests surface the anomaly.
            else 5
        end as risk_rank
    from all_rs
),

highest_per_condition as (
    select
        person_id,
        condition,
        risk_group,
        risk_rank
    from ranked
    qualify row_number() over (
        partition by person_id, condition
        order by risk_rank
    ) = 1
),

pivoted as (
    select
        person_id,
        max(case when condition = 'AF' then risk_group end) as af_risk_group,
        max(case when condition = 'Asthma Adult' then risk_group end) as asthma_adult_risk_group,
        max(case when condition = 'Asthma CYP' then risk_group end) as asthma_cyp_risk_group,
        max(case when condition = 'CHD' then risk_group end) as chd_risk_group,
        max(case when condition = 'CKD' then risk_group end) as ckd_risk_group,
        max(case when condition = 'COPD' then risk_group end) as copd_risk_group,
        max(case when condition = 'Diabetes' then risk_group end) as diabetes_risk_group,
        max(case when condition = 'HF' then risk_group end) as hf_risk_group,
        max(case when condition = 'Hypertension' then risk_group end) as hypertension_risk_group,
        max(case when condition = 'NAFLD' then risk_group end) as nafld_risk_group,
        max(case when condition = 'PAD' then risk_group end) as pad_risk_group,
        max(case when condition = 'Stroke/TIA' then risk_group end) as stroke_tia_risk_group,
        min(risk_rank) as overall_risk_rank
    from highest_per_condition
    group by person_id
)

select
    person_id,
    af_risk_group,
    asthma_adult_risk_group,
    asthma_cyp_risk_group,
    chd_risk_group,
    ckd_risk_group,
    copd_risk_group,
    diabetes_risk_group,
    hf_risk_group,
    hypertension_risk_group,
    nafld_risk_group,
    pad_risk_group,
    stroke_tia_risk_group,
    case overall_risk_rank
        when 1 then 'HRC'
        when 2 then 'HR'
        when 3 then 'MR'
        when 4 then 'MR'
        when 5 then 'LR'
        else 'LR'
    end as overall_risk_group,
    overall_risk_rank
from pivoted
