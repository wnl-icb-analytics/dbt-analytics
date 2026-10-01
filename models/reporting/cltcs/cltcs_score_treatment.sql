{{ config(materialized='table', tags=['cltcs']) }}

-- Domain weights for the treatment score (each default 1 = equal weighting).
{% set weight_biomarker_gaps = 1 %}
{% set weight_care_gaps = 1 %}
{% set weight_complexity = 1 %}
{% set weight_medication = 1 %}
{% set weight_illness_uec = 1 %}
{% set weight_barriers = 1 %}
{% set weight_total = weight_biomarker_gaps + weight_care_gaps + weight_complexity + weight_medication + weight_illness_uec + weight_barriers %}

with inclusion_list as (
    select *
    from {{ ref('cltcs_adult_population') }}
    ),

encoding_features as (
    select il.sk_patient_id
        , il.neighbourhood_code
        , il.practice_code
        , pd.age
        -- biomarker and monitoring gaps
        ---- blood pressure
        , case when bp.latest_bp_date between dateadd(month, -6, current_date()) and current_date() and bp.is_overall_bp_controlled = false then 1 else 0 end as poor_recent_bp_control_flag
        , case when bp.latest_bp_date < dateadd(month, -12, current_date()) or bp.latest_bp_date is null and hpr.person_id is not null then 1 else 0 end as bp_monitoring_overdue_flag
        ---- hba1c
        , case when hba1c.clinical_effective_date between dateadd(month, -6, current_date()) and current_date() and hba1c.meets_qof_target = false then 1 else 0 end as poor_recent_hba1c_control_flag
        , case when dpr.person_id is not null and dcp.hba1c_completed_in_last_12m = false then 1 else 0 end as hba1c_monitoring_overdue_flag
        , case when hba1c.hba1c_ifcc > 90 then 1 else 0 end as hba1c_dangerous_flag
        -- ckd and egfr (intervals by latest eGFR mL/min/1.73m²: 45-59 yearly, 30-44 6-monthly, <30 ~quarterly)
        , case
            when eg.clinical_effective_date is null then 0
            when eg.egfr_value is null then 0
            when eg.egfr_value >= 45 and eg.egfr_value < 60
                and eg.clinical_effective_date < dateadd(month, -12, current_date()) then 1
            when eg.egfr_value >= 30 and eg.egfr_value < 45
                and eg.clinical_effective_date < dateadd(month, -6, current_date()) then 1
            when eg.egfr_value >= 0 and eg.egfr_value < 30
                and eg.clinical_effective_date < dateadd(month, -3, current_date()) then 1
            else 0
          end as egfr_overdue_flag

        -- actionable risk factors
        , zeroifnull(br.smoking_risk_sort_key) as smoking_risk_sort_key -- 0 unknown, 1 never, 2 ex, 3 current therefore unchanged
        , case
            when zeroifnull(br.bmi_risk_sort_key) >= 5 then 3 -- obese class 2/3
            when zeroifnull(br.bmi_risk_sort_key) = 4 then 2 -- obese class 1
            when zeroifnull(br.bmi_risk_sort_key) < 4 and zeroifnull(br.bmi_risk_sort_key) > 1 then 1 -- overweight
            else 0 end as bmi_flag -- normal
        , case
            when zeroifnull(br.alcohol_risk_sort_key) >= 7 then 3
            when zeroifnull(br.alcohol_risk_sort_key) < 7 and zeroifnull(br.alcohol_risk_sort_key) >=3 then 2
            when zeroifnull(br.alcohol_risk_sort_key) < 3 and zeroifnull(br.alcohol_risk_sort_key) > 1 then 1
            else 0 end as alcohol_flag
        , case when id.illicit_drug_pattern is not null
                  and id.illicit_drug_pattern <> 'Does not misuse drugs' then 3 else 0 end as substance_misuse_flag

        -- medication burden
        , zeroifnull(rm.unique_active_ingredient_count_12mo) as unique_active_ingredient_count_12mo
        , case when polyp.is_polypharmacy_5plus then 1 else 0 end as polypharmacy_5plus_flag
        , case when polyp.is_polypharmacy_10plus then 1 else 0 end as polypharmacy_10plus_flag
        , zeroifnull(polyp.medication_count) as medication_count

        -- multimorbidity and biopsychosocial complexity
        , pc.total_qof_conditions
        , pc.mental_health_conditions
        , zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score
        , {{ encode_ltc_lcs_risk_group('lcs.chd_risk_group') }} as chd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.ckd_risk_group') }} as ckd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.copd_risk_group') }} as copd_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.diabetes_risk_group') }} as diabetes_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.hf_risk_group') }} as hf_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.hypertension_risk_group') }} as hypertension_risk_group_sort_key
        , {{ encode_ltc_lcs_risk_group('lcs.overall_risk_group') }} as overall_risk_group_sort_key

        -- barriers to treatment
        , case when ps.is_housebound then 1 else 0 end as is_housebound_flag
        , case when pc.has_severe_mental_illness then 1 else 0 end as has_smi_flag

        -- acute use related features
        , zeroifnull(aea.ae_ill_12mo) as ae_ill_12mo
        , zeroifnull(aea.ae_t1_12mo) as ae_t1_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(aea.ae_lower_respiratory_attendance_12mo) as ae_lower_respiratory_attendance_12mo
        , zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo
        -- condition specific care gaps
        ---- asthma
        , case when am.testing_no_diagnosis = TRUE then 1 else 0 end as asthma_testing_no_diagnosis_flag
        , case when am.diagnosis_no_testing = TRUE then 1 else 0 end as asthma_diagnosis_no_testing_flag
        , case when am.diagnosis_no_act = TRUE then 1 else 0 end as asthma_diagnosis_no_act_flag
        , case when  am.salbutamol_only= TRUE then 1 else 0 end as asthma_salbutamol_only_flag
        , case when  am.salbutamol_repeats  = TRUE then 1 else 0 end as asthma_salbutamol_repeats_flag
        ---- Diabetes
        , case when pc.has_diabetes then greatest(0, 8 - zeroifnull(dcp.care_processes_completed)) else 0 end as diabetes_care_gap_count
        , case when pc.has_diabetes and coalesce(dt.all_three_targets_met, false) = false then 1 else 0 end as diabetes_not_triple_target_flag
        ---- COPD
        ---- chronic kidney disease
        ---- chronic heart disease
        -- Engagement in existing care pathways
        , lcs.moc_stage_completed
      from inclusion_list il
    left join {{ref('dim_person_conditions')}} pc
        on il.person_id =pc.person_id
    left join {{ref('fct_person_sus_uec_recent')}} aea
        on il.sk_patient_id  =aea.sk_patient_id
    left join {{ ref('fct_person_sus_apc_recent') }} apca
        on il.sk_patient_id =apca.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.sk_patient_id  =gpa.sk_patient_id
    left join {{ref('fct_person_medications_recent')}} rm
        on il.person_id =rm.person_id
    left join {{ref('dim_person_demographics')}} pd
        on il.person_id =pd.person_id
    left join {{ref('fct_person_behavioural_risk_factors')}} br
        on il.person_id =br.person_id
    left join {{ref('int_asthma_management')}} am
        on il.person_id =am.person_id
    left join {{ref('fct_person_bp_control')}} bp
        on il.person_id =bp.person_id
    left join {{ref('dim_person_ccms')}} ccms
        on il.person_id =ccms.person_id
    left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
        on il.person_id =lcs.person_id
    left join {{ref('fct_person_polypharmacy_current')}} polyp
        on il.person_id =polyp.person_id
    left join {{ref('fct_person_diabetes_register')}} dpr
        on il.person_id =dpr.person_id
    left join {{ref('fct_person_hypertension_register')}} hpr
        on il.person_id =hpr.person_id
    left join {{ref('fct_person_diabetes_8_care_processes')}} dcp
        on il.person_id =dcp.person_id
    left join {{ref('fct_person_diabetes_triple_target')}} dt
        on il.person_id =dt.person_id
    left join {{ref('int_hba1c_latest')}} hba1c
        on il.person_id =hba1c.person_id
    left join {{ref('int_smi_lifestyle')}} id
        on il.person_id =id.person_id
    left join {{ ref('int_egfr_latest') }} eg
        on il.person_id =eg.person_id
    left join {{ref('dim_person_status_summary')}} ps
        on il.person_id =ps.person_id
),

domain_sub_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,

        -- biomarker and monitoring gaps
        (
            poor_recent_bp_control_flag * 2
            + poor_recent_hba1c_control_flag *2
            + hba1c_dangerous_flag * 4
            + bp_monitoring_overdue_flag
            + hba1c_monitoring_overdue_flag
            + egfr_overdue_flag

        ) as score_biomarker_gaps,

        -- condition and prevention care gaps (overall LTC LCS risk only — avoid summing per-condition keys, flags * 3 to match scale of BMI/Smoking/alcohol)
        (
            asthma_salbutamol_only_flag * 3
            + asthma_salbutamol_repeats_flag * 3
            + diabetes_care_gap_count * 3
            + diabetes_not_triple_target_flag * 3
            + case
                when zeroifnull(moc_stage_completed) < 2
                    and overall_risk_group_sort_key > 0 then 3
                else 0
              end
            + smoking_risk_sort_key
            + bmi_flag
            + alcohol_flag
        ) as score_care_gaps,

        -- clinical complexity: multimorbidity
        (
            chd_risk_group_sort_key
            + ckd_risk_group_sort_key
            + copd_risk_group_sort_key
            + diabetes_risk_group_sort_key
            + hf_risk_group_sort_key
            + hypertension_risk_group_sort_key
            + least(zeroifnull(total_qof_conditions), 8)
            + zeroifnull(mental_health_conditions)
            + cambridge_comorbidity_score * 5
        ) as score_complexity,

        -- D4: medication burden
        ( ln( 1+ medication_count)
            + ln( 1+ unique_active_ingredient_count_12mo)
            + polypharmacy_10plus_flag * 2
        ) as score_medication,

        -- illness-related UEC (not injury totals; no ae_ill_3mo in feature set)
        (
            ln( 1+ ae_ill_12mo)
            + ln( 1+ ae_t1_12mo)
            + ln(1+acs_nel_12mo)
            - ln( 1+ gp_att_tot_12mo)
        ) as score_illness_uec,
        -- barriers to treatment / and biopsychosocial treatment complexity
        (
            is_housebound_flag
            + has_smi_flag
            + substance_misuse_flag / 3 -- to reduce back to binary flag
            + case when alcohol_flag >= 3 then 1 else 0 end -- binary flag for active alcohol misuse disorder
        ) as score_barriers
        -- to add existring engagement in care pathways
    from encoding_features
),

composite_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        score_biomarker_gaps,
        score_care_gaps,
        score_complexity,
        score_medication,
        score_illness_uec,
        score_barriers,
        (score_biomarker_gaps - avg(score_biomarker_gaps) over (partition by neighbourhood_code)) / nullif(stddev(score_biomarker_gaps) over (partition by neighbourhood_code), 0) as scaled_score_biomarker_gaps,
        (score_care_gaps - avg(score_care_gaps) over (partition by neighbourhood_code)) / nullif(stddev(score_care_gaps) over (partition by neighbourhood_code), 0) as scaled_score_care_gaps,
        (score_complexity - avg(score_complexity) over (partition by neighbourhood_code)) / nullif(stddev(score_complexity) over (partition by neighbourhood_code), 0) as scaled_score_complexity,
        (score_medication - avg(score_medication) over (partition by neighbourhood_code)) / nullif(stddev(score_medication) over (partition by neighbourhood_code), 0) as scaled_score_medication,
        (score_illness_uec - avg(score_illness_uec) over (partition by neighbourhood_code)) / nullif(stddev(score_illness_uec) over (partition by neighbourhood_code), 0) as scaled_score_illness_uec,
        (score_barriers - avg(score_barriers) over (partition by neighbourhood_code)) / nullif(stddev(score_barriers) over (partition by neighbourhood_code), 0) as scaled_score_barriers
    from domain_sub_scores
),


clipped_scores as (
    select
        *,
        least(greatest(zeroifnull(scaled_score_biomarker_gaps), -3), 3) as clipped_score_biomarker_gaps,
        least(greatest(zeroifnull(scaled_score_care_gaps), -3), 3) as clipped_score_care_gaps,
        least(greatest(zeroifnull(scaled_score_complexity), -3), 3) as clipped_score_complexity,
        least(greatest(zeroifnull(scaled_score_medication), -3), 3) as clipped_score_medication,
        least(greatest(zeroifnull(scaled_score_illness_uec), -3), 3) as clipped_score_illness_uec,
        least(greatest(zeroifnull(scaled_score_barriers), -3), 3) as clipped_score_barriers,
        from composite_scores
),
-- rescale each clipped domain z-score to a 0-100 sub-score, then take the weighted
-- average across domains (normalised by the sum of weights) to get raw_score_treatment.
remapped_scores as (
    select
        *,
        round((clipped_score_biomarker_gaps + 3) / 6.0 * 100, 1) as score_biomarker_gaps_0_100,
        round((clipped_score_care_gaps + 3) / 6.0 * 100, 1) as score_care_gaps_0_100,
        round((clipped_score_complexity + 3) / 6.0 * 100, 1) as score_complexity_0_100,
        round((clipped_score_medication + 3) / 6.0 * 100, 1) as score_medication_0_100,
        round((clipped_score_illness_uec + 3) / 6.0 * 100, 1) as score_illness_uec_0_100,
        round((clipped_score_barriers + 3) / 6.0 * 100, 1) as score_barriers_0_100,
        (
            score_biomarker_gaps_0_100 * {{ weight_biomarker_gaps }}
            + score_care_gaps_0_100 * {{ weight_care_gaps }}
            + score_complexity_0_100 * {{ weight_complexity }}
            + score_medication_0_100 * {{ weight_medication }}
            + score_illness_uec_0_100 * {{ weight_illness_uec }}
            + score_barriers_0_100 * {{ weight_barriers }}
        ) / {{ weight_total }} as raw_score_treatment
    from clipped_scores
)

select
    *,
    -- apply the age multiplier to the weighted 0-100 score, then bound to [1, 100].
    least(greatest(
        raw_score_treatment * (
            case
                when age is null then 1.0
                else (
                    1
                    -- max +15% boost at age 18, linear decline to 0 at age 60
                    + 0.15 * (60 - least(greatest(age, 18), 60)) / 42.0
                    -- linear penalty from age 60, capped at -15% from age 100 onward
                    - 0.15 * least(greatest(age - 60, 0), 40) / 40.0
                )
            end
        )
    , 1), 100) as score_treatment
from remapped_scores
