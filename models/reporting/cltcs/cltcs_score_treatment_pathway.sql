{{ config(materialized='table', tags=['cltcs']) }}

-- Domain weights for the treatment score (each default 1 = equal weighting).
{% set domain_weights = {
    'disease_control': 1,
    'monitoring_gaps': 1,
    'treatment_optimisation': 1,
    'complexity': 1,
    'illness_uec': 1,
    'barriers': 1,
    'ltc_lcs_engagement': 0.5
} %}
{% set weight_total = domain_weights.values() | sum %}
-- Non-negative count domains are log-transformed before scaling to reduce the skew of
-- mostly-zero counts. Complexity is a weighted mix and illness_uec is already a log
-- difference (it can be negative), so both are scaled as they are.
-- Weight on gap depth in the disease control, monitoring and optimisation domains: a group
-- with a gap scores 1 + this weight x the share of its items that are gaps. Keep below 1 so
-- breadth across conditions outranks depth within one.
{% set gap_depth_weight = 0.5 %}
{% set log_domains =['disease_control', 'monitoring_gaps', 'treatment_optimisation', 'barriers', 'ltc_lcs_engagement'] %}
-- Points per composite z unit when mapping to 0-100. 15 covers about +/-3.3 z before the
-- score is capped at 0 or 100.
{% set score_points_per_z = 15 %}
-- Priority tiers by percentile of score_treatment within neighbourhood, cohort and age band.
{% set tier_1_percentile = 0.95 %}
{% set tier_2_percentile = 0.85 %}

with spine as (
    select sk_patient_id
        , person_id
        , neighbourhood_code
        , practice_code
    from {{ ref('cltcs_adult_population') }}
    where meets_original_criteria or meets_adults_with_complexity),

inclusion_list as (
    select sk_patient_id
        , 'original_criteria' as inclusion_rule
        , applicable_cohort = 'original_criteria' as is_applicable_rule
    from {{ ref('cltcs_adult_population') }}
    where meets_original_criteria
    union all
    select sk_patient_id
        , 'adults_with_complexity'
        , applicable_cohort = 'adults_with_complexity'
    from {{ ref('cltcs_adult_population') }}
    where meets_adults_with_complexity
),

-- each group with a gap among its assessable items (cltcs_gap_items) adds 1 for breadth,
-- plus gap_depth_weight x the share of its items that are gaps for depth. With the weight
-- below 1 a group adds less than 2, so gaps across more conditions always outrank more gaps
-- within one condition (multimorbidity dominates), and a condition with many indicators
-- cannot outweigh one with few.
group_gaps as (
    select person_id
        , score_domain
        , score_group
        , max(iff(is_gap, 1, 0)) as has_gap
        , avg(iff(is_gap, 1, 0)) as gap_share
        , max(iff(is_gap, 1, 0)) + {{ gap_depth_weight }} * avg(iff(is_gap, 1, 0)) as group_gap_score
    from {{ ref('cltcs_gap_items') }}
    group by person_id, score_domain, score_group
),

care_pathway_scores as (
    select person_id
        , sum(iff(score_domain = 'disease_control', group_gap_score, 0)) as disease_control_gaps
        , sum(iff(score_domain = 'monitoring_gaps', group_gap_score, 0)) as monitoring_gaps
        , sum(iff(score_domain = 'treatment_optimisation', group_gap_score, 0)) as treatment_optimisation_gaps
        , count_if(score_domain = 'disease_control') as disease_control_groups_assessed
        , count_if(score_domain = 'monitoring_gaps') as monitoring_groups_assessed
        , count_if(score_domain = 'treatment_optimisation') as treatment_optimisation_groups_assessed
    from group_gaps
    group by person_id
),

encoding_features as (
    select il.sk_patient_id
        , il.neighbourhood_code
        , il.practice_code
        , pd.age

        -- care pathway gaps (disease control, monitoring, treatment optimisation)
        , zeroifnull(cps.disease_control_gaps) as disease_control_gaps
        , zeroifnull(cps.monitoring_gaps) as monitoring_gaps
        , zeroifnull(cps.treatment_optimisation_gaps) as treatment_optimisation_gaps
        , zeroifnull(cps.disease_control_groups_assessed) as disease_control_groups_assessed
        , zeroifnull(cps.monitoring_groups_assessed) as monitoring_groups_assessed
        , zeroifnull(cps.treatment_optimisation_groups_assessed) as treatment_optimisation_groups_assessed

        -- risk factor status
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
        , case when polyp.is_polypharmacy_10plus then 1 else 0 end as polypharmacy_10plus_flag
        , zeroifnull(polyp.medication_count) as medication_count

        -- multimorbidity and biopsychosocial complexity
        , pc.total_qof_conditions
        , pc.mental_health_conditions
        , zeroifnull(ccms.cambridge_comorbidity_score) as cambridge_comorbidity_score

        -- barriers to treatment
        , case when ps.is_housebound then 1 else 0 end as is_housebound_flag
        , case when pc.has_severe_mental_illness then 1 else 0 end as has_smi_flag

        -- acute use related features
        , zeroifnull(aea.ae_ill_12mo) as ae_ill_12mo
        , zeroifnull(aea.ae_t1_12mo) as ae_t1_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo

        -- LTC LCS priority (summed across conditions, LR 1 to HRC 4 each) and Model of Care progress
        , {{ encode_ltc_lcs_risk_group('lcs.af_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.asthma_adult_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.chd_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.ckd_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.copd_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.diabetes_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.hf_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.hypertension_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.nafld_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.pad_risk_group') }}
            + {{ encode_ltc_lcs_risk_group('lcs.stroke_tia_risk_group') }} as ltc_lcs_risk_group_sum
        , lcs.moc_pathway_status
      from spine il
    left join care_pathway_scores cps
        on il.person_id = cps.person_id
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
    left join {{ref('dim_person_ccms')}} ccms
        on il.person_id =ccms.person_id
    left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
        on il.person_id =lcs.person_id
    left join {{ref('fct_person_polypharmacy_current')}} polyp
        on il.person_id =polyp.person_id
    left join {{ref('int_smi_lifestyle')}} id
        on il.person_id =id.person_id
    left join {{ref('dim_person_status_summary')}} ps
        on il.person_id =ps.person_id
),

domain_sub_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        disease_control_groups_assessed,
        monitoring_groups_assessed,
        treatment_optimisation_groups_assessed,

        -- disease control: results out of target and uncontrolled respiratory disease
        disease_control_gaps as score_disease_control,

        -- monitoring gaps: tests, reviews and care processes not done (incl. missing control results)
        monitoring_gaps as score_monitoring_gaps,

        -- treatment optimisation: under-treatment (indicated treatment or lifestyle
        -- intervention not given) and over-treatment or prescribing safety (no structured
        -- medication review, NSAID with low eGFR)
        treatment_optimisation_gaps as score_treatment_optimisation,

        -- clinical complexity: multimorbidity, medication burden and risk factor status.
        -- Alcohol dependence (alcohol_flag 3) is scored in barriers only.
        (
            least(zeroifnull(total_qof_conditions), 8)
            + zeroifnull(mental_health_conditions)
            + cambridge_comorbidity_score * 5
            + ln( 1+ medication_count)
            + ln( 1+ unique_active_ingredient_count_12mo)
            + polypharmacy_10plus_flag * 2
            + smoking_risk_sort_key
            + bmi_flag
            + iff(alcohol_flag < 3, alcohol_flag, 0)
        ) as score_complexity,

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
        ) as score_barriers,

        -- LTC LCS priority not yet acted on: risk groups summed across conditions, in full
        -- when the Model of Care has not started or was declined, halved while in progress
        -- and zero once the cycle is complete
        (
            ltc_lcs_risk_group_sum * case
                when moc_pathway_status = 'Cycle complete' then 0
                when moc_pathway_status = 'In progress' then 0.5
                else 1
            end
        ) as score_ltc_lcs_engagement
    from encoding_features
),

cohort_sub_scores as(
    select dss.*
    , il.inclusion_rule
    , il.is_applicable_rule
    from inclusion_list il
    inner join domain_sub_scores dss on il.sk_patient_id = dss.sk_patient_id
),

transformed_scores as (
    select *
        {%- for domain in domain_weights %}
        , {% if domain in log_domains %}ln(1 + score_{{ domain }}){% else %}score_{{ domain }}{% endif %} as transformed_score_{{ domain }}
        {%- endfor %}
    from cohort_sub_scores
),

-- centre each domain on its neighbourhood and cohort mean
centred_scores as (
    select *
        {%- for domain in domain_weights %}
        , transformed_score_{{ domain }}
            - avg(transformed_score_{{ domain }}) over (partition by neighbourhood_code, inclusion_rule) as centred_score_{{ domain }}
        {%- endfor %}
    from transformed_scores
),

-- divide by the pooled within-neighbourhood standard deviation for the cohort, so a small
-- neighbourhood does not get its own unstable scale. Null when the domain does not vary.
scaled_scores as (
    select *
        {%- for domain in domain_weights %}
        , centred_score_{{ domain }}
            / nullif(stddev(centred_score_{{ domain }}) over (partition by inclusion_rule), 0) as scaled_score_{{ domain }}
        {%- endfor %}
    from centred_scores
),

-- soft-clip each z-score to (-3, 3) with 3 * tanh(z / 3): close to z near the mean and
-- compressing the tails, so no domain's long tail dominates while a higher z still
-- scores higher (unlike a hard clip at 3)
bounded_scores as (
    select *
        {%- for domain in domain_weights %}
        , 3 * tanh(zeroifnull(scaled_score_{{ domain }}) / 3) as bounded_score_{{ domain }}
        {%- endfor %}
    from scaled_scores
),

-- combine the bounded domain z-scores as a weighted average. Each domain is also shown on
-- 0-100 for interpretation; the composite uses the unrounded bounded scores.
combined_scores as (
    select *
        {%- for domain in domain_weights %}
        , round((bounded_score_{{ domain }} + 3) / 6.0 * 100, 1) as score_{{ domain }}_0_100
        {%- endfor %}
        , (
            {%- for domain, weight in domain_weights.items() %}
            {% if not loop.first %}+ {% endif %}bounded_score_{{ domain }} * {{ weight }}
            {%- endfor %}
          ) / {{ weight_total }} as raw_score_treatment
    from bounded_scores
),

centred_composite as (
    select *
        , raw_score_treatment
            - avg(raw_score_treatment) over (partition by neighbourhood_code, inclusion_rule) as centred_score_treatment
    from combined_scores
),

-- averaging several domains narrows the composite's spread, so re-standardise it the same
-- way as each domain (pooled within-neighbourhood standard deviation for the cohort).
-- No age adjustment is applied.
composite_scores as (
    select *
        , zeroifnull(
            centred_score_treatment
            / nullif(stddev(centred_score_treatment) over (partition by inclusion_rule), 0)
          ) as composite_z
        -- map to 0-100 on a straight line, so the score keeps the composite's shape: 50 is an
        -- average patient and each z unit is worth score_points_per_z. The hard cap only
        -- keeps the few patients beyond the range within 0-100.
        , round(least(greatest(50 + {{ score_points_per_z }} * composite_z, 0), 100), 1) as score_treatment
    from centred_composite
),

{% set tier_1_top_pct = ((1 - tier_1_percentile) * 100) | round | int %}
{% set tier_2_top_pct = ((1 - tier_2_percentile) * 100) | round | int %}

age_banded_scores as (
    select *
        , case
            when age is null then 'Unknown'
            when age < 40 then '18-39'
            when age < 60 then '40-59'
            when age < 75 then '60-74'
            else '75+'
          end as age_band
    from composite_scores
),

-- priority tiers for clinicians, by position within the neighbourhood, cohort and age band,
-- so each age band has its own top tiers
tiered_scores as (
    select *
        , case
            when percent_rank() over (partition by neighbourhood_code, inclusion_rule, age_band order by score_treatment)
                >= {{ tier_1_percentile }} then 1
            when percent_rank() over (partition by neighbourhood_code, inclusion_rule, age_band order by score_treatment)
                >= {{ tier_2_percentile }} then 2
            else 3
          end as treatment_priority_tier
    from age_banded_scores
)

labelled_scores as (
    select *
        , case treatment_priority_tier
            when 1 then 'Top {{ tier_1_top_pct }}% in neighbourhood and age band'
            when 2 then 'Top {{ tier_1_top_pct }}-{{ tier_2_top_pct }}% in neighbourhood and age band'
            else 'Below top {{ tier_2_top_pct }}% in neighbourhood and age band'
          end as treatment_priority_tier_label
    from tiered_scores
)

-- last step: the age multiplier from cltcs_score_treatment, applied to score_treatment in a
-- separate column so the unadjusted score and the tiers are unchanged
select
        *,
        case
            when age is null then 1.0
            else (
                1
                -- max +15% boost at age 18, linear decline to 0 at age 60
                + 0.15 * (60 - least(greatest(age, 18), 60)) / 42.0
                -- linear penalty from age 60, capped at -15% from age 100 onward
                - 0.15 * least(greatest(age - 60, 0), 40) / 40.0
            )
        end as age_multiplier,
        round(least(greatest(score_treatment * age_multiplier, 0), 100), 1) as score_treatment_age_adjusted
    from labelled_scores
