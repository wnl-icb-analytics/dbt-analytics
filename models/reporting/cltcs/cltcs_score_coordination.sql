{{ config(materialized='table', tags=['cltcs']) }}

-- Domain weights for the treatment score (each default 1 = equal weighting).
{% set weight_fragmentation = 1 %}
{% set weight_impact = 1 %}
{% set weight_multimorbid = 1 %}

{% set weight_total = weight_fragmentation + weight_impact + weight_multimorbid %}

with inclusion_list as (
    select *
    from {{ ref('cltcs_adult_population') }}
    ),

-- Join and encode relevant features 
encoding_features as(
    select il.sk_patient_id
        , il.practice_code
        , zeroifnull(wl.wl_current_total_count) as wl_total_count
        , zeroifnull(wl.wl_current_distinct_providers_count) as wl_provider_count
        , zeroifnull(opa.op_att_tot_12mo) as op_att_tot_12mo
        , zeroifnull(opa.op_spec_12mo) as op_spec_12mo
        , zeroifnull(opa.op_prov_12mo) as op_prov_12mo
        , zeroifnull(gpa.gp_att_tot_12mo) as gp_att_tot_12mo
        , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
        , zeroifnull(apca.apc_los_12mo) as apc_los_12mo
        , zeroifnull(apca.apc_12mo) as apc_12mo
        , case when wl.same_tfc_multiple_providers_flag  = TRUE then 1 else 0 end as has_same_tfc_multiple_providers_flag_flag
    from inclusion_list il
    left join {{ref('fct_person_wl_current_count_total')}} wl
        on il.sk_patient_id = wl.sk_patient_id
    left join {{ref('fct_person_sus_op_recent')}} opa
        on il.sk_patient_id  = opa.sk_patient_id
    left join {{ref('fct_person_gp_recent')}} gpa
        on il.sk_patient_id  = gpa.sk_patient_id
    left join {{ref('fct_person_sus_uec_recent')}} aea
        on il.sk_patient_id  = aea.sk_patient_id
    left join {{ref('fct_person_sus_apc_recent')}} apca
        on il.sk_patient_id  = apca.sk_patient_id
)

-- Establish and derive subdomain scores
domain_sub_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        -- Domain: Disjointed care e.g.,  fragmentation of past / future appointments and waiting lists over multiple sites (across relevant service lines?), certain prescribing patterns
        (has_same_tfc_multiple_providers_flag_flag * 2
        + wl_provider_count) as score_fragmentation,

        -- Domain: Actionable mismanagement of care indicators e.g., long history of low cadence appointments in relevant service lines, referral route, certain actionable prescribing patterns, very high appointment volumes, inflated appointment count? Time since appointment with deg?
        () as score_impact,

        -- Domain: Multimorbid e.g., condition flags and prescribing patterns
        () as score_multimorbid,

        -- Domain: vulnerability to fragmented / multisite care (to explore) e.g., housebound, language, history of struggling with high volumes of appointments, cummulative distance to appointments?
        from encoding_features


-- Calculate z-score
composite_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        score_fragmentation,
        score_impact,
        score_multimorbid,
        (score_fragmentation - avg(score_fragmentation) over (partition by neighbourhood_code)) / nullif(stddev(score_fragmentation) over (partition by neighbourhood_code), 0) as scaled_score_fragmentation,
        (score_impact - avg(score_impact) over (partition by neighbourhood_code)) / nullif(stddev(score_impact) over (partition by neighbourhood_code), 0) as scaled_score_impact,
        (score_multimorbid - avg(score_multimorbid) over (partition by neighbourhood_code)) / nullif(stddev(score_multimorbid) over (partition by neighbourhood_code), 0) as scaled_score_multimorbid,

    from domain_sub_scores
),

-- Clip z-score
clipped_scores as (
    select
        *,
        least(greatest(zeroifnull(scaled_score_fragmentation), -3), 3) as clipped_score_fragmentation,
        least(greatest(zeroifnull(scaled_score_impact), -3), 3) as clipped_score_impact,
        least(greatest(zeroifnull(scaled_score_multimorbid), -3), 3) as clipped_score_multimorbid
        from composite_scores
),

-- rescale each clipped domain z-score to a 0-100 sub-score, then take the weighted
-- average across domains (normalised by the sum of weights) to get raw_score_treatment.
remapped_scores as (
    select
        *,
        round((clipped_score_fragmentation + 3) / 6.0 * 100, 1) as score_fragmentation_0_100,
        round((clipped_score_impact + 3) / 6.0 * 100, 1) as score_impact_0_100,
        round((clipped_score_multimorbid + 3) / 6.0 * 100, 1) as score_multimorbid_0_100,
        (
            score_fragmentation_0_100 * {{ weight_biomarker_gaps }}
            + score_impact_0_100 * {{ weight_biomarker_gaps }}
            + score_multimorbid_0_100 * {{ weight_biomarker_gaps }}
        ) / {{ weight_total }} as raw_score_coordination
    from clipped_scores
)


-- apply the age multiplier to the weighted 0-100 score, then bound to [1, 100].
select
    *,
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
    , 1), 100) as score_coordination
from remapped_scores
