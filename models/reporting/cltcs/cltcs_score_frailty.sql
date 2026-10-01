{{ config(materialized='table', tags=['cltcs']) }}
with inclusion_list as (
    select *
    from {{ ref('cltcs_adult_population') }}
),

encoding_features as (
    select
        il.sk_patient_id,
        il.neighbourhood_code,
        il.practice_code,
        pd.age,

        -- multimorbidity
        case when pc.has_atrial_fibrillation = true then 1 else 0 end as has_atrial_fibrillation_flag, 
        case when pc.has_coronary_heart_disease = true then 1 else 0 end as has_coronary_heart_disease_flag, 
        case when pc.has_heart_failure = true then 1 else 0 end as has_heart_failure_flag, 
        case when pc.has_chronic_kidney_disease = true then 1 else 0 end as has_chronic_kidney_disease_flag, 
        case when pc.has_copd = true then 1 else 0 end as has_copd_flag, 
        case when pc.has_diabetes = true then 1 else 0 end as has_diabetes_flag, 
        case when pc.has_multiple_sclerosis = true then 1 else 0 end as has_multiple_sclerosis_flag, 
        case when pc.has_dementia = true then 1 else 0 end as has_dementia_flag, 
        case when pc.has_osteoporosis = true then 1 else 0 end as has_osteoporosis_flag, 
        case when pc.has_osteoarthritis = true then 1 else 0 end as has_osteoarthritis_flag, 
        case when pc.has_rheumatoid_arthritis = true then 1 else 0 end as has_rheumatoid_arthritis_flag, 
        case when pc.has_stroke_tia  = true then 1 else 0 end as has_stroke_tia_flag, 
        case when pc.has_parkinsons = true then 1 else 0 end as has_parkinsons_flag, 
        case when pc.has_frailty = true then 1 else 0 end as has_frailty_flag, 

        -- frailty
        zeroifnull(efi.efi_score) as efi2_score,
        case
            when lower(efi.category) = 'severe frailty' then 3
            when lower(efi.category) = 'moderate frailty' then 2
            when lower(efi.category) = 'mild frailty' then 1
            else 0
        end as efi2_category_weight,
        zeroifnull(rockwood.rockwood_score) as rockwood_score,
        case
            when rockwood.rockwood_score is null then 0
            when rockwood.rockwood_score >= 7 then 3
            when rockwood.rockwood_score >= 5 then 2
            when rockwood.rockwood_score >= 3 then 1
            else 0
        end as rockwood_category_weight,
        case 
            when fr.latest_frailty_severity is null then 0
            when fr.latest_frailty_severity = 'Severe' then 3
            when fr.latest_frailty_severity = 'Moderate' then 2
            when fr.latest_frailty_severity = 'Mild' then 1
            else 0
        end as frailty_severity_weight,

        -- medicines management
        case 
            when polyp.is_polypharmacy_5plus is null then 0
            when polyp.is_polypharmacy_5plus then 1 
            else 0 
            end as polypharmacy_5plus_flag,
        case 
            when polyp.is_polypharmacy_10plus is null then 0
            when polyp.is_polypharmacy_10plus then 1 
            else 0 
            end as polypharmacy_10plus_flag,
        zeroifnull(polyp.medication_count) as medication_count,

        -- emergency use
        zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo,
        zeroifnull(aea.ae_tot_3mo) as ae_tot_3mo,
        zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo,
        zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo,
        zeroifnull(apca.apc_los_12mo) as apc_los_12mo,
        zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo,
        ln(1 + greatest(zeroifnull(aea.ae_tot_3mo) - zeroifnull(aea.ae_tot_12mo) / 4.0, 0)) as uec_recency_excess,

        -- residential / social factors
        case when ch.is_care_home_resident = true then 1 else 0 end as is_care_home_flag,
        case when pc.has_palliative_care = true then 1 else 0 end as is_palliative_care_flag,
        case when ps.is_housebound = true then 1 else 0 end as is_housebound_flag,
        case when pc.has_learning_disability = true then 1 else 0 end as has_learning_disability_flag,
        case when pc.has_severe_mental_illness = true then 1 else 0 end as has_severe_mental_illness_flag,
        case when id.illicit_drug_pattern is not null
                  and id.illicit_drug_pattern <> 'Does not misuse drugs' then 1 else 0 end as substance_misuse_flag,
        -- wider care engagement
        asc_cld.primary_support_reason_category_count,
        case when asc_cld.has_physical_support_personal_care = true then 1 else 0 end as has_physical_support_personal_care_flag,
        case when asc_cld.has_physical_support_access_mobility = true then 1 else 0 end as has_physical_support_access_mobility_flag,
        case when asc_cld.has_memory_cognition_support = true then 1 else 0 end as has_memory_cognition_support_flag,
        case when asc_cld.has_social_support_unpaid_carer = true then 1 else 0 end as has_social_support_unpaid_carer_flag,
        case when asc_cld.has_social_support_social_isolation = true then 1 else 0 end as has_social_support_social_isolation_flag,
        case when asc_cld.has_sensory_support_visual_impairment = true then 1 else 0 end as has_sensory_support_visual_impairment_flag,
        case when asc_cld.has_sensory_support_hearing_impairment = true then 1 else 0 end as has_sensory_support_hearing_impairment_flag,
        case when asc_cld.has_sensory_support_dual_impairment = true then 1 else 0 end as has_sensory_support_dual_impairment_flag,

    from inclusion_list il
    left join {{ ref('dim_person_demographics') }} pd
        on il.person_id =pd.person_id
    left join {{ ref('dim_person_conditions') }} pc
        on il.person_id =pc.person_id
    left join {{ref('dim_person_status_summary')}} ps
        on il.person_id =ps.person_id
    left join {{ ref('fct_person_sus_uec_recent') }} aea
        on il.sk_patient_id =aea.sk_patient_id
    left join {{ ref('fct_person_sus_apc_recent') }} apca
        on il.sk_patient_id =apca.sk_patient_id
    left join {{ ref('fct_person_efi2') }} efi
        on il.person_id =efi.person_id
    left join {{ ref('fct_person_frailty_register') }} fr
        on il.person_id =fr.person_id
    left join {{ ref('int_rockwood_latest') }} rockwood
        on il.person_id =rockwood.person_id
    left join {{ ref('fct_person_polypharmacy_current') }} polyp
        on il.person_id =polyp.person_id
    left join {{ ref('int_illicit_drug_use_latest') }} id
        on il.person_id =id.person_id
    left join {{ ref('dim_person_care_home') }} ch
        on il.person_id =ch.person_id
    left join {{ref('fct_person_asc_service_recent')}} asc_cld
        on il.sk_patient_id = asc_cld.sk_patient_id

),

domain_sub_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        -- clinical complexity / multimorbidity
        ( has_atrial_fibrillation_flag
        + has_coronary_heart_disease_flag
        + has_heart_failure_flag
        + has_chronic_kidney_disease_flag
        + has_copd_flag
        + has_diabetes_flag
        + has_multiple_sclerosis_flag
        + has_dementia_flag * 3
        + has_osteoporosis_flag * 3
        + has_osteoarthritis_flag * 2
        + has_rheumatoid_arthritis_flag * 3
        + has_stroke_tia_flag *3
        + has_parkinsons_flag *3
        + ln(1+apc_los_12mo) ) as score_clinical_complexity,
        -- clinical frailty
        ( has_frailty_flag
        + greatest(frailty_severity_weight, rockwood_category_weight, efi2_category_weight) * 2
        ) as score_clinical_frailty,
        -- medicines management
        ( polypharmacy_5plus_flag
        + polypharmacy_10plus_flag * 3
        -- TO DO: add anticholinergic drug score
        ) as score_medicines_management,
        -- emergency use
        ( ln(1+ae_tot_12mo)
        + ln(1+apc_nel_12mo)
        + uec_recency_excess * 2
            -- TO DO: add specific discharge reason flags, add attendances related to geriatric syndromes
        ) as score_emergency_use,
        -- indicators of lack of independence
        ( is_housebound_flag * 3
        + has_learning_disability_flag
        + has_severe_mental_illness_flag
        + has_physical_support_personal_care_flag
        + has_physical_support_access_mobility_flag
        + has_memory_cognition_support_flag
        + has_social_support_unpaid_carer_flag
        + has_social_support_social_isolation_flag
        + has_sensory_support_visual_impairment_flag
        + has_sensory_support_hearing_impairment_flag
        + has_sensory_support_dual_impairment_flag
        -- TO DO Add community events, ASC events (num per day if possible), OP DNA rate
        ) as score_wider_care_engagement,
            -- TO DO: add community, referrals and carer support flags
        -- ASC indicators
        ( ln(1+acs_nel_12mo)
        ) as score_asc_indicators,
        -- care home and other exclusions
        ( is_care_home_flag ) as score_exclusions
from encoding_features
),

composite_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        (score_clinical_complexity - avg(score_clinical_complexity) over (partition by neighbourhood_code)) / nullif(stddev(score_clinical_complexity) over (partition by neighbourhood_code), 0) as scaled_score_clinical_complexity,
        (score_clinical_frailty - avg(score_clinical_frailty) over (partition by neighbourhood_code)) / nullif(stddev(score_clinical_frailty) over (partition by neighbourhood_code), 0) as scaled_score_clinical_frailty,
        (score_medicines_management - avg(score_medicines_management) over (partition by neighbourhood_code)) / nullif(stddev(score_medicines_management) over (partition by neighbourhood_code), 0) as scaled_score_medicines_management,
        (score_emergency_use - avg(score_emergency_use) over (partition by neighbourhood_code)) / nullif(stddev(score_emergency_use) over (partition by neighbourhood_code), 0) as scaled_score_emergency_use,
        (score_wider_care_engagement - avg(score_wider_care_engagement) over (partition by neighbourhood_code)) / nullif(stddev(score_wider_care_engagement) over (partition by neighbourhood_code), 0) as scaled_score_wider_care_engagement,
        (score_asc_indicators - avg(score_asc_indicators) over (partition by neighbourhood_code)) / nullif(stddev(score_asc_indicators) over (partition by neighbourhood_code), 0) as scaled_score_asc_indicators,
        score_exclusions
    from domain_sub_scores ),

clipped_scores as (
    select
        *,
        least(greatest(zeroifnull(scaled_score_clinical_complexity), -3), 3) as clipped_score_clinical_complexity,
        least(greatest(zeroifnull(scaled_score_clinical_frailty), -3), 3) as clipped_score_clinical_frailty,
        least(greatest(zeroifnull(scaled_score_medicines_management), -3), 3) as clipped_score_medicines_management,
        least(greatest(zeroifnull(scaled_score_emergency_use), -3), 3) as clipped_score_emergency_use,
        least(greatest(zeroifnull(scaled_score_wider_care_engagement), -3), 3) as clipped_score_wider_care_engagement,
        least(greatest(zeroifnull(scaled_score_asc_indicators), -3), 3) as clipped_score_asc_indicators
    from composite_scores
),

reweighted_scores as (
    select *,
     ( 1 * clipped_score_clinical_complexity + 2 * clipped_score_clinical_frailty + 1 * clipped_score_medicines_management + 1 * clipped_score_emergency_use + 1 * clipped_score_wider_care_engagement + 1 * clipped_score_asc_indicators) / 7 as raw_score_frailty
    from clipped_scores
)
-- score_frailty is a within-neighbourhood percentile rank of raw_score_frailty, not an
-- absolute frailty measure. Ranking within neighbourhood keeps the scale consistent with
-- the neighbourhood z-scoring above and gives a uniform 0-100 spread; the equally-weighted
-- mean of six clipped z-scores is bunched near its midpoint and never approaches its
-- theoretical -3/+3 bounds. Tied raw scores receive the same percentile.
select *,
    round((raw_score_frailty + 3) / 6.0 * 100, 1) as score_frailty,
    round(percent_rank() over (partition by neighbourhood_code order by raw_score_frailty) * 100, 1) as score_frailty_percentile
from reweighted_scores
