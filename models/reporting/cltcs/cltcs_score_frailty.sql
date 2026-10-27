{{ config(materialized='table', tags=['cltcs']) }}


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

-- Frailty evidence is tiered. Clinician assessments (Rockwood CFS and coded frailty
-- severity) take precedence; eFI2 is used only as a fallback.
-- Severity: 0 fit, 1 mild (CFS 4-5), 2 moderate (CFS 6), 3 severe (CFS 7-9).
clinical_assessments as (
    select r.person_id
        , r.clinical_effective_date as assessed_date
        , 1 as source_priority
        , case r.frailty_category
            when 'Severe Frailty' then 3
            when 'Moderate Frailty' then 2
            when 'Mild Frailty' then 1
            when 'Vulnerable' then 1
            else 0
        end as severity
    from {{ ref('int_rockwood_all') }} r
    inner join spine s on r.person_id = s.person_id
    where r.is_valid_rockwood_code
    union all
    select d.person_id
        , d.clinical_effective_date
        , 2
        , case d.frailty_severity
            when 'Severe' then 3
            when 'Moderate' then 2
            else 1
        end
    from {{ ref('int_frailty_diagnoses_all') }} d
    inner join spine s on d.person_id = s.person_id
    where d.clinical_effective_date <= current_date()
),

-- Most recent clinical assessment; on the same date Rockwood is preferred over a
-- coded severity, then the more severe finding.
latest_clinical as (
    select person_id, assessed_date, severity
    from clinical_assessments
    qualify row_number() over (
        partition by person_id
        order by assessed_date desc, source_priority, severity desc) = 1
),

frailty_evidence as (
    select s.person_id
        , lc.assessed_date as clinical_frailty_assessed_date
        , lc.assessed_date >= dateadd(month, -24, current_date()) as is_clinical_current
        , lc.severity as clinical_severity
        , case lower(efi.category)
            when 'severe frailty' then 3
            when 'moderate frailty' then 2
            when 'mild frailty' then 1
            when 'robust' then 0
        end as efi2_severity
    from spine s
    left join latest_clinical lc on s.person_id = lc.person_id
    left join {{ ref('fct_person_efi2') }} efi on s.person_id = efi.person_id
),

-- A current clinical assessment stands in either direction. A stale one is kept as
-- a floor that eFI2 may raise, since frailty is largely progressive. eFI2 alone is
-- used only where there is no clinical assessment; it covers ages 65+ only.
combined_frailty as (
    select person_id
        , clinical_frailty_assessed_date
        , case
            when is_clinical_current then 'clinical_current'
            when clinical_severity is not null and efi2_severity > clinical_severity then 'clinical_stale_efi2_higher'
            when clinical_severity is not null then 'clinical_stale'
            when efi2_severity is not null then 'efi2_fallback'
            else 'not_assessed'
        end as frailty_evidence_source
        , case
            when is_clinical_current then clinical_severity
            when clinical_severity is not null then greatest(clinical_severity, zeroifnull(efi2_severity))
            else efi2_severity
        end as frailty_severity
    from frailty_evidence
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

        -- frailty: clinical evidence first, eFI2 discounted as a fallback
        cf.frailty_severity,
        cf.frailty_evidence_source,
        cf.clinical_frailty_assessed_date,
        zeroifnull(cf.frailty_severity) * case cf.frailty_evidence_source
            when 'clinical_current' then 1.0
            when 'clinical_stale' then 0.85
            when 'clinical_stale_efi2_higher' then 0.6
            when 'efi2_fallback' then 0.6
            else 0
        end as frailty_evidence_weight,

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
        zeroifnull(polyp.medication_count) as polypharmacy_count,

        -- emergency use
        zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo,
        zeroifnull(aea.ae_tot_3mo) as ae_tot_3mo,
        zeroifnull(aea.ae_inj_12mo) as ae_inj_12mo,
        zeroifnull(apca.apc_nel_12mo) as apc_nel_12mo,
        zeroifnull(apca.apc_los_12mo) as apc_los_12mo,
        zeroifnull(apca.acs_nel_12mo) as acs_nel_12mo,
        ln(1 + greatest(zeroifnull(aea.ae_tot_3mo) - zeroifnull(aea.ae_tot_12mo) / 4.0, 0)) as uec_recency_excess,
        -- predicted one-year emergency admission risk (%); null when not scored
        iff(qa.is_scored, qa.score_percent, null) as qadmissions_score_percent,

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

    from spine il
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
    left join combined_frailty cf
        on il.person_id =cf.person_id
    left join {{ ref('fct_person_polypharmacy_current') }} polyp
        on il.person_id =polyp.person_id
    left join {{ ref('int_illicit_drug_use_latest') }} id
        on il.person_id =id.person_id
    left join {{ ref('dim_person_care_home') }} ch
        on il.person_id =ch.person_id
    left join {{ref('fct_person_asc_service_recent')}} asc_cld
        on il.sk_patient_id = asc_cld.sk_patient_id
    left join {{ ref('qadmissions_predictions') }} qa
        on il.person_id = qa.person_id

),

domain_sub_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        age,
        frailty_severity,
        frailty_evidence_source,
        clinical_frailty_assessed_date,
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
        ( frailty_evidence_weight
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
        ) as score_emergency_use_history,
        -- QAdmissions term is added per cohort below, where missing values are imputed
        ln(1 + qadmissions_score_percent / 10 ) as qadmissions_term,
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

cohort_sub_scores as(
    select dss.*
    , il.inclusion_rule
    , il.is_applicable_rule
    -- People QAdmissions does not score (outside 18-100, sex not male/female, or
    -- rejected inputs) take the median term for their neighbourhood and cohort.
    , dss.score_emergency_use_history
        + zeroifnull(coalesce(
            dss.qadmissions_term,
            median(dss.qadmissions_term) over (partition by dss.neighbourhood_code, il.inclusion_rule)
        )) as score_emergency_use
    from inclusion_list il
    inner join domain_sub_scores dss on il.sk_patient_id = dss.sk_patient_id
),

composite_scores as (
    select
        sk_patient_id,
        neighbourhood_code,
        practice_code,
        inclusion_rule,
        is_applicable_rule,
        age,
        frailty_severity,
        frailty_evidence_source,
        clinical_frailty_assessed_date,
        (score_clinical_complexity - avg(score_clinical_complexity) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_clinical_complexity) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_clinical_complexity,
        (score_clinical_frailty - avg(score_clinical_frailty) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_clinical_frailty) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_clinical_frailty,
        (score_medicines_management - avg(score_medicines_management) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_medicines_management) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_medicines_management,
        (score_emergency_use - avg(score_emergency_use) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_emergency_use) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_emergency_use,
        (score_wider_care_engagement - avg(score_wider_care_engagement) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_wider_care_engagement) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_wider_care_engagement,
        (score_asc_indicators - avg(score_asc_indicators) over (partition by neighbourhood_code, inclusion_rule)) / nullif(stddev(score_asc_indicators) over (partition by neighbourhood_code, inclusion_rule), 0) as scaled_score_asc_indicators,
        score_exclusions
    from cohort_sub_scores ),

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
    round(percent_rank() over (partition by neighbourhood_code, inclusion_rule order by raw_score_frailty) * 100, 1) as score_frailty_percentile
from reweighted_scores
