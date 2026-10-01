-- LTC LCS: Diabetes Register - Priority Group 2 (High Risk)
-- Parent population: Diabetes register, excluding PG1 (HRC)
--
-- Inclusion rules (any one qualifies):
-- - Rule 2: HbA1c 75-90
-- - Rule 3: Diabetic foot ulceration in last 3 years
-- - Rule 4: earliest CHD code (episode excluding Review/End) in last 1 year
-- - Rule 5: earliest stroke/TIA code (episode excluding Review/End) in last 1 year
-- - Rule 6: eGFR 15-29
-- - Rule 7: ACR > 70 (if failed, excluded from final result)

with
-- Parent population: Patients on diabetes register
diabetes_register as (
    select distinct person_id
    from {{ ref('fct_person_diabetes_register') }}
),

-- Rule 1: Exclude patients already in PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_dm_pg1_hrc') }}
),

-- Rule 2: HbA1c 75-90 (latest value)
-- vs1 = HbA1c codes
rule_2_hba1c_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg2_hr_vs1") }})
    where result_value > 75 and result_value <= 90
),

-- Rule 3: Diabetic foot ulceration in last 3 years
-- vs2 = foot ulcer codes
rule_3_foot_ulcer as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_dm_reg_pg2_hr_vs2") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- Rule 4: earliest CHD code (episode excluding Review/End) within last 1 year
-- vs3 = CHD_COD codes
rule_4_mi as (
    select person_id
    from (
        select o.person_id, o.observation_id, o.clinical_effective_date
        from ({{ get_ltc_lcs_observations("on_dm_reg_pg2_hr_vs3") }}) o
        left join {{ ref('stg_olids_enriched_concept_map') }} ecm
            on o.episodicity_source_concept_id = ecm.source_concept_id
        where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
    )
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and clinical_effective_date >= dateadd(year, -1, current_date())
),

-- Rule 5: earliest stroke/TIA code (episode excluding Review/End) within last 1 year
-- vs4 = STRK_COD, vs5 = TIA_COD
rule_5_cerebral_thrombosis_tia as (
    select person_id
    from (
        select o.person_id, o.observation_id, o.clinical_effective_date
        from ({{ get_ltc_lcs_observations("on_dm_reg_pg2_hr_vs4, on_dm_reg_pg2_hr_vs5") }}) o
        left join {{ ref('stg_olids_enriched_concept_map') }} ecm
            on o.episodicity_source_concept_id = ecm.source_concept_id
        where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
    )
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and clinical_effective_date >= dateadd(year, -1, current_date())
),

-- Rule 6: eGFR 15-29 (latest value)
-- vs6 = eGFR codes
rule_6_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg2_hr_vs6") }})
    where result_value >= 15 and result_value < 29
),

-- Rule 7: ACR > 70 (latest value)
-- vs7 = ACR codes
rule_7_acr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg2_hr_vs7") }})
    where result_value > 70
),

-- Patients who have ACR data but don't meet threshold (Rule 7 failed = exclude)
rule_7_acr_tested as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_dm_reg_pg2_hr_vs7") }})
),

-- Combine rule results for all diabetes register patients (excluding PG1)
patient_rules as (
    select
        dr.person_id,
        (r2.person_id is not null) as rule_2_hba1c_range,
        (r3.person_id is not null) as rule_3_foot_ulcer,
        (r4.person_id is not null) as rule_4_mi,
        (r5.person_id is not null) as rule_5_cerebral_thrombosis_tia,
        (r6.person_id is not null) as rule_6_egfr_range,
        (r7.person_id is not null) as rule_7_acr_high,
        (r7_tested.person_id is not null) as has_acr_test,
        case
            when r2.person_id is not null then true
            when r3.person_id is not null then true
            when r4.person_id is not null then true
            when r5.person_id is not null then true
            when r6.person_id is not null then true
            when r7.person_id is not null then true
            else false
        end as meets_inclusion_criteria,
        -- Rule 7 logic: if tested and failed (has test but ACR <= 70), exclude
        -- Only exclude if they reached rule 7 (didn't pass rules 2-6) and failed
        case
            when r2.person_id is not null or r3.person_id is not null or r4.person_id is not null
                 or r5.person_id is not null or r6.person_id is not null or r7.person_id is not null
                then 'Included'
            else 'Excluded'
        end as final_status
    from diabetes_register dr
    -- Exclude PG1 patients
    left join pg1_exclusions pg1 on dr.person_id = pg1.person_id
    left join rule_2_hba1c_range r2 on dr.person_id = r2.person_id
    left join rule_3_foot_ulcer r3 on dr.person_id = r3.person_id
    left join rule_4_mi r4 on dr.person_id = r4.person_id
    left join rule_5_cerebral_thrombosis_tia r5 on dr.person_id = r5.person_id
    left join rule_6_egfr_range r6 on dr.person_id = r6.person_id
    left join rule_7_acr_high r7 on dr.person_id = r7.person_id
    left join rule_7_acr_tested r7_tested on dr.person_id = r7_tested.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Diabetes' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
