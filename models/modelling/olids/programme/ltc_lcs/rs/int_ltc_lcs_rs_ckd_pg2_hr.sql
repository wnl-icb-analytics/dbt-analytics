-- LTC LCS: CKD Register - Priority Group 2 (High Risk)
-- Parent population: CKD register, excluding PG1 (HRC)
--
-- One row per included person. Rules 2-5 select independently; otherwise rule 6
-- (3+ repeat antihypertensive issues strictly within three months) gates rules 7-10.
-- Rules 7-8 test the latest complete clinic reading across each person's linked
-- pool, with the separate clinic-setting criterion. Rules 9-10 test the latest
-- complete home/ambulatory reading, with the separate home-setting criterion.
-- Thresholds are diastolic > 90 and systolic >= 1, or systolic > 150 and
-- diastolic >= 1. DATE = DATE links define each pool before ranking per person.
-- The lead-approved reading of Latest N followed by a test requires every
-- restricted record to pass, based on CKD PG2 EMIS practice-count comparisons.
-- Every latest-100 clinic/home marker must be strictly within 12 months for
-- that setting's threshold chain. Setting and threshold criteria may use
-- different dates. XML descriptions mentioning 140/90 or 135/85 are stale.
-- Rule 6 retains the existing project medication list, broader than the XML group.

with
-- Parent population: Patients on CKD register
ckd_register as (
    select distinct person_id
    from {{ ref('fct_person_ckd_register') }}
),

-- Rule 1: Exclude patients already in PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_ckd_pg1_hrc') }}
),

-- Rule 2: eGFR 45-59 AND ACR > 30 (compound - both required)
-- vs1 = eGFR codes
rule_2_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 45 and result_value <= 59
),
-- vs2 = ACR codes
rule_2_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value > 30
),
rule_2_combined as (
    select e.person_id
    from rule_2_egfr e
    inner join rule_2_acr a on e.person_id = a.person_id
),

-- Rule 3: eGFR 30-44 AND ACR > 3 (compound - both required)
rule_3_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 30 and result_value <= 44
),
rule_3_acr as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value > 3
),
rule_3_combined as (
    select e.person_id
    from rule_3_egfr e
    inner join rule_3_acr a on e.person_id = a.person_id
),

-- Rule 4: eGFR 15-29 (latest value)
rule_4_egfr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs1") }})
    where result_value >= 15 and result_value <= 29
),

-- Rule 5: ACR 70-250 (>=70 and <250, as 250+ is PG1 HRC)
rule_5_acr_range as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_ckd_reg_pg2_hr_vs2") }})
    where result_value >= 70 and result_value < 250
),

-- Rule 6: 3+ repeat antihypertensive prescriptions in last 3 months (gate for BP rules)
-- EMIS requires at least three repeat issues. The project antihypertensive list
-- remains broader than the EMIS drug group; the count is not a proxy for that gap.
rule_6_antihypertensive as (
    select am.person_id
    from {{ ref('int_antihypertensive_medications_all') }} am
    inner join {{ ref('stg_olids_medication_statement') }} ms
        on am.medication_statement_id = ms.id
    where am.order_date > dateadd(month, -3, current_date())
        and ms.authorisation_type_source_code = 'Repeat'
    group by am.person_id
    having count(distinct am.medication_order_id) >= 3
),

-- Keep observation membership across both match paths. BP components are linked
-- by date in this XML, rather than by the shared paired-reading model.
bp_observations as (
    select
        observation_id,
        person_id,
        clinical_effective_date,
        result_value,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs4') as is_clinic_marker,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs5') as is_home_marker,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs6') as is_any_systolic,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs7') as is_any_diastolic,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs8') as is_clinic_systolic,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs9') as is_clinic_diastolic,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs10') as is_home_systolic,
        boolor_agg(valueset_friendly_name = 'on_ckd_reg_pg2_hr_vs11') as is_home_diastolic
    from ({{ get_ltc_lcs_observations("on_ckd_reg_pg2_hr_vs4, on_ckd_reg_pg2_hr_vs5, on_ckd_reg_pg2_hr_vs6, on_ckd_reg_pg2_hr_vs7, on_ckd_reg_pg2_hr_vs8, on_ckd_reg_pg2_hr_vs9, on_ckd_reg_pg2_hr_vs10, on_ckd_reg_pg2_hr_vs11", exclude_result_headers=true) }})
    group by observation_id, person_id, clinical_effective_date, result_value
),

-- The first clinic criterion filters by date before Latest 100. The clinic/home
-- threshold criteria apply the date test after Latest 100, as encoded in the XML.
recent_markers_100 as (
    select *
    from bp_observations
    where (is_clinic_marker or is_home_marker)
        and clinical_effective_date > dateadd(month, -12, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) <= 100
),

clinic_markers_100 as (
    select *
    from bp_observations
    where is_clinic_marker
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) <= 100
),

home_markers_100 as (
    select *
    from bp_observations
    where is_home_marker
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) <= 100
),

-- Each DATE link restricts the child pool before Latest 1. The earlier Latest 100
-- systolic child is an existence check on that date; Latest 1 also proves it exists.
any_systolic_dates as (
    select distinct person_id, clinical_effective_date
    from bp_observations
    where is_any_systolic and result_value > 0
),

clinic_systolic_latest as (
    select person_id, clinical_effective_date, result_value
    from bp_observations
    where is_clinic_systolic and result_value > 0
    qualify row_number() over (
        partition by person_id, clinical_effective_date
        order by observation_id desc
    ) = 1
),

clinic_diastolic_latest as (
    select person_id, clinical_effective_date, result_value
    from bp_observations
    where is_clinic_diastolic and result_value > 0
    qualify row_number() over (
        partition by person_id, clinical_effective_date
        order by observation_id desc
    ) = 1
),

home_systolic_latest as (
    select person_id, clinical_effective_date, result_value
    from bp_observations
    where is_home_systolic and result_value > 0
    qualify row_number() over (
        partition by person_id, clinical_effective_date
        order by observation_id desc
    ) = 1
),

home_diastolic_latest as (
    select person_id, clinical_effective_date, result_value
    from bp_observations
    where is_home_diastolic and result_value > 0
    qualify row_number() over (
        partition by person_id, clinical_effective_date
        order by observation_id desc
    ) = 1
),

clinic_setting as (
    select person_id
    from (
        select d.person_id, d.is_clinic_marker
        from bp_observations as d
        where d.is_any_diastolic and d.result_value > 0
            and exists (
                select 1 from recent_markers_100 as m
                where m.person_id = d.person_id
                    and m.clinical_effective_date = d.clinical_effective_date
            )
            and exists (
                select 1 from any_systolic_dates as s
                where s.person_id = d.person_id
                    and s.clinical_effective_date = d.clinical_effective_date
            )
        qualify row_number() over (
            partition by d.person_id
            order by d.clinical_effective_date desc, d.observation_id desc
        ) = 1
    )
    -- Test the latest linked combined diastolic, rather than any qualifying date.
    where is_clinic_marker
),

clinic_bp_dates as (
    select distinct
        m.person_id,
        m.clinical_effective_date,
        s.result_value as systolic_value,
        d.result_value as diastolic_value
    from clinic_markers_100 as m
    inner join clinic_systolic_latest as s
        on m.person_id = s.person_id
        and m.clinical_effective_date = s.clinical_effective_date
    inner join clinic_diastolic_latest as d
        on s.person_id = d.person_id
        and s.clinical_effective_date = d.clinical_effective_date
),

home_bp_dates as (
    select distinct
        m.person_id,
        m.clinical_effective_date,
        s.result_value as systolic_value,
        d.result_value as diastolic_value
    from home_markers_100 as m
    inner join home_systolic_latest as s
        on m.person_id = s.person_id
        and m.clinical_effective_date = s.clinical_effective_date
    inner join home_diastolic_latest as d
        on s.person_id = d.person_id
        and s.clinical_effective_date = d.clinical_effective_date
),

-- Apply the all-marker date test before selecting the latest linked reading.
clinic_latest as (
    select b.*
    from clinic_bp_dates as b
    where b.clinical_effective_date > dateadd(month, -12, current_date())
        and not exists (
            select 1 from clinic_markers_100 as m
            where m.person_id = b.person_id
                and m.clinical_effective_date <= dateadd(month, -12, current_date())
        )
    qualify row_number() over (
        partition by b.person_id order by b.clinical_effective_date desc
    ) = 1
),

home_latest as (
    select b.*
    from home_bp_dates as b
    where b.clinical_effective_date > dateadd(month, -12, current_date())
        and not exists (
            select 1 from home_markers_100 as m
            where m.person_id = b.person_id
                and m.clinical_effective_date <= dateadd(month, -12, current_date())
        )
    qualify row_number() over (
        partition by b.person_id order by b.clinical_effective_date desc
    ) = 1
),

-- The home-setting criterion's baseCriteriaGroup unions complete clinic and home
-- chains on/before baseline, then restricts that union to Latest 1 before linking
-- back to a home chain on the same date.
baseline_markers_100 as (
    select observation_id, person_id, clinical_effective_date, 'clinic' as setting
    from bp_observations
    where is_clinic_marker and clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) <= 100
    union all
    select observation_id, person_id, clinical_effective_date, 'home' as setting
    from bp_observations
    where is_home_marker and clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) <= 100
),

complete_bp_dates as (
    select m.person_id, m.clinical_effective_date
    from baseline_markers_100 as m
    inner join clinic_systolic_latest as s
        on m.person_id = s.person_id
        and m.clinical_effective_date = s.clinical_effective_date
    inner join clinic_diastolic_latest as d
        on s.person_id = d.person_id
        and s.clinical_effective_date = d.clinical_effective_date
    where m.setting = 'clinic'
    union
    select m.person_id, m.clinical_effective_date
    from baseline_markers_100 as m
    inner join home_systolic_latest as s
        on m.person_id = s.person_id
        and m.clinical_effective_date = s.clinical_effective_date
    inner join home_diastolic_latest as d
        on s.person_id = d.person_id
        and s.clinical_effective_date = d.clinical_effective_date
    where m.setting = 'home'
),

latest_complete_bp as (
    select person_id, max(clinical_effective_date) as clinical_effective_date
    from complete_bp_dates
    group by person_id
),

home_setting as (
    select distinct b.person_id
    from latest_complete_bp as b
    inner join home_bp_dates as h
        on b.person_id = h.person_id
        and b.clinical_effective_date = h.clinical_effective_date
),

rule_7_bp_diastolic as (
    select distinct b.person_id
    from clinic_latest as b
    inner join clinic_setting as c on b.person_id = c.person_id
    where b.diastolic_value > 90 and b.systolic_value >= 1
),

rule_8_bp_systolic as (
    select distinct b.person_id
    from clinic_latest as b
    inner join clinic_setting as c on b.person_id = c.person_id
    where b.systolic_value > 150 and b.diastolic_value >= 1
),

rule_9_home_bp_diastolic as (
    select distinct b.person_id
    from home_latest as b
    inner join home_setting as h on b.person_id = h.person_id
    where b.diastolic_value > 90 and b.systolic_value >= 1
),

rule_10_home_bp_systolic as (
    select distinct b.person_id
    from home_latest as b
    inner join home_setting as h on b.person_id = h.person_id
    where b.systolic_value > 150 and b.diastolic_value >= 1
),

-- Combine rule results for all CKD register patients (excluding PG1)
-- Rules 2-5: independent inclusion criteria (eGFR/ACR thresholds)
-- Rule 6: antihypertensive is a gate (must-match) for Rules 7-10 (BP)
-- Rules 7-10: BP inclusion only applies if Rule 6 (antihypertensive gate) passes
patient_rules as (
    select
        cr.person_id,
        (r2.person_id is not null) as rule_2_egfr_acr_compound,
        (r3.person_id is not null) as rule_3_egfr_acr_compound,
        (r4.person_id is not null) as rule_4_egfr_range,
        (r5.person_id is not null) as rule_5_acr_range,
        (r6.person_id is not null) as rule_6_antihypertensive,
        (r7.person_id is not null) as rule_7_bp_diastolic,
        (r8.person_id is not null) as rule_8_bp_systolic,
        (r9.person_id is not null) as rule_9_home_bp_diastolic,
        (r10.person_id is not null) as rule_10_home_bp_systolic,
        case
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is not null then 'Included'
            -- Rule 6 is a gate: must have 3+ antihypertensives to reach BP rules
            when r6.person_id is not null and (r7.person_id is not null or r8.person_id is not null
                or r9.person_id is not null or r10.person_id is not null) then 'Included'
            else 'Excluded'
        end as final_status
    from ckd_register cr
    left join pg1_exclusions pg1 on cr.person_id = pg1.person_id
    left join rule_2_combined r2 on cr.person_id = r2.person_id
    left join rule_3_combined r3 on cr.person_id = r3.person_id
    left join rule_4_egfr_range r4 on cr.person_id = r4.person_id
    left join rule_5_acr_range r5 on cr.person_id = r5.person_id
    left join rule_6_antihypertensive r6 on cr.person_id = r6.person_id
    left join rule_7_bp_diastolic r7 on cr.person_id = r7.person_id
    left join rule_8_bp_systolic r8 on cr.person_id = r8.person_id
    left join rule_9_home_bp_diastolic r9 on cr.person_id = r9.person_id
    left join rule_10_home_bp_systolic r10 on cr.person_id = r10.person_id
    where pg1.person_id is null  -- Exclude PG1 patients
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'CKD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
