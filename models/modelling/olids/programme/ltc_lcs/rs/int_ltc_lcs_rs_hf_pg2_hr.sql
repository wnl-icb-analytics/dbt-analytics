-- LTC LCS: HF Register - Priority Group 2 (High Risk)
-- Parent population: HF register
-- NB: there is no PG1 for HF.
-- EMIS source: 'On HF Register- LTC LCS Priority Group 2 (HR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hf/on_hf_register_ltc_lcs_priority_group_2_hr.md;
-- rule actions verified against the EMIS search screenshots in issue #400,
-- which are canonical where the XML parse disagrees)
--
-- Rule chain:
-- - Rule 1: latest NYHA classification (vs1+vs2) is Class III or IV (vs2) -> include
-- - Rule 2: latest oedema code (vs3+vs4) is significant oedema (vs4), or
--   metolazone order (vs5) in last year -> include
-- - Rule 3 (gate): latest HbA1c (vs6) > 48 and <= 75. Fail -> exclude.
-- - Rule 4 (gate): latest LVEF (vs7) < 50, or HFLVSD/REDEJCFRAC code (vs8/vs9)
--   with episode excluding Review/End. Fail -> exclude.
-- - Rule 5: on SGLT2 - course commenced (vs10) or order (vs10) in last
--   6 months, or adverse reaction (vs11). Fail -> include (therapy gap).
-- - Rule 6: on MRA - course commenced (vs12) or order (vs12) in last
--   6 months, or adverse reaction (vs13). Fail -> include.
-- - Rule 7: on beta-blocker - order (vs14) in last 6 months, atenolol
--   hypersensitivity (vs15), or TXLBB/LBBDEC code (vs16/vs17) on or before
--   1 year ago. Fail -> include.
-- - Rule 8: on the ACE inhibitor pathway - latest ACEi order (vs18) in last
--   6 months, ACEi/AIIRA declined (vs19/vs20) in last 12 months, persistent
--   ACEi contraindication (XACE_COD cluster), ACEi adverse reaction (vs21) in
--   last 12 months, XAII code ever (vs22), or TXAII (vs23) in last 12 months.
--   Pass -> exclude; fail -> include.

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
    where is_on_register = true
),

-- Rule 1: latest NYHA classification is Class III or IV.
-- vs2 (Class III/IV) is a subset of the NYHA pool, so the class check
-- matches on observation_id.
rule_1_nyha_class_3_4 as (
    select latest_nyha.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs1, on_hf_reg_pg2_hr_vs2") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_nyha
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs2") }}) class_3_4
        on latest_nyha.observation_id = class_3_4.observation_id
),

-- Rule 2: latest oedema code is significant oedema, or metolazone in last year
rule_2_oedema_or_metolazone as (
    select latest_oedema.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs3, on_hf_reg_pg2_hr_vs4") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_oedema
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs4") }}) significant
        on latest_oedema.observation_id = significant.observation_id
    union
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg2_hr_vs5") }})
    where order_date >= dateadd(year, -1, current_date())
),

-- Rule 3: latest HbA1c > 48 and <= 75
rule_3_hba1c as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs6") }})
    where result_value > 48 and result_value <= 75
),

-- Rule 4: latest LVEF < 50, or HFLVSD/REDEJCFRAC code with episode
-- excluding Review/End
rule_4_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs7") }})
    where result_value < 50
    union
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs8, on_hf_reg_pg2_hr_vs9") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
),

-- Rule 5: on SGLT2 inhibitors
rule_5_sglt2 as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg2_hr_vs10") }})
    where statement_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs10") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs11") }})
),

-- Rule 6: on MRAs
rule_6_mra as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg2_hr_vs12") }})
    where statement_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs12") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs13") }})
),

-- Rule 7: on beta-blockers
rule_7_beta_blockers as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg2_hr_vs14") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs15") }})
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs16") }})
    where clinical_effective_date <= dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs17") }})
    where clinical_effective_date <= dateadd(year, -1, current_date())
),

-- Rule 8: on the ACE inhibitor pathway
rule_8_ace_pathway as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg2_hr_vs18") }})
    where order_date > dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs19") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- Persistent ACEi contraindications (EMIS library item, XACE_COD cluster)
    select distinct person_id
    from ({{ get_observations("'XACE_COD'") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs21") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg2_hr_vs22") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg2_hr_vs23") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (r1.person_id is not null) as rule_1_nyha_class_3_4,
        (r2.person_id is not null) as rule_2_oedema_or_metolazone,
        (r3.person_id is not null) as rule_3_hba1c,
        (r4.person_id is not null) as rule_4_lvef,
        (r5.person_id is not null) as rule_5_sglt2,
        (r6.person_id is not null) as rule_6_mra,
        (r7.person_id is not null) as rule_7_beta_blockers,
        (r8.person_id is not null) as rule_8_ace_pathway,
        case
            when r1.person_id is not null then 'Included'
            when r2.person_id is not null then 'Included'
            when r3.person_id is null then 'Excluded'
            when r4.person_id is null then 'Excluded'
            when r5.person_id is null then 'Included'
            when r6.person_id is null then 'Included'
            when r7.person_id is null then 'Included'
            when r8.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hf_register hr
    left join rule_1_nyha_class_3_4 r1 on hr.person_id = r1.person_id
    left join rule_2_oedema_or_metolazone r2 on hr.person_id = r2.person_id
    left join rule_3_hba1c r3 on hr.person_id = r3.person_id
    left join rule_4_lvef r4 on hr.person_id = r4.person_id
    left join rule_5_sglt2 r5 on hr.person_id = r5.person_id
    left join rule_6_mra r6 on hr.person_id = r6.person_id
    left join rule_7_beta_blockers r7 on hr.person_id = r7.person_id
    left join rule_8_ace_pathway r8 on hr.person_id = r8.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'HF' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
