-- LTC LCS: HF Register - Priority Group 3 (Medium Risk)
-- Parent population: HF register, excluding PG2 (HR)
-- EMIS source: 'On HF Register- LTC LCS Priority Group 3 (MR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/hf/on_hf_register_ltc_lcs_priority_group_3_mr.md;
-- rule actions verified against the EMIS search screenshots in issue #400,
-- which are canonical where the XML parse disagrees)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG2 (HR) patients
-- - Rule 2: earliest HF code (vs1, HF_COD refset, episode excluding
--   Review/End) in last 6 months -> include (first diagnosis within 6 months)
-- - Rule 3: latest NYHA classification (vs2+vs3) is Class II (vs3) -> include
-- - Rule 4: latest oedema code (vs4+vs5) is ankle/feet oedema (vs5) -> include
-- - Rule 5 (gate): latest LVEF (vs6) < 50. Fail -> exclude.
-- - Rule 6: on SGLT2 - course commenced (vs7) or order (vs7) in last
--   6 months, or adverse reaction (vs8). Fail -> include (therapy gap).
-- - Rule 7: on MRA - course commenced (vs9) or order (vs9) in last 6 months,
--   or adverse reaction (vs10). Fail -> include.
-- - Rule 8: on beta-blocker - order (vs11) in last 6 months, atenolol
--   hypersensitivity (vs12), or TXLBB/LBBDEC code (vs13/vs14) on or before
--   1 year ago. Fail -> include.
-- - Rule 9: on the ACE inhibitor pathway - latest ACEi order (vs15) in last
--   6 months, ACEi/AIIRA declined (vs16/vs17) in last 12 months, persistent
--   ACEi contraindication (XACE_COD cluster), ACEi adverse reaction (vs18) in
--   last 12 months, XAII code ever (vs19), or TXAII (vs20) in last 12 months.
--   Pass -> exclude; fail -> include.

with
-- Parent population: Patients currently on HF register
hf_register as (
    select distinct person_id
    from {{ ref('fct_person_heart_failure_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG2 (HR)
pg2_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_hf_pg2_hr') }}
),

-- Rule 2: earliest HF code (episode excluding Review/End) in last 6 months
rule_2_new_hf_diagnosis as (
    select person_id
    from (
        select o.person_id, o.observation_id, o.clinical_effective_date
        from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs1") }}) o
        left join {{ ref('stg_olids_enriched_concept_map') }} ecm
            on o.episodicity_source_concept_id = ecm.source_concept_id
        where ecm.source_display not in ('Review', 'End') or ecm.source_display is null
    )
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and clinical_effective_date > dateadd(month, -6, current_date())
),

-- Rule 3: latest NYHA classification is Class II.
-- vs3 (Class II) is a subset of the NYHA pool, so the class check matches
-- on observation_id.
rule_3_nyha_class_2 as (
    select latest_nyha.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs2, on_hf_reg_pg3_mr_vs3") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_nyha
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs3") }}) class_2
        on latest_nyha.observation_id = class_2.observation_id
),

-- Rule 4: latest oedema code is ankle/feet oedema
rule_4_oedema as (
    select latest_oedema.person_id
    from (
        select person_id, observation_id
        from (
            select distinct person_id, observation_id, clinical_effective_date
            from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs4, on_hf_reg_pg3_mr_vs5") }})
        )
        qualify row_number() over (
            partition by person_id
            order by clinical_effective_date desc, observation_id desc
        ) = 1
    ) latest_oedema
    inner join ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs5") }}) ankle_feet
        on latest_oedema.observation_id = ankle_feet.observation_id
),

-- Rule 5: latest LVEF < 50
rule_5_lvef as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs6") }})
    where result_value < 50
),

-- Rule 6: on SGLT2 inhibitors
rule_6_sglt2 as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg3_mr_vs7") }})
    where statement_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs7") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs8") }})
),

-- Rule 7: on MRAs
rule_7_mra as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_statements("on_hf_reg_pg3_mr_vs9") }})
    where statement_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs9") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs10") }})
),

-- Rule 8: on beta-blockers
rule_8_beta_blockers as (
    select distinct person_id
    from ({{ get_ltc_lcs_medication_orders("on_hf_reg_pg3_mr_vs11") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs12") }})
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs13") }})
    where clinical_effective_date <= dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs14") }})
    where clinical_effective_date <= dateadd(year, -1, current_date())
),

-- Rule 9: on the ACE inhibitor pathway
rule_9_ace_pathway as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("on_hf_reg_pg3_mr_vs15") }})
    where order_date > dateadd(month, -6, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs16") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs17") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    -- Persistent ACEi contraindications (EMIS library item, XACE_COD cluster)
    select distinct person_id
    from ({{ get_observations("'XACE_COD'") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs18") }})
    where clinical_effective_date > dateadd(year, -1, current_date())
    union
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_hf_reg_pg3_mr_vs19") }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations_latest("on_hf_reg_pg3_mr_vs20") }})
    where clinical_effective_date > dateadd(month, -12, current_date())
),

-- Combine rule results for all HF register patients
patient_rules as (
    select
        hr.person_id,
        (pg2.person_id is not null) as rule_1_in_pg2,
        (r2.person_id is not null) as rule_2_new_hf_diagnosis,
        (r3.person_id is not null) as rule_3_nyha_class_2,
        (r4.person_id is not null) as rule_4_oedema,
        (r5.person_id is not null) as rule_5_lvef,
        (r6.person_id is not null) as rule_6_sglt2,
        (r7.person_id is not null) as rule_7_mra,
        (r8.person_id is not null) as rule_8_beta_blockers,
        (r9.person_id is not null) as rule_9_ace_pathway,
        case
            when pg2.person_id is not null then 'Excluded'
            when r2.person_id is not null then 'Included'
            when r3.person_id is not null then 'Included'
            when r4.person_id is not null then 'Included'
            when r5.person_id is null then 'Excluded'
            when r6.person_id is null then 'Included'
            when r7.person_id is null then 'Included'
            when r8.person_id is null then 'Included'
            when r9.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from hf_register hr
    left join pg2_exclusions pg2 on hr.person_id = pg2.person_id
    left join rule_2_new_hf_diagnosis r2 on hr.person_id = r2.person_id
    left join rule_3_nyha_class_2 r3 on hr.person_id = r3.person_id
    left join rule_4_oedema r4 on hr.person_id = r4.person_id
    left join rule_5_lvef r5 on hr.person_id = r5.person_id
    left join rule_6_sglt2 r6 on hr.person_id = r6.person_id
    left join rule_7_mra r7 on hr.person_id = r7.person_id
    left join rule_8_beta_blockers r8 on hr.person_id = r8.person_id
    left join rule_9_ace_pathway r9 on hr.person_id = r9.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'HF' as condition,
    '3' as priority_group,
    'MR' as risk_group
from patient_rules
where final_status = 'Included'
