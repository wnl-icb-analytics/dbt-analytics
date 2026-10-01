-- LTC LCS: Stroke/TIA Register - Priority Group 1 (High Risk & Complex)
-- Parent population: Stroke/TIA register
-- EMIS source: 'On Stroke/TIA Register- LTC LCS Priority Group 1 (HRC)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/stroke_tia/on_stroke_tia_register_ltc_lcs_priority_group_1_hrc.md)
--
-- Rule chain:
-- - Rule 1a: stroke (vs1, STRK_COD) or TIA (vs2, TIA_COD) code in last 30 days
--   with Episode = First, New or Flare Up -> include
-- - Rule 1b: same codes in last 30 days with Problem Significance = Significant.
--   Data gap: problem significance is not in OLIDS (same as CHD PG1 rule 1b),
--   so this arm cannot be implemented.

with
-- Parent population: Patients currently on Stroke/TIA register
stroke_tia_register as (
    select distinct person_id
    from {{ ref('fct_person_stroke_tia_register') }}
    where is_on_register = true
),

-- Rule 1a: first/new or flare-up stroke/TIA code in last 30 days
rule_1a_recent_stroke_episode as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_stroketia_reg_pg1_hrc_vs1, on_stroketia_reg_pg1_hrc_vs2") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where o.clinical_effective_date >= dateadd(day, -30, current_date())
        and ecm.source_display in ('First', 'New', 'Flare Up')
),

-- Combine rule results for all Stroke/TIA register patients
patient_rules as (
    select
        sr.person_id,
        (r1a.person_id is not null) as rule_1a_recent_stroke_episode,
        case
            when r1a.person_id is not null then 'Included'
            else 'Excluded'
        end as final_status
    from stroke_tia_register sr
    left join rule_1a_recent_stroke_episode r1a on sr.person_id = r1a.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Stroke/TIA' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
