-- LTC LCS: CHD Register - Priority Group 1 (High Risk & Complex)
-- Implements inclusion/exclusion rules for high-risk & complex patient identification
-- Parent population: CHD register
--
-- Inclusion rules:
-- - Rule 1a: First/new or flare-up of CHD within last month or
-- - Rule 1b: Significant CHD within last month. Rule 1b is not possible with current data.
--   'Significant' CHD within last month is a data gap. Problem significance is not in OLIDS.
--   It is also not used consistently by all practices and not used in a consistent way.

with
-- Parent population: Patients currently on CHD register
chd_register as (
    select distinct person_id
    from {{ ref('fct_person_chd_register') }}
),
-- Rule 1a: First/new or flare-up of CHD within last month 45
rule_1a_first_new_flare_up as (
    select person_id
    from
        (select
        *
        from ({{ get_ltc_lcs_observations("on_chd_reg_pg1_hrc_vs1") }})) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id -- join episodiity for first/new/flare-up
    where
    clinical_effective_date > dateadd(month, -1, current_date())
        and ecm.source_display in ('First','New','Flare Up')
    qualify row_number() over (partition by person_id order by clinical_effective_date desc) = 1
),

-- Rule 1b: 'Significant' CHD within last month is a data gap. Problem significance is not in OLIDS.
--           It is also not used consistently by all practices and not used in a consistent way.

-- Combine rule results for all CHD register patients
patient_rules as (
    select
        cr.person_id,
        (r1a.person_id is not null) as rule_1a_first_new_flare_up,
        case
            when r1a.person_id is not null then 'Included'  -- Rule 1a passed
            else 'Excluded'
        end as final_status
    from chd_register cr
    left join rule_1a_first_new_flare_up r1a on cr.person_id = r1a.person_id
)
-- Final result: included patients only
select
    person_id,
    final_status,
    'CHD' as condition,
    '1' as priority_group,
    'HRC' as risk_group
from patient_rules
where final_status = 'Included'
