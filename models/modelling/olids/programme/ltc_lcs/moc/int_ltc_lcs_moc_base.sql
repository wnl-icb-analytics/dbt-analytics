{{ config(materialized='table') }}

-- LTC LCS Model of Care base population.
-- Union of all LTC LCS disease registers. Persons not on any of these registers
-- are excluded from downstream MOC / risk stratification reporting.

with register_population as (
    select person_id, 'AF' as condition from {{ ref('fct_person_atrial_fibrillation_register') }}
    union all
    select person_id, 'CKD' as condition from {{ ref('fct_person_ckd_register') }}
    union all
    select person_id, 'CHD' as condition from {{ ref('fct_person_chd_register') }}
    union all
    select person_id, 'Diabetes' as condition from {{ ref('fct_person_diabetes_register') }}
    union all
    select person_id, 'Hypertension' as condition from {{ ref('fct_person_hypertension_register') }}
    union all
    select person_id, 'NAFLD' as condition from {{ ref('int_ltc_lcs_nafld_register') }}
    union all
    select person_id, 'Asthma Adult' as condition from {{ ref('fct_person_asthma_register') }}
    union all
    select person_id, 'Asthma CYP' as condition from {{ ref('fct_person_cyp_asthma_register') }}
    union all
    select person_id, 'COPD' as condition from {{ ref('int_ltc_lcs_copd_register') }}
    union all
    select person_id, 'HF' as condition from {{ ref('fct_person_heart_failure_register') }}
    union all
    select person_id, 'PAD' as condition from {{ ref('fct_person_pad_register') }}
    union all
    select person_id, 'Stroke/TIA' as condition from {{ ref('fct_person_stroke_tia_register') }}
)

select
    person_id,
    count(distinct condition) as number_of_ltc_lcs_conditions
from register_population
group by person_id
