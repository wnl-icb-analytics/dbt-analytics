{{
    config(
        materialized = 'table',
        tags = ['mhsds']
    )
}}

with latest_spell as (
    {{
        select_latest_mhsds_record(
            mhsds_table = ref('raw_mhsds_mhs501hospprovspell'),
            partition_cols = ['uniq_hosp_prov_spell_num'],
            tie_breaker_cols = ['mhs501_uniq_id']
        )
    }}
)

select
    spell.uniq_hosp_prov_spell_num
    , spell.uniq_hosp_prov_spell_id
    , spell.mhs501_uniq_id
    , spell.hosp_prov_spell_id
    , spell.hosp_prov_spell_num
    , spell.service_request_id
    , spell.uniq_serv_req_id
    , spell.person_id
    , iff(
        spell.start_date_hosp_prov_spell::date < '1901-01-01'::date
        , null
        , spell.start_date_hosp_prov_spell::date
    ) as start_date_hosp_prov_spell
    , spell.start_time_hosp_prov_spell::time as start_time_hosp_prov_spell
    , iff(
        spell.decided_to_admit_date::date < '1901-01-01'::date
        , null
        , spell.decided_to_admit_date::date
    ) as decided_to_admit_date
    , spell.decided_to_admit_time::time as decided_to_admit_time
    , spell.source_adm_code_hosp_prov_spell
    , spell.source_adm_mh_hosp_prov_spell
    , spell.adm_meth_code_hosp_prov_spell
    , spell.meth_adm_mh_hosp_prov_spell
    , iff(
        spell.estimated_disch_date_hosp_prov_spell::date < '1901-01-01'::date
        , null
        , spell.estimated_disch_date_hosp_prov_spell::date
    ) as estimated_disch_date_hosp_prov_spell
    , iff(
        spell.planned_disch_date_hosp_prov_spell::date < '1901-01-01'::date
        , null
        , spell.planned_disch_date_hosp_prov_spell::date
    ) as planned_disch_date_hosp_prov_spell
    , spell.planned_disch_dest_code
    , spell.planned_dest_disch
    , iff(
        spell.disch_date_hosp_prov_spell::date < '1901-01-01'::date
        , null
        , spell.disch_date_hosp_prov_spell::date
    ) as disch_date_hosp_prov_spell
    , spell.disch_time_hosp_prov_spell::time as disch_time_hosp_prov_spell
    , spell.disch_meth_code_hosp_prov_spell
    , spell.meth_of_disch_mh_hosp_prov_spell
    , spell.disch_dest_code_hosp_prov_spell
    , spell.dest_of_disch_hosp_prov_spell
    , spell.org_id_prov
    , spell.dmic_ccg_code as dm_icb_commissioner
    , spell.age_hosp_start_date
    , spell.age_hosp_disch_date
    , spell.record_start_date::date as record_start_date
    , spell.record_end_date::date as record_end_date
    , spell.uniq_submission_id
    , spell.uniq_month_id
    , spell.reporting_period_start_date::date as reporting_period_start_date
    , spell.reporting_period_end_date::date as reporting_period_end_date
    , submission.dat_set_ver
    , spell.dmic_dataset
    , spell.effective_from
    , spell.dmic_date_added
from latest_spell as spell
left join {{ ref('stg_mhsds_activesubmission') }} as submission
    on spell.uniq_submission_id = submission.uniq_submission_id
