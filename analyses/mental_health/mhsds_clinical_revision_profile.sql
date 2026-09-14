select 'MHS601' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , null as changed_score_keys
from (
    select org_id_prov, local_patient_id, diag_scheme_in_use, prev_diag, coded_diag_timestamp
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
    from {{ ref('stg_mhsds_previous_diagnosis') }}
    group by org_id_prov, local_patient_id, diag_scheme_in_use, prev_diag, coded_diag_timestamp
)

union all

select 'MHS603' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , null as changed_score_keys
from (
    select org_id_prov, uniq_serv_req_id, diag_scheme_in_use, prov_diag, coded_prov_diag_timestamp
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
    from {{ ref('stg_mhsds_provisional_diagnosis') }}
    group by org_id_prov, uniq_serv_req_id, diag_scheme_in_use, prov_diag, coded_prov_diag_timestamp
)

union all

select 'MHS604' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , null as changed_score_keys
from (
    select org_id_prov, uniq_serv_req_id, diag_scheme_in_use, prim_diag, coded_diag_timestamp
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
    from {{ ref('stg_mhsds_primdiag') }}
    group by org_id_prov, uniq_serv_req_id, diag_scheme_in_use, prim_diag, coded_diag_timestamp
)

union all

select 'MHS605' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , null as changed_score_keys
from (
    select org_id_prov, uniq_serv_req_id, diag_scheme_in_use, sec_diag, coded_diag_timestamp
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
    from {{ ref('stg_mhsds_secondary_diagnosis') }}
    group by org_id_prov, uniq_serv_req_id, diag_scheme_in_use, sec_diag, coded_diag_timestamp
)

union all

select 'MHS606' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , count_if(scores > 1) as changed_score_keys
from (
    select org_id_prov, uniq_serv_req_id, coded_ass_tool_type, ass_tool_comp_timestamp
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
        , count(distinct pers_score) as scores
    from {{ ref('stg_mhsds_referral_assessment') }}
    group by org_id_prov, uniq_serv_req_id, coded_ass_tool_type, ass_tool_comp_timestamp
)

union all

select 'MHS607' as source_table
    , count(*) as candidate_key_count
    , sum(n) as accepted_rows
    , count_if(n > 1) as repeated_keys
    , count_if(periods > 1) as cross_period_keys
    , count_if(persons > 1) as changed_person_keys
    , count_if(scores > 1) as changed_score_keys
from (
    select org_id_prov, uniq_care_act_id, coded_ass_tool_type
        , count(*) as n
        , count(distinct reporting_period_end_date) as periods
        , count(distinct person_id) as persons
        , count(distinct pers_score) as scores
    from {{ ref('stg_mhsds_activity_assessment') }}
    group by org_id_prov, uniq_care_act_id, coded_ass_tool_type
)
