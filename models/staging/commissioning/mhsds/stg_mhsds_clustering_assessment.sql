{{ config(materialized='view', tags=['mhsds']) }}

with accepted as (
    {{ select_accepted_mhsds_period_records(ref('raw_mhsds_mhs801clustertool')) }}
)
select
    mhs801_uniq_id
    , person_id
    , uniq_clust_id
    , clust_cat
    , iff(ass_tool_comp_date::date >= '1901-01-01'::date, ass_tool_comp_date::date, null) as ass_tool_comp_date
    , ass_tool_comp_time::time as ass_tool_comp_time
    , clust_tool_ass_reason
    , mh_care_cluster_super_class
    , amh_care_clust_code_init
    , ld_care_clust_init
    , fld_care_clust_init
    , org_id_prov
    , uniq_submission_id
    , iff(reporting_period_start_date::date >= '1901-01-01'::date, reporting_period_start_date::date, null) as reporting_period_start_date
    , iff(reporting_period_end_date::date >= '1901-01-01'::date, reporting_period_end_date::date, null) as reporting_period_end_date
    , effective_from
    , dmic_dataset
from accepted
