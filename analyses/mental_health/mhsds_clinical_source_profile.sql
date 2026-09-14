{% set sources = [
    ('601', 'stg_mhsds_previous_diagnosis', 'prev_diag', 'coded_diag_timestamp', 'diag_date', 'local_patient_id'),
    ('603', 'stg_mhsds_provisional_diagnosis', 'prov_diag', 'coded_prov_diag_timestamp', 'prov_diag_date', 'uniq_serv_req_id'),
    ('604', 'stg_mhsds_primdiag', 'prim_diag', 'coded_diag_timestamp', 'diag_date', 'uniq_serv_req_id'),
    ('605', 'stg_mhsds_secondary_diagnosis', 'sec_diag', 'coded_diag_timestamp', 'diag_date', 'uniq_serv_req_id'),
    ('606', 'stg_mhsds_referral_assessment', 'coded_ass_tool_type', 'ass_tool_comp_timestamp', 'ass_tool_comp_date', 'uniq_serv_req_id'),
    ('607', 'stg_mhsds_activity_assessment', 'coded_ass_tool_type', none, none, 'uniq_care_act_id')
] %}

{% for number, model, code, timestamp, date, parent in sources %}
select
    'MHS{{ number }}' as source_table
    , iff(grouping(a.dmic_dataset) = 1, 'all_versions',
        coalesce(a.dmic_dataset, 'unknown_version')) as source_version
    , count(*) as accepted_rows
    , count(distinct a.mhs{{ number }}_uniq_id) as distinct_source_ids
    , count_if(a.{{ code }} is null) as missing_codes
    , count_if(a.{{ parent }} is null) as missing_parent_ids
    , count_if(a.person_id is null) as missing_person_ids
    {% if timestamp %}
    , count_if(a.{{ timestamp }} is null) as missing_timestamps
    , count_if(a.{{ date }} is null) as missing_dates
    , count_if(a.{{ timestamp }} is null and a.{{ date }} is not null) as date_only_rows
    , count_if(a.{{ timestamp }}::date <> a.{{ date }}::date) as date_disagreements
    , count_if(a.{{ timestamp }}::date > a.reporting_period_end_date::date) as after_reporting_period
    {% else %}
    , null as missing_timestamps
    , null as missing_dates
    , null as date_only_rows
    , null as date_disagreements
    , null as after_reporting_period
    {% endif %}
    , min(a.reporting_period_start_date)::date as first_period
    , max(a.reporting_period_end_date)::date as last_period
from {{ ref(model) }} as a
group by grouping sets ((a.dmic_dataset), ())
{% if not loop.last %}union all{% endif %}
{% endfor %}
