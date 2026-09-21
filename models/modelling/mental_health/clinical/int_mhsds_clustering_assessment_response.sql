with keyed as (
    select
        a.*
        , {{ dbt_utils.generate_surrogate_key(['a.org_id_prov','a.uniq_clust_id','a.coded_ass_tool_type',
            "iff(a.org_id_prov is null or a.uniq_clust_id is null or a.coded_ass_tool_type is null, a.mhs802_uniq_id, null)"]) }} as source_record_id
    from {{ ref('stg_mhsds_clustering_assessment_response') }} as a
)
, latest as (
    select
        a.*
        , min(reporting_period_end_date) over (partition by source_record_id) as first_reported_period_end_date
        , max(reporting_period_end_date) over (partition by source_record_id) as last_reported_period_end_date
        , count(*) over (partition by source_record_id) as accepted_source_record_count
        , count(distinct reporting_period_end_date) over (partition by source_record_id) as reported_period_count
        , count(distinct person_id) over (partition by source_record_id) > 1 as has_person_identifier_changed
    from keyed as a
    qualify row_number() over (
        partition by source_record_id order by reporting_period_end_date desc,
            effective_from desc nulls last, uniq_submission_id desc, mhs802_uniq_id desc
    ) = 1
)
select
    a.*
    , p.mhs801_uniq_id is not null as is_assessment_parent_linked
    , a.person_id = p.person_id as is_assessment_parent_person_consistent
    , iff(a.person_id is not distinct from p.person_id, p.ass_tool_comp_date, null) as assessment_date
    , iff(a.person_id is not distinct from p.person_id, p.ass_tool_comp_time, null) as assessment_time
from latest as a
left join {{ ref('stg_mhsds_clustering_assessment') }} as p
    on a.uniq_submission_id = p.uniq_submission_id and a.uniq_clust_id = p.uniq_clust_id
