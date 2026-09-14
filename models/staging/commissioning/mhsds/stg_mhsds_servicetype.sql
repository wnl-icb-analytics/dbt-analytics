{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with mhs102 as (
    {{
        select_latest_mhsds_record(
            mhsds_table = ref('raw_mhsds_mhs102servicetypereferredto'),
            partition_cols = ['uniq_serv_req_id'],
            tie_breaker_cols = ['mhs102_uniq_id']
        )
    }}
)

-- MHS902 team details: v6 source of the team type where neither MHS102 nor
-- MHS101 carries it (DMIC usually pre-derives this onto MHS102; the residual
-- is ~0.05% of referrals). Resolved across every active MHS102 row for the
-- referral, not just the deduplicated survivor, because the resolvable team
-- id can sit on an earlier submission's row.
, mhs902_resolved as (
    select
        m.uniq_serv_req_id
        , t.serv_team_type_mh
        , t.serv_team_int_age_group
    from {{ ref('raw_mhsds_mhs102servicetypereferredto') }} as m
    inner join {{ ref('raw_mhsds_activesubmission') }} as a
        on m.uniq_submission_id = a.uniq_submission_id
    -- v5 rows carry the team id in care_prof_team_local_id, v6 in
    -- other_care_prof_team_local_id
    inner join {{ ref('raw_mhsds_mhs902serviceteamdetails') }} as t
        on coalesce(m.care_prof_team_local_id, m.other_care_prof_team_local_id)
            = t.care_prof_team_local_id
        and m.uniq_submission_id = t.uniq_submission_id
    where t.serv_team_type_mh is not null
    qualify row_number() over (
        partition by m.uniq_serv_req_id
        order by
            m.reporting_period_end_date desc
            , m.effective_from desc
            , t.effective_from desc
            , m.mhs102_uniq_id desc
    ) = 1
)

select
    r.uniq_serv_req_id
    , r.person_id
    -- ~23% of referrals have no MHS102 row and carry the team type only on the
    -- v6 MHS101 ServTeamType field
    , coalesce(s.serv_team_type_ref_to_mh, t.serv_team_type_mh, r.serv_team_type) as serv_team_type_ref_to_mh
    , coalesce(s.serv_team_int_age_group, t.serv_team_int_age_group, r.serv_team_int_age_group) as serv_team_int_age_group
    , coalesce(s.service_type_name, r.service_type_name) as service_type_name
    , r.org_id_prov
    , r.uniq_submission_id
-- the referral interface already holds one row per uniq_serv_req_id, resolved
-- to the newest submitted version of each MHS101 referral
from {{ ref('stg_mhsds_referral') }} as r
left join mhs102 as s
    on r.uniq_serv_req_id = s.uniq_serv_req_id
left join mhs902_resolved as t
    on r.uniq_serv_req_id = t.uniq_serv_req_id
