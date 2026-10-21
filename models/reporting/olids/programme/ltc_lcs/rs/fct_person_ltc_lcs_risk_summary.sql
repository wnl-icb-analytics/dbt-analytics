{{ config(materialized='table', cluster_by=['overall_risk_rank']) }}

{% set fy_start = "date_from_parts(case when month(current_date()) >= 4 then year(current_date()) else year(current_date()) - 1 end, 4, 1)" %}

-- One row per adult MoC member (including Check & Test-only LR members) or CYP-only asthma member, assessed today.
with event_dates as (
    select
        person_id,
        max(case when event_code = 'C' then event_date end) as latest_check_test_date,
        max(case when event_code = 'M' then event_date end) as latest_mdt_review_date,
        max(case when event_code = 'S' then event_date end) as latest_careplan_sharing_date,
        max(case when event_code = 'A' then event_date end) as latest_discussion_date,
        max(case when event_code = 'R' then event_date end) as latest_followup_date,
        max(case when event_code = 'D' then event_date end) as latest_declined_date,
        max(case when event_code = 'O' then event_date end) as latest_offered_date,
        max_by(
            case when event_code in ('C','S','A','R','D') and event_date >= dateadd(month, -12, current_date()) then event_code end,
            case when event_code in ('C','S','A','R','D') and event_date >= dateadd(month, -12, current_date()) then to_char(event_date, 'YYYYMMDD') || event_code end
        ) as latest_e_code,
        max_by(
            case when event_code in ('C','S','A','R','D','O') and event_date >= dateadd(month, -12, current_date()) then event_code end,
            case when event_code in ('C','S','A','R','D','O') and event_date >= dateadd(month, -12, current_date()) then to_char(event_date, 'YYYYMMDD') || event_code end
        ) as latest_e_plus_code,
        max_by(
            case when event_code in ('C','A','R','D','O') then event_code end,
            case when event_code in ('C','A','R','D','O') then to_char(event_date, 'YYYYMMDD') || event_code end
        ) as latest_b_code
    -- EMIS exports no same-day event order. Event code descending is a stable tie-break.
    from {{ ref('int_ltc_lcs_moc_events') }}
    group by person_id
),
population as (
    select
        p.person_id,
        p.number_of_ltc_lcs_conditions,
        p.is_in_moc_base,
        p.moc_base_exclusion_reason,
        p.is_cyp_asthma_only,
        p.is_moc_check_test_only,
        -- Full adult pathway; Check & Test-only members are in the MoC base but skip the later stages.
        p.is_in_moc_base and not p.is_moc_check_test_only as is_full_moc_pathway,
        rs.af_risk_group,
        rs.asthma_adult_risk_group,
        case when p.is_cyp_asthma_only then coalesce(cyp.risk_group, 'LR')
            else rs.asthma_cyp_risk_group end as asthma_cyp_risk_group,
        rs.chd_risk_group,
        rs.ckd_risk_group,
        rs.copd_risk_group,
        rs.diabetes_risk_group,
        rs.hf_risk_group,
        rs.hypertension_risk_group,
        rs.nafld_risk_group,
        rs.pad_risk_group,
        rs.stroke_tia_risk_group,
        coalesce(rs.is_type_1_diabetes_hrc, false) as is_type_1_diabetes_hrc,
        -- GROUP4 is the eligible adult MoC base minus HRC, HR and MR.
        coalesce(rs.overall_risk_group, cyp.risk_group, 'LR') as overall_risk_group,
        coalesce(rs.overall_risk_rank, case when cyp.person_id is not null then 2 else 5 end) as overall_risk_rank,
        (rs.person_id is not null or cyp.person_id is not null or p.is_cyp_asthma_only) as in_any_risk_group,
        -- Four risk pathways; CYP-only asthma rows follow HRS or LRS by CYP risk.
        case when p.is_cyp_asthma_only then case coalesce(cyp.risk_group, 'LR') when 'HR' then 'HRS' else 'LRS' end
            else case coalesce(rs.overall_risk_group, 'LR')
                when 'HRC' then 'HRCS' when 'HR' then 'HRS' when 'MR' then 'MRS' else 'LRS'
            end end as moc_pathway,
        ev.latest_check_test_date,
        ev.latest_mdt_review_date,
        ev.latest_careplan_sharing_date,
        ev.latest_discussion_date,
        ev.latest_followup_date,
        ev.latest_declined_date,
        ev.latest_offered_date,
        ev.latest_e_code,
        ev.latest_e_plus_code,
        ev.latest_b_code,
        coalesce(p.is_cyp_asthma_only and cypr.annual_review_completed, false) as cyp_annual_review_completed,
        coalesce(p.is_cyp_asthma_only and cyp.person_id is not null and cypr.desktop_review_completed, false) as cyp_desktop_review_completed,
        case when p.is_cyp_asthma_only then cypr.annual_review_date end as cyp_annual_review_date,
        case when p.is_cyp_asthma_only and cyp.person_id is not null then cypr.desktop_review_completed_date end as cyp_desktop_review_date,
        greatest_ignore_nulls(cypr.annual_review_date, cypr.desktop_review_date) as cyp_latest_review_date
    from {{ ref('int_ltc_lcs_moc_base') }} p
    left join {{ ref('int_ltc_lcs_rs_person_risk_summary') }} rs on p.person_id = rs.person_id
    left join {{ ref('int_ltc_lcs_rs_acyp_pg2_hr') }} cyp
        on p.person_id = cyp.person_id and p.is_cyp_asthma_only
    left join event_dates ev on p.person_id = ev.person_id
    left join {{ ref('int_ltc_lcs_moc_cyp_review') }} cypr on p.person_id = cypr.person_id
    where p.is_in_moc_base or p.is_cyp_asthma_only
),
activity as (
    select
        p.*,
        case overall_risk_group
            when 'HRC' then 'High Risk + Complex' when 'HR' then 'High Risk'
            when 'MR' then 'Medium Risk' else 'Low Risk'
        end as moc_risk_category,
        -- All MoC base members share the adult C&T rule; Check & Test-only members skip the later stages.
        coalesce(is_in_moc_base and (latest_check_test_date > dateadd(year, -1, current_date()) or (moc_pathway = 'LRS' and latest_check_test_date = dateadd(year, -1, current_date()))), false) as moc_check_test_completed,
        case when moc_check_test_completed then latest_check_test_date end as moc_check_test_date,
        coalesce(is_full_moc_pathway and (latest_mdt_review_date >= dateadd(month, -12, current_date())), false) as moc_mdt_review_completed,
        case when moc_mdt_review_completed then latest_mdt_review_date end as moc_mdt_review_date,
        coalesce(is_full_moc_pathway and (latest_careplan_sharing_date > dateadd(year, -1, current_date()) or (moc_pathway = 'LRS' and latest_careplan_sharing_date = dateadd(year, -1, current_date()))), false) as moc_careplan_sharing_completed,
        case when moc_careplan_sharing_completed then latest_careplan_sharing_date end as moc_careplan_sharing_date,
        coalesce(is_full_moc_pathway and (latest_discussion_date >= dateadd(month, -12, current_date())), false) as moc_discussion_completed,
        case when moc_discussion_completed then latest_discussion_date end as moc_discussion_date,
        coalesce(is_full_moc_pathway and (latest_followup_date >= dateadd(month, -12, current_date())), false) as moc_followup_completed,
        case when moc_followup_completed then latest_followup_date end as moc_followup_date,
        coalesce(is_full_moc_pathway and (latest_declined_date >= dateadd(month, -12, current_date())), false) as moc_declined,
        case when moc_declined then latest_declined_date end as moc_declined_date,
        (is_cyp_asthma_only and not cyp_annual_review_completed) as cyp_requires_annual_review,
        -- Project rule from the EMIS HR description (the exported search selects no one): desktop review
        -- due 3 months after the latest annual or desktop review, once the annual review is done.
        coalesce(is_cyp_asthma_only and moc_pathway = 'HRS' and cyp_annual_review_completed
            and not cyp_desktop_review_completed
            and cyp_latest_review_date <= dateadd(month, -3, current_date()), false) as cyp_requires_desktop_review,
        coalesce(latest_e_code = 'D', false) as moc_declined_is_latest
    from population p
),
stages as (
    select
        a.*,
        moc_check_test_completed as moc_stage_1_completed,
        coalesce(is_full_moc_pathway and latest_check_test_date > dateadd(year, -1, current_date())
            and (moc_careplan_sharing_completed
                or (moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed)), false) as moc_stage_2_completed,
        (is_full_moc_pathway and (moc_declined or moc_discussion_completed)) as moc_stage_3_completed,
        (is_full_moc_pathway and (moc_declined or (moc_discussion_completed and moc_followup_completed))) as moc_stage_4_completed,
        case
            when is_cyp_asthma_only then false
            when is_moc_check_test_only then not moc_check_test_completed
            when latest_check_test_date > dateadd(year, -1, current_date()) then false
            when latest_check_test_date < dateadd(year, -1, current_date()) then true
            when latest_e_code in ('A','D','C','S')
                and greatest_ignore_nulls(latest_discussion_date, latest_declined_date,
                    latest_check_test_date, latest_careplan_sharing_date) > dateadd(year, -1, current_date()) then false
            when latest_check_test_date is not null then false
            when greatest_ignore_nulls(latest_followup_date, latest_discussion_date,
                latest_declined_date, latest_check_test_date) > dateadd(year, -1, current_date()) then false
            else true
        end as moc_requires_check_test,
        coalesce(is_full_moc_pathway and latest_e_code = 'C'
            and latest_check_test_date > dateadd(year, -1, current_date()), false) as moc_requires_stage_2,
        coalesce(is_full_moc_pathway and latest_e_code = 'S', false) as moc_requires_discussion,
        coalesce(is_full_moc_pathway and latest_e_code not in ('D','R','C','S')
            and latest_e_plus_code = 'A', false) as moc_requires_followup,
        coalesce(is_full_moc_pathway and latest_check_test_date > dateadd(month, -12, current_date())
            and latest_b_code = 'C' and latest_check_test_date < dateadd(day, -42, current_date()), false) as moc_ct_overdue,
        coalesce(is_full_moc_pathway and (latest_check_test_date is null or latest_check_test_date > dateadd(month, -12, current_date()))
            and latest_b_code = 'A' and latest_discussion_date < dateadd(month, -3, current_date()), false) as moc_discussion_overdue
    from activity a
),
work_lists as (
    select
        s.*,
        (moc_requires_stage_2 and moc_pathway in ('HRCS','HRS')
            and not coalesce(latest_mdt_review_date > dateadd(month, -12, current_date()), false)) as moc_requires_mdt_review,
        (moc_requires_stage_2 and (moc_pathway in ('MRS','LRS')
            or (coalesce(latest_mdt_review_date > dateadd(month, -12, current_date()), false)
                and not coalesce(latest_careplan_sharing_date >= dateadd(month, -12, current_date()), false)))) as moc_requires_careplan_sharing
    from stages s
),
progression as (
    select
        w.*,
        (moc_requires_check_test or moc_requires_stage_2 or moc_requires_discussion
            or moc_requires_followup or cyp_requires_annual_review or cyp_requires_desktop_review) as moc_has_work_list,
        coalesce(is_full_moc_pathway and moc_discussion_completed and moc_followup_completed
            and latest_e_code = 'R' and not moc_has_work_list, false)
            -- CYP-only asthma: annual review, plus desktop review for HR.
            or (is_cyp_asthma_only and cyp_annual_review_completed
                and (moc_pathway = 'LRS' or cyp_desktop_review_completed))
            or (is_moc_check_test_only and moc_check_test_completed) as moc_cycle_complete,
        -- Existing status vocabulary; work-list detail is in moc_requires_* and moc_next_action.
        case
            when is_full_moc_pathway and moc_declined_is_latest then 'Declined'
            when moc_cycle_complete then 'Cycle complete'
            when moc_check_test_completed or moc_mdt_review_completed or moc_careplan_sharing_completed
                or moc_discussion_completed or moc_followup_completed
                or cyp_annual_review_completed or cyp_desktop_review_completed then 'In progress'
            else 'Not started'
        end as moc_pathway_status,
        case
            when cyp_requires_annual_review then 'Annual Review'
            when cyp_requires_desktop_review then 'Desktop Review'
            when moc_requires_check_test then 'Check & Test'
            when moc_requires_mdt_review then 'MDT Review'
            when moc_requires_careplan_sharing then 'Care Plan Sharing'
            -- Main EMIS stage 2 list without a sublist match (MDT and sharing both recent, newer C&T).
            when moc_requires_stage_2 then 'Care Plan Sharing'
            when moc_requires_discussion then 'Discussion'
            when moc_requires_followup then 'Follow-up'
        end as moc_next_action,
        case moc_next_action
            when 'Annual Review' then 'AR' when 'Desktop Review' then 'DTR' when 'Check & Test' then 'C&T'
            when 'MDT Review' then 'MDT' when 'Care Plan Sharing' then 'CPS'
            when 'Discussion' then 'DISC' when 'Follow-up' then 'F/U'
        end as moc_next_action_code,
        case
            when moc_discussion_completed and moc_followup_completed then 4
            when moc_discussion_completed then 3
            when moc_stage_2_completed then 2 when moc_stage_1_completed then 1 else 0
        end as moc_stage_completed,
        case
            when is_cyp_asthma_only and cyp_annual_review_completed then 'Annual Review'
            when is_cyp_asthma_only and cyp_desktop_review_completed then 'Desktop Review'
            when moc_discussion_completed and moc_followup_completed then 'Follow-up'
            when moc_discussion_completed then 'Discussion'
            when moc_stage_2_completed and moc_careplan_sharing_completed then 'Care Plan Sharing'
            when moc_stage_2_completed then 'MDT Review'
            when moc_stage_1_completed then 'Check & Test' else 'Not started'
        end as moc_stage_completed_label,
        case moc_stage_completed_label
            when 'Annual Review' then 'AR' when 'Desktop Review' then 'DTR'
            when 'Follow-up' then 'F/U' when 'Discussion' then 'DISC' when 'Care Plan Sharing' then 'CPS'
            when 'MDT Review' then 'MDT' when 'Check & Test' then 'C&T'
        end as moc_stage_completed_code,
        case when moc_stage_2_completed then
            case when moc_pathway in ('HRCS','HRS') then greatest_ignore_nulls(moc_mdt_review_date, moc_careplan_sharing_date)
                else moc_careplan_sharing_date end
        end as moc_stage_2_date,
        -- Gap flags use stage 2 activity; EMIS stage 2 completion already requires C&T.
        (is_full_moc_pathway and (moc_discussion_completed or moc_followup_completed or moc_careplan_sharing_completed
            or (moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed))
            and not moc_check_test_completed) as moc_missing_check_test,
        (is_full_moc_pathway and (moc_discussion_completed or moc_followup_completed)
            and not (moc_careplan_sharing_completed or (moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed)))
            as moc_missing_stage_2,
        (moc_followup_completed and not moc_discussion_completed) as moc_missing_discussion,
        (moc_missing_check_test or moc_missing_stage_2 or moc_missing_discussion) as moc_has_missing_priors
    from work_lists w
)
select
    person_id,
    number_of_ltc_lcs_conditions,
    is_in_moc_base,
    moc_base_exclusion_reason,
    is_cyp_asthma_only,
    is_moc_check_test_only,
    is_type_1_diabetes_hrc,
    af_risk_group,
    asthma_adult_risk_group,
    asthma_cyp_risk_group,
    chd_risk_group,
    ckd_risk_group,
    copd_risk_group,
    diabetes_risk_group,
    hf_risk_group,
    hypertension_risk_group,
    nafld_risk_group,
    pad_risk_group,
    stroke_tia_risk_group,
    overall_risk_group,
    overall_risk_rank,
    in_any_risk_group,
    moc_pathway,
    moc_risk_category,
    moc_check_test_completed,
    moc_check_test_date,
    moc_mdt_review_completed,
    moc_mdt_review_date,
    moc_careplan_sharing_completed,
    moc_careplan_sharing_date,
    moc_discussion_completed,
    moc_discussion_date,
    moc_followup_completed,
    moc_followup_date,
    moc_declined,
    moc_declined_date,
    -- Retained for consumers: annual review event in the last 12 months, any pathway.
    -- No longer a MoC stage; MR/LR stage 2 is care plan sharing.
    moc_mdt_review_completed as moc_remote_desktop_review_completed,
    moc_mdt_review_date as moc_remote_desktop_review_date,
    coalesce(moc_declined and greatest_ignore_nulls(latest_check_test_date, latest_careplan_sharing_date,
        latest_discussion_date, latest_followup_date) > moc_declined_date, false) as moc_re_engaged_after_decline,
    cyp_annual_review_completed,
    cyp_requires_annual_review,
    cyp_requires_desktop_review,
    cyp_desktop_review_completed,
    cyp_annual_review_date,
    cyp_desktop_review_date,
    (moc_check_test_completed) as moc_check_test_completed_12m,
    case when moc_check_test_completed then moc_check_test_date end as moc_check_test_date_12m,
    coalesce(moc_check_test_completed and moc_check_test_date >= {{ fy_start }}, false) as moc_check_test_completed_fy,
    case when moc_check_test_date >= {{ fy_start }} then moc_check_test_date end as moc_check_test_date_fy,
    (moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed) as moc_mdt_review_completed_12m,
    case when moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed then moc_mdt_review_date end as moc_mdt_review_date_12m,
    coalesce(moc_pathway in ('HRCS','HRS') and moc_mdt_review_completed and moc_mdt_review_date >= {{ fy_start }}, false) as moc_mdt_review_completed_fy,
    case when moc_pathway in ('HRCS','HRS') and moc_mdt_review_date >= {{ fy_start }} then moc_mdt_review_date end as moc_mdt_review_date_fy,
    (moc_pathway in ('MRS','LRS') and moc_mdt_review_completed) as moc_remote_desktop_review_completed_12m,
    case when moc_pathway in ('MRS','LRS') and moc_mdt_review_completed then moc_mdt_review_date end as moc_remote_desktop_review_date_12m,
    coalesce(moc_pathway in ('MRS','LRS') and moc_mdt_review_completed and moc_mdt_review_date >= {{ fy_start }}, false) as moc_remote_desktop_review_completed_fy,
    case when moc_pathway in ('MRS','LRS') and moc_mdt_review_date >= {{ fy_start }} then moc_mdt_review_date end as moc_remote_desktop_review_date_fy,
    (moc_careplan_sharing_completed) as moc_careplan_sharing_completed_12m,
    case when moc_careplan_sharing_completed then moc_careplan_sharing_date end as moc_careplan_sharing_date_12m,
    coalesce(moc_careplan_sharing_completed and moc_careplan_sharing_date >= {{ fy_start }}, false) as moc_careplan_sharing_completed_fy,
    case when moc_careplan_sharing_date >= {{ fy_start }} then moc_careplan_sharing_date end as moc_careplan_sharing_date_fy,
    (moc_discussion_completed) as moc_discussion_completed_12m,
    case when moc_discussion_completed then moc_discussion_date end as moc_discussion_date_12m,
    coalesce(moc_discussion_completed and moc_discussion_date >= {{ fy_start }}, false) as moc_discussion_completed_fy,
    case when moc_discussion_date >= {{ fy_start }} then moc_discussion_date end as moc_discussion_date_fy,
    (moc_followup_completed) as moc_followup_completed_12m,
    case when moc_followup_completed then moc_followup_date end as moc_followup_date_12m,
    coalesce(moc_followup_completed and moc_followup_date >= {{ fy_start }}, false) as moc_followup_completed_fy,
    case when moc_followup_date >= {{ fy_start }} then moc_followup_date end as moc_followup_date_fy,
    (moc_check_test_completed or moc_mdt_review_completed or moc_careplan_sharing_completed
        or moc_discussion_completed or moc_followup_completed or moc_declined
        or cyp_annual_review_completed or cyp_desktop_review_completed) as moc_any_activity_12m,
    (coalesce(moc_check_test_date >= {{ fy_start }}, false)
        or coalesce(moc_mdt_review_date >= {{ fy_start }}, false)
        or coalesce(moc_careplan_sharing_date >= {{ fy_start }}, false)
        or coalesce(moc_discussion_date >= {{ fy_start }}, false)
        or coalesce(moc_followup_date >= {{ fy_start }}, false)
        or coalesce(moc_declined_date >= {{ fy_start }}, false)
        or coalesce(cyp_annual_review_date >= {{ fy_start }}, false)
        or coalesce(cyp_desktop_review_date >= {{ fy_start }}, false)) as moc_any_activity_fy,
    moc_stage_1_completed,
    moc_stage_2_completed,
    moc_stage_3_completed,
    moc_stage_4_completed,
    moc_requires_check_test,
    moc_requires_stage_2,
    moc_requires_mdt_review,
    moc_requires_careplan_sharing,
    moc_requires_discussion,
    moc_requires_followup,
    moc_ct_overdue,
    moc_discussion_overdue,
    moc_stage_completed,
    moc_stage_completed_label,
    moc_stage_completed_code,
    moc_pathway_status,
    moc_next_action,
    moc_next_action_code,
    moc_cycle_complete,
    moc_missing_check_test,
    moc_missing_stage_2,
    moc_missing_discussion,
    moc_has_missing_priors,
    moc_stage_2_date,
    datediff(day, moc_check_test_date, moc_stage_2_date) as moc_days_check_test_to_stage_2,
    datediff(day, moc_stage_2_date, moc_discussion_date) as moc_days_stage_2_to_discussion,
    datediff(day, moc_discussion_date, moc_followup_date) as moc_days_discussion_to_followup,
    datediff(day, moc_check_test_date, moc_followup_date) as moc_days_check_test_to_followup,
    {{ ltc_lcs_activity_last_valid_date('moc_discussion_date', inclusive=true) }} as moc_careplan_expires_date,
    least_ignore_nulls(
        case when moc_pathway = 'LRS' then {{ ltc_lcs_activity_last_valid_date('moc_check_test_date', inclusive=true) }} else {{ ltc_lcs_activity_last_valid_date('moc_check_test_date') }} end,
        case when moc_pathway in ('HRCS','HRS') then {{ ltc_lcs_activity_last_valid_date('moc_mdt_review_date', inclusive=true) }} end,
        case when moc_pathway = 'LRS' then {{ ltc_lcs_activity_last_valid_date('moc_careplan_sharing_date', inclusive=true) }} else {{ ltc_lcs_activity_last_valid_date('moc_careplan_sharing_date') }} end,
        {{ ltc_lcs_activity_last_valid_date('moc_discussion_date', inclusive=true) }},
        {{ ltc_lcs_activity_last_valid_date('moc_followup_date', inclusive=true) }},
        {{ ltc_lcs_activity_last_valid_date('moc_declined_date', inclusive=true) }},
        case when cyp_annual_review_completed then {{ ltc_lcs_activity_last_valid_date('cyp_annual_review_date') }} end,
        case when cyp_desktop_review_completed then {{ ltc_lcs_activity_last_valid_date('cyp_desktop_review_date') }} end
    ) as moc_next_expiry_date,
    current_timestamp()::timestamp_ntz as table_refresh_date
from progression
