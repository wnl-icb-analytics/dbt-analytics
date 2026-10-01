{{
    config(
        materialized='table',
        cluster_by=['overall_risk_rank']
    )
}}

{% set fy_start = "date_from_parts(case when month(current_date()) >= 4 then year(current_date()) else year(current_date()) - 1 end, 4, 1)" %}

-- LTC LCS person-level risk stratification and Model of Care activity summary.
-- One row per person on the LTC LCS MOC base population (any LTC LCS disease register)
-- with their risk group status per condition, MOC activity flags for the last 12 months,
-- and pathway progression (which stage is complete, what is next) aligned to the EMIS
-- HRCS / HRS / MRS / LRS pathway families.

with moc_base as (
    select person_id, number_of_ltc_lcs_conditions
    from {{ ref('int_ltc_lcs_moc_base') }}
),

rs as (
    select * from {{ ref('int_ltc_lcs_rs_person_risk_summary') }}
),

moc_check_test as (
    select person_id, latest_completed_date, check_test_completed
    from {{ ref('int_ltc_lcs_moc_check_test') }}
),

moc_review as (
    select person_id, latest_completed_date, review_completed
    from {{ ref('int_ltc_lcs_moc_review') }}
),

moc_careplan_sharing as (
    select person_id, latest_completed_date, careplan_sharing_completed
    from {{ ref('int_ltc_lcs_moc_careplan_sharing') }}
),

moc_discussion as (
    select person_id, latest_completed_date, discussion_completed
    from {{ ref('int_ltc_lcs_moc_discussion') }}
),

moc_followup as (
    select person_id, latest_completed_date, followup_completed
    from {{ ref('int_ltc_lcs_moc_followup') }}
),

moc_declined as (
    select person_id, latest_declined_date, moc_declined
    from {{ ref('int_ltc_lcs_moc_declined') }}
),

joined as (
    select
        p.person_id,
        p.number_of_ltc_lcs_conditions,
        rs.af_risk_group,
        rs.asthma_adult_risk_group,
        rs.asthma_cyp_risk_group,
        rs.chd_risk_group,
        rs.ckd_risk_group,
        rs.copd_risk_group,
        rs.diabetes_risk_group,
        rs.hf_risk_group,
        rs.hypertension_risk_group,
        rs.nafld_risk_group,
        rs.pad_risk_group,
        rs.stroke_tia_risk_group,
        -- overall_risk_group: falls back to LR for MOC base members not stratified higher
        -- (mirrors EMIS [GROUP4- LR] = MOC base ∖ (HRC ∪ HR ∪ MR))
        coalesce(rs.overall_risk_group, 'LR') as overall_risk_group,
        coalesce(rs.overall_risk_rank, 5) as overall_risk_rank,
        (rs.person_id is not null) as in_any_risk_group,

        coalesce(ct.check_test_completed, false) as moc_check_test_completed,
        ct.latest_completed_date as moc_check_test_date,
        coalesce(rev.review_completed, false) as moc_remote_desktop_review_completed,
        rev.latest_completed_date as moc_remote_desktop_review_date,
        coalesce(cps.careplan_sharing_completed, false) as moc_careplan_sharing_completed,
        cps.latest_completed_date as moc_careplan_sharing_date,
        coalesce(dsc.discussion_completed, false) as moc_discussion_completed,
        dsc.latest_completed_date as moc_discussion_date,
        coalesce(fu.followup_completed, false) as moc_followup_completed,
        fu.latest_completed_date as moc_followup_date,
        coalesce(dec.moc_declined, false) as moc_declined,
        dec.latest_declined_date as moc_declined_date
    from moc_base p
    left join rs on p.person_id = rs.person_id
    left join moc_check_test ct on p.person_id = ct.person_id
    left join moc_review rev on p.person_id = rev.person_id
    left join moc_careplan_sharing cps on p.person_id = cps.person_id
    left join moc_discussion dsc on p.person_id = dsc.person_id
    left join moc_followup fu on p.person_id = fu.person_id
    left join moc_declined dec on p.person_id = dec.person_id
),

with_pathway as (
    select
        j.*,
        -- MOC pathway family: which EMIS search series applies to this person
        -- HRC → HRCS, HR → HRS, MR/MRa/MRb → MRS, LR → LRS
        case upper(j.overall_risk_group)
            when 'HRC' then 'HRCS'
            when 'HR'  then 'HRS'
            when 'MR'  then 'MRS'
            when 'LR'  then 'LRS'
        end as moc_pathway,
        case upper(j.overall_risk_group)
            when 'HRC' then 'High Risk + Complex'
            when 'HR'  then 'High Risk'
            when 'MR'  then 'Medium Risk'
            when 'LR'  then 'Low Risk'
        end as moc_risk_category,
        -- HRCS/HRS stage 2 has MDT Review (2A) and Care Plan Sharing (2B).
        -- MRS/LRS stage 2 is Remote Desktop Review.
        case
            when upper(j.overall_risk_group) in ('HRC', 'HR')
                then (j.moc_remote_desktop_review_completed or j.moc_careplan_sharing_completed)
            else j.moc_remote_desktop_review_completed
        end as moc_stage_2_reached,
        case
            when upper(j.overall_risk_group) in ('HRC', 'HR')
                then (j.moc_remote_desktop_review_completed and j.moc_careplan_sharing_completed)
            else j.moc_remote_desktop_review_completed
        end as moc_stage_2_completed,
        -- Stage 2 completes on the later HRCS/HRS substage date.
        case
            when upper(j.overall_risk_group) in ('HRC', 'HR') then
                case
                    when j.moc_remote_desktop_review_date is null
                        or j.moc_careplan_sharing_date is null then null
                    when j.moc_remote_desktop_review_date >= j.moc_careplan_sharing_date
                        then j.moc_remote_desktop_review_date
                    else j.moc_careplan_sharing_date
                end
            else j.moc_remote_desktop_review_date
        end as moc_stage_2_date,
        -- Named stage measures for dashboard selection. The review event maps to
        -- MDT Review for HRCS/HRS and Remote Desktop Review for MRS/LRS.
        j.moc_check_test_completed as moc_check_test_completed_12m,
        j.moc_check_test_date as moc_check_test_date_12m,
        (
            j.moc_check_test_completed
            and j.moc_check_test_date >= {{ fy_start }}
        ) as moc_check_test_completed_fy,
        case when j.moc_check_test_date >= {{ fy_start }}
            then j.moc_check_test_date end as moc_check_test_date_fy,
        (
            upper(j.overall_risk_group) in ('MR', 'LR')
            and j.moc_remote_desktop_review_completed
        ) as moc_remote_desktop_review_completed_12m,
        case when upper(j.overall_risk_group) in ('MR', 'LR')
            then j.moc_remote_desktop_review_date end as moc_remote_desktop_review_date_12m,
        (
            upper(j.overall_risk_group) in ('MR', 'LR')
            and j.moc_remote_desktop_review_completed
            and j.moc_remote_desktop_review_date >= {{ fy_start }}
        ) as moc_remote_desktop_review_completed_fy,
        case when upper(j.overall_risk_group) in ('MR', 'LR')
            and j.moc_remote_desktop_review_date >= {{ fy_start }}
            then j.moc_remote_desktop_review_date end as moc_remote_desktop_review_date_fy,
        (
            upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_remote_desktop_review_completed
        ) as moc_mdt_review_completed_12m,
        case when upper(j.overall_risk_group) in ('HRC', 'HR')
            then j.moc_remote_desktop_review_date end as moc_mdt_review_date_12m,
        (
            upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_remote_desktop_review_completed
            and j.moc_remote_desktop_review_date >= {{ fy_start }}
        ) as moc_mdt_review_completed_fy,
        case when upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_remote_desktop_review_date >= {{ fy_start }}
            then j.moc_remote_desktop_review_date end as moc_mdt_review_date_fy,
        (
            upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_careplan_sharing_completed
        ) as moc_careplan_sharing_completed_12m,
        case when upper(j.overall_risk_group) in ('HRC', 'HR')
            then j.moc_careplan_sharing_date end as moc_careplan_sharing_date_12m,
        (
            upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_careplan_sharing_completed
            and j.moc_careplan_sharing_date >= {{ fy_start }}
        ) as moc_careplan_sharing_completed_fy,
        case when upper(j.overall_risk_group) in ('HRC', 'HR')
            and j.moc_careplan_sharing_date >= {{ fy_start }}
            then j.moc_careplan_sharing_date end as moc_careplan_sharing_date_fy,
        j.moc_discussion_completed as moc_discussion_completed_12m,
        j.moc_discussion_date as moc_discussion_date_12m,
        (
            j.moc_discussion_completed
            and j.moc_discussion_date >= {{ fy_start }}
        ) as moc_discussion_completed_fy,
        case when j.moc_discussion_date >= {{ fy_start }}
            then j.moc_discussion_date end as moc_discussion_date_fy,
        j.moc_followup_completed as moc_followup_completed_12m,
        j.moc_followup_date as moc_followup_date_12m,
        (
            j.moc_followup_completed
            and j.moc_followup_date >= {{ fy_start }}
        ) as moc_followup_completed_fy,
        case when j.moc_followup_date >= {{ fy_start }}
            then j.moc_followup_date end as moc_followup_date_fy,
        (
            j.moc_check_test_completed
            or j.moc_remote_desktop_review_completed
            or j.moc_careplan_sharing_completed
            or j.moc_discussion_completed
            or j.moc_followup_completed
            or j.moc_declined
        ) as moc_any_activity_12m,
        (
            coalesce(j.moc_check_test_date >= {{ fy_start }}, false)
            or coalesce(j.moc_remote_desktop_review_date >= {{ fy_start }}, false)
            or coalesce(j.moc_careplan_sharing_date >= {{ fy_start }}, false)
            or coalesce(j.moc_discussion_date >= {{ fy_start }}, false)
            or coalesce(j.moc_followup_date >= {{ fy_start }}, false)
            or coalesce(j.moc_declined_date >= {{ fy_start }}, false)
        ) as moc_any_activity_fy,
        -- Latest progression activity date (excludes decline)
        greatest(
            coalesce(j.moc_check_test_date, '1900-01-01'),
            coalesce(j.moc_remote_desktop_review_date, '1900-01-01'),
            coalesce(j.moc_careplan_sharing_date, '1900-01-01'),
            coalesce(j.moc_discussion_date, '1900-01-01'),
            coalesce(j.moc_followup_date, '1900-01-01')
        ) as moc_latest_progression_date_raw
    from joined j
),

with_decline_logic as (
    select
        w.*,
        case when moc_latest_progression_date_raw = '1900-01-01' then null
             else moc_latest_progression_date_raw end as moc_latest_progression_date,
        -- Declined event is the most recent (or only) 12m event
        (
            w.moc_declined
            and (
                moc_latest_progression_date_raw = '1900-01-01'
                or w.moc_declined_date >= moc_latest_progression_date_raw
            )
        ) as moc_declined_is_latest,
        -- Person declined but has since re-engaged with a progression activity
        (
            w.moc_declined
            and moc_latest_progression_date_raw <> '1900-01-01'
            and moc_latest_progression_date_raw > w.moc_declined_date
        ) as moc_re_engaged_after_decline
    from with_pathway w
)

select
    person_id,
    number_of_ltc_lcs_conditions,

    -- Condition risk groups
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

    -- MOC activity flags (last 12 months)
    moc_check_test_completed,
    moc_check_test_date,
    moc_remote_desktop_review_completed,
    moc_remote_desktop_review_date,
    moc_careplan_sharing_completed,
    moc_careplan_sharing_date,
    moc_discussion_completed,
    moc_discussion_date,
    moc_followup_completed,
    moc_followup_date,
    moc_declined,
    moc_declined_date,
    moc_re_engaged_after_decline,
    moc_any_activity_12m,
    moc_any_activity_fy,

    -- Pathway progression
    moc_pathway,
    moc_risk_category,
    moc_check_test_completed_12m,
    moc_check_test_date_12m,
    moc_check_test_completed_fy,
    moc_check_test_date_fy,
    moc_remote_desktop_review_completed_12m,
    moc_remote_desktop_review_date_12m,
    moc_remote_desktop_review_completed_fy,
    moc_remote_desktop_review_date_fy,
    moc_mdt_review_completed_12m,
    moc_mdt_review_date_12m,
    moc_mdt_review_completed_fy,
    moc_mdt_review_date_fy,
    moc_careplan_sharing_completed_12m,
    moc_careplan_sharing_date_12m,
    moc_careplan_sharing_completed_fy,
    moc_careplan_sharing_date_fy,
    moc_discussion_completed_12m,
    moc_discussion_date_12m,
    moc_discussion_completed_fy,
    moc_discussion_date_fy,
    moc_followup_completed_12m,
    moc_followup_date_12m,
    moc_followup_completed_fy,
    moc_followup_date_fy,
    moc_stage_2_completed,
    case
        when moc_followup_completed then 4
        when moc_discussion_completed then 3
        when moc_stage_2_reached then 2
        when moc_check_test_completed then 1
        else 0
    end as moc_stage_completed,
    case
        when moc_followup_completed then 'Follow-up'
        when moc_discussion_completed then 'Discussion'
        when moc_pathway in ('HRCS', 'HRS') and moc_careplan_sharing_completed
            then 'Care Plan Sharing'
        when moc_pathway in ('HRCS', 'HRS') and moc_remote_desktop_review_completed
            then 'MDT Review'
        when moc_stage_2_reached then 'Remote Desktop Review'
        when moc_check_test_completed then 'Check & Test'
        else 'Not started'
    end as moc_stage_completed_label,
    case
        when moc_followup_completed then 'F/U'
        when moc_discussion_completed then 'DISC'
        when moc_pathway in ('HRCS', 'HRS') and moc_careplan_sharing_completed then 'CPS'
        when moc_pathway in ('HRCS', 'HRS') and moc_remote_desktop_review_completed then 'MDT'
        when moc_stage_2_reached then 'RDR'
        when moc_check_test_completed then 'C&T'
    end as moc_stage_completed_code,
    -- 'Cycle complete' requires all pathway stages (same condition as moc_cycle_complete).
    -- People with follow-up recorded but missing priors stay 'In progress' so the
    -- status aligns with moc_cycle_complete and moc_has_missing_priors.
    case
        when moc_declined_is_latest then 'Declined'
        when moc_check_test_completed
            and moc_stage_2_completed
            and moc_discussion_completed
            and moc_followup_completed then 'Cycle complete'
        when moc_check_test_completed
            or moc_stage_2_reached
            or moc_discussion_completed
            or moc_followup_completed then 'In progress'
        else 'Not started'
    end as moc_pathway_status,
    case
        when moc_declined_is_latest then null
        when moc_check_test_completed
            and moc_stage_2_completed
            and moc_discussion_completed
            and moc_followup_completed then null
        when not moc_check_test_completed then 'Check & Test'
        when moc_pathway in ('HRCS', 'HRS') and not moc_remote_desktop_review_completed
            then 'MDT Review'
        when moc_pathway in ('HRCS', 'HRS') and not moc_careplan_sharing_completed
            then 'Care Plan Sharing'
        when moc_pathway in ('MRS', 'LRS') and not moc_remote_desktop_review_completed
            then 'Remote Desktop Review'
        when not moc_discussion_completed then 'Discussion'
        else 'Follow-up'
    end as moc_next_action,
    case
        when moc_declined_is_latest then null
        when moc_check_test_completed
            and moc_stage_2_completed
            and moc_discussion_completed
            and moc_followup_completed then null
        when not moc_check_test_completed then 'C&T'
        when moc_pathway in ('HRCS', 'HRS') and not moc_remote_desktop_review_completed
            then 'MDT'
        when moc_pathway in ('HRCS', 'HRS') and not moc_careplan_sharing_completed
            then 'CPS'
        when moc_pathway in ('MRS', 'LRS') and not moc_remote_desktop_review_completed
            then 'RDR'
        when not moc_discussion_completed then 'DISC'
        else 'F/U'
    end as moc_next_action_code,

    -- Retrospective gap flags: prior stages missing given the furthest stage reached.
    (moc_discussion_completed or moc_followup_completed or moc_stage_2_reached)
        and not moc_check_test_completed
        as moc_missing_check_test,
    (moc_discussion_completed or moc_followup_completed)
        and not moc_stage_2_completed
        as moc_missing_stage_2,
    moc_followup_completed and not moc_discussion_completed
        as moc_missing_discussion,
    (
        ((moc_discussion_completed or moc_followup_completed or moc_stage_2_reached) and not moc_check_test_completed)
        or ((moc_discussion_completed or moc_followup_completed) and not moc_stage_2_completed)
        or (moc_followup_completed and not moc_discussion_completed)
    ) as moc_has_missing_priors,
    (
        moc_check_test_completed
        and moc_stage_2_completed
        and moc_discussion_completed
        and moc_followup_completed
    ) as moc_cycle_complete,

    -- Pathway durations (days). Null where either endpoint is missing.
    -- Negative values indicate out-of-order recording (data quality signal).
    moc_stage_2_date,
    datediff('day', moc_check_test_date, moc_stage_2_date) as moc_days_check_test_to_stage_2,
    datediff('day', moc_stage_2_date, moc_discussion_date) as moc_days_stage_2_to_discussion,
    datediff('day', moc_discussion_date, moc_followup_date) as moc_days_discussion_to_followup,
    datediff('day', moc_check_test_date, moc_followup_date) as moc_days_check_test_to_followup,

    -- Expiry dates (12m rolling window). The care plan expiry anchors on the
    -- discussion appointment (PCSP agreed), since that is the care plan itself.
    dateadd('month', 12, moc_discussion_date) as moc_careplan_expires_date,
    -- Earliest anniversary across all 12m MOC events - the next date on which
    -- any currently-counted event drops out of the 12m window. Null if no events.
    -- Uses the individual stage 2 substage dates (not moc_stage_2_date) so a
    -- partial stage 2 still expires and a completed stage 2 expires on the
    -- earlier substage anniversary.
    case
        when coalesce(
            moc_check_test_date, moc_remote_desktop_review_date,
            moc_careplan_sharing_date, moc_discussion_date,
            moc_followup_date, moc_declined_date
        ) is null then null
        else dateadd('month', 12, least(
            coalesce(moc_check_test_date, '9999-12-31'::date),
            coalesce(moc_remote_desktop_review_date, '9999-12-31'::date),
            coalesce(moc_careplan_sharing_date, '9999-12-31'::date),
            coalesce(moc_discussion_date, '9999-12-31'::date),
            coalesce(moc_followup_date, '9999-12-31'::date),
            coalesce(moc_declined_date, '9999-12-31'::date)
        ))
    end as moc_next_expiry_date,

    current_timestamp()::timestamp_ntz as table_refresh_date
from with_decline_logic
