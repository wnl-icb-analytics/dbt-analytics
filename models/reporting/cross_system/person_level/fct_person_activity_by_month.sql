/*
Monthly summary of patient activity

Clinical Purpose:
- Tracking outcomes and activity over time
- Measuring impact of interventions

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

with date_range AS (-- Generate month start dates for 10 years (120 months)
    select 
        date_trunc('month', dateadd('month', seq4() - 119, current_date)) as month_start_date
        , last_day(date_trunc('month', dateadd('month', seq4() - 119, current_date))) as month_end_date
        , date_trunc('month', dateadd('month', seq4() - 118, current_date)) as next_month_start_date
    from 
        table(generator(rowcount => 120))
    where 
        month_start_date <= date_trunc('month', current_date)
)
, ae_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'A&E' as activity_type
        , pod as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(duration) as duration
    from 
        {{ ref('int_sus_uec_encounter') }}
    where 
        sk_patient_id is not null and sk_patient_id != '1'
    group by 
        sk_patient_id, date_trunc('month', start_date), pod
)
, apc_encounter_summary as(
    select
        e.sk_patient_id
        , d.month_start_date as activity_month
        , 'Inpatient' as activity_type
        , case when e.spec_comm_flag = 'Y' then 'spec_comm' else e.admission_method_group end as activity_subtype
        , sum(case when d.month_start_date = date_trunc('month', e.start_date) then 1 end) as encounters
        -- allocate cost and duration proportionally to days in month
        , sum(
            (datediff('day'
            , greatest(e.start_date, d.month_start_date)
            , least(coalesce(e.end_date, current_date), d.next_month_start_date)
            )+1) * (cost/(duration_to_date+1))
        ) as cost
        , sum(
            datediff('day'
            , greatest(e.start_date, d.month_start_date)
            , least(coalesce(e.end_date, current_date), d.next_month_start_date)
            )
        ) as duration
    from 
        {{ ref('int_sus_apc_encounter') }} as e
    left join -- join all months during the spell
        date_range as d
        on d.month_start_date <= coalesce(e.end_date, current_date)
        and d.month_end_date >= e.start_date
    where 
        e.sk_patient_id is not null and e.sk_patient_id != '1'
    group by 
        e.sk_patient_id, d.month_start_date, case when e.spec_comm_flag = 'Y' then 'spec_comm' else e.admission_method_group end
)
, op_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'Outpatient' as activity_type
        , case when spec_comm_flag = 'Y' then 'spec_comm' else pod end as activity_subtype
        , count(*) as encounters
        , sum(cost) as cost
        , sum(expected_duration) as duration
    from 
        {{ ref('int_sus_op_encounter') }}
    where 
        sk_patient_id is not null and sk_patient_id != '1'
    group by 
        sk_patient_id, date_trunc('month', start_date), case when spec_comm_flag = 'Y' then 'spec_comm' else pod end
)
, gp_encounter_summary as(
    select
        pp.sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'PrimaryCare' as activity_type
        , null::varchar as activity_subtype
        , count(*) as encounters
        , sum(zeroifnull(appointment_cost_gbp_nominal)) as cost -- TO DO: replace with pricing from reference book according to app type?
        , sum(zeroifnull(duration_minutes)) as duration
    from 
        {{ ref('int_appointment_gp_clinical') }} gpa
    left join {{ref('dim_person_pseudo')}} pp on pp.person_id = gpa.person_id
    where gpa.is_attended = TRUE -- Attended
    group by 
        pp.sk_patient_id, date_trunc('month', start_date)
)
, csds_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'CommunityCareContact' as activity_type
        , null::varchar as activity_subtype
        , count(*) as encounters
        , null::number as cost
        , sum(duration) as duration
    from 
        {{ ref('int_csds_encounters') }}
    where sk_patient_id is not null and sk_patient_id != '1'
    group by
        sk_patient_id, date_trunc('month', start_date)
)
, mhsds_spell_encounter_summary as(
    select
        e.sk_patient_id
        , d.month_start_date as activity_month
        , 'MentalHealthServices' as activity_type
        , 'Spell' as activity_subtype
        , sum(case when d.month_start_date = date_trunc('month', e.start_date) then 1 end) as encounters
        -- allocate cost and duration proportionally to days in month
        , sum(
            (datediff('day'
            , greatest(e.start_date, d.month_start_date)
            , least(coalesce(e.end_date, current_date), d.next_month_start_date)
            )+1) * (proxy_cost/(duration_to_date+1))
        ) as cost
        , sum(
            datediff('day'
            , greatest(e.start_date, d.month_start_date)
            , least(coalesce(e.end_date, current_date), d.next_month_start_date)
            )
        ) as duration
    from 
        {{ ref('int_mhsds_spell_encounters') }} as e
    left join -- join all months during the spell
        date_range as d
        on d.month_start_date <= coalesce(e.end_date, current_date)
        and d.month_end_date >= e.start_date
    where 
        e.sk_patient_id is not null and e.sk_patient_id != '1'
    group by 
        e.sk_patient_id, d.month_start_date
)
, mhsds_carecontact_encounter_summary as(
    select
        sk_patient_id
        , date_trunc('month', start_date) as activity_month
        , 'MentalHealthServices' as activity_type
        , 'CareContact' as activity_subtype
        , count(*) as encounters
        , sum(proxy_cost) as cost
        , sum(duration/1440) as duration -- convert to days from minutes to align with spells
    from 
        {{ ref('int_mhsds_carecontact_encounters') }}
    where 
        sk_patient_id is not null and sk_patient_id != '1'
    group by
        sk_patient_id, date_trunc('month', start_date)
)
, combined as(
    select * from ae_encounter_summary
    union all
    select * from apc_encounter_summary
    union all
    select * from op_encounter_summary
    union all
    select * from gp_encounter_summary
    union all
    select * from csds_encounter_summary
    union all
    select * from mhsds_spell_encounter_summary
    union all
    select * from mhsds_carecontact_encounter_summary
)

select 
    sk_patient_id
    , activity_month
    , sum(encounters) as total_encounters
    , sum(cost) as total_cost
    --- high level breakdowns
    -- encounters
    , sum(case when activity_type = 'A&E' then encounters else 0 end) as ae_encounters
    , sum(case when activity_type = 'Inpatient' then encounters else 0 end) as ip_encounters
    , sum(case when activity_type = 'Outpatient' then encounters else 0 end) as op_encounters
    , sum(case when activity_type = 'PrimaryCare' then encounters else 0 end) as gp_encounters
    , sum(case when activity_type = 'CommunityCareContact' then encounters else 0 end) as cc_encounters
    , sum(case when activity_type = 'MentalHealthServices' then encounters else 0 end) as mh_encounters
    -- cost
    , sum(case when activity_type = 'A&E' then cost else 0 end) as ae_cost
    , sum(case when activity_type = 'Inpatient' then cost else 0 end) as ip_cost
    , sum(case when activity_type = 'Outpatient' then cost else 0 end) as op_cost
    , sum(case when activity_type = 'CommunityCareContact' then cost else 0 end) as cc_cost
    , sum(case when activity_type = 'MentalHealthServices' then cost else 0 end) as mh_cost
    -- duration
    , sum(case when activity_type = 'A&E' then duration else 0 end) as ae_duration
    , sum(case when activity_type = 'Inpatient' then duration else 0 end) as ip_duration
    , sum(case when activity_type = 'Outpatient' then duration else 0 end) as op_duration
    , sum(case when activity_type = 'PrimaryCare' then duration else 0 end) as gp_duration
    , sum(case when activity_type = 'CommunityCareContact' then duration else 0 end) as cc_duration
    , sum(case when activity_type = 'MentalHealthServices' then duration else 0 end) as mh_duration
    --- subtype breakdowns
    -- A&E
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then encounters else 0 end) as ae_emergency_encounters
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then encounters else 0 end) as ae_urgent_encounters
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then cost else 0 end) as ae_emergency_cost
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then cost else 0 end) as ae_urgent_cost
    , sum(case when activity_type = 'A&E' and activity_subtype in ('AE-T1', 'AE-Other') then duration else 0 end) as ae_emergency_duration
    , sum(case when activity_type = 'A&E' and activity_subtype in ('UCC', 'WiC', 'SDEC') then duration else 0 end) as ae_urgent_duration
    -- Inpatient
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then encounters else 0 end) as ip_el_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then encounters else 0 end) as ip_nel_emergency_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then encounters else 0 end) as ip_nel_maternity_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then encounters else 0 end) as ip_nel_other_encounters
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'spec_comm' then encounters else 0 end) as ip_spec_comm_encounters

    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then cost else 0 end) as ip_el_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then cost else 0 end) as ip_nel_emergency_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then cost else 0 end) as ip_nel_maternity_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then cost else 0 end) as ip_nel_other_cost
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'spec_comm' then cost else 0 end) as ip_spec_comm_cost   

    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Elective' then duration else 0 end) as ip_el_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - emergency' then duration else 0 end) as ip_nel_emergency_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Maternity' then duration else 0 end) as ip_nel_maternity_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'Non-elective - Other' then duration else 0 end) as ip_nel_other_duration
    , sum(case when activity_type = 'Inpatient' and activity_subtype = 'spec_comm' then duration else 0 end) as ip_spec_comm_duration
    -- Outpatient
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then encounters else 0 end) as op_fa_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then encounters else 0 end) as op_fup_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then encounters else 0 end) as op_proc_encounters
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('spec_comm') then encounters else 0 end) as op_spec_comm_encounters

    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then cost else 0 end) as op_fa_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then cost else 0 end) as op_fup_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then cost else 0 end) as op_proc_cost
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('spec_comm') then cost else 0 end) as op_spec_comm_cost


    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFA-F2F', 'OPFA-NFTF') then duration else 0 end) as op_fa_duration
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPFUP-F2F', 'OPFUP-NFTF') then duration else 0 end) as op_fup_duration
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('OPPROC') then duration else 0 end) as op_proc_duration
    , sum(case when activity_type = 'Outpatient'  and activity_subtype in ('spec_comm') then duration else 0 end) as op_spec_comm_duration

    -- MH
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'Spell' then encounters else 0 end) as mh_spell_encounters
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'CareContact' then encounters else 0 end) as mh_contact_encounters
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'Spell' then cost else 0 end) as mh_spell_cost
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'CareContact' then cost else 0 end) as mh_contact_cost
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'Spell' then duration else 0 end) as mh_spell_duration
    , sum(case when activity_type = 'MentalHealthServices' and activity_subtype = 'CareContact' then duration*1440 else 0 end) as mh_contact_duration -- convert back to minutes
from 
    combined
group by 
    sk_patient_id, activity_month