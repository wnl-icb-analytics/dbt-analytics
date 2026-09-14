-- Current-state fields use the active MHSDS feed, which is about six weeks
-- behind the run date. Contact recency and Mental Health Act currency both
-- measure back from the latest accepted reporting period end date, so the
-- model uses one clock rather than mixing feed dates with the run date.
with latest_reporting_period as (
    select
        max(reporting_period_end_date) as latest_reporting_period_end_date
    from {{ ref('stg_mhsds_activesubmission') }}
)

, population as (
    select person_id
    from {{ ref('fct_mhsds_referral') }}
    where person_id is not null

    union

    select person_id
    from {{ ref('fct_mhsds_care_contact') }}
    where person_id is not null

    union

    select person_id
    from {{ ref('int_mhsds_spell_currency') }}
    where person_id is not null
)

, referral_aggregates as (
    select
        person_id
        , count(*) as n_referrals_ever
        , count_if(referral_status = 'open') as n_open_referrals
        , min(referral_received_date) as first_referral_date
        , max(referral_received_date) as latest_referral_date
    from {{ ref('fct_mhsds_referral') }}
    where person_id is not null
    group by person_id
)

, contact_aggregates as (
    select
        c.person_id
        , max(c.care_contact_date) as latest_contact_date
        , count_if(
            c.care_contact_date
                >= dateadd(month, -12, p.latest_reporting_period_end_date)
        ) > 0 as has_contact_last_12m
        , count_if(
            c.care_contact_date
                >= dateadd(day, -90, p.latest_reporting_period_end_date)
        ) > 0 as has_contact_last_90d
        , count_if(
            c.care_contact_date
                >= dateadd(month, -12, p.latest_reporting_period_end_date)
            and lpad(c.attendance_status_code, 2, '0') in ('05', '06')
        ) as n_attended_contacts_12m
        , count_if(
            c.care_contact_date
                >= dateadd(month, -12, p.latest_reporting_period_end_date)
            and c.attendance_status_code is null
        ) as n_contacts_with_missing_attendance_status_12m
    from {{ ref('fct_mhsds_care_contact') }} as c
    cross join latest_reporting_period as p
    where c.person_id is not null
    group by c.person_id
)

, crisis_contact_aggregates as (
    select
        c.person_id
        , count_if(
            c.care_contact_date
                >= dateadd(month, -12, p.latest_reporting_period_end_date)
            and lpad(c.attendance_status_code, 2, '0') in ('05', '06')
        ) > 0 as has_crisis_contact_12m
    from {{ ref('fct_mhsds_care_contact') }} as c
    inner join {{ ref('fct_mhsds_referral_episodes') }} as r
        on c.referral_source_record_id = r.source_record_id
    cross join latest_reporting_period as p
    where c.person_id is not null
        and r.is_crisis_referral
    group by c.person_id
)

, current_inpatient_aggregates as (
    select
        person_id
        , true as is_current_inpatient
    from {{ ref('fct_mhsds_current_inpatients') }}
    where person_id is not null
    group by person_id
)

, spell_aggregates as (
    select
        person_id
        , max(start_date_hosp_prov_spell) as latest_admission_date
        , max(iff(end_date_source = 'discharged', end_date, null))
            as latest_discharge_date
        , count(*) as n_spells_ever
        , count(*) > 0 as ever_inpatient
    from {{ ref('int_mhsds_spell_currency') }}
    where person_id is not null
    group by person_id
)

-- Ranking is by coding timestamp alone, so the retained row is the person's
-- latest dated primary diagnosis rather than their latest ICD-10-coded one.
-- int_mhsds_currency_primary_diagnosis leaves icd10_3 null for an ICD-10 code outside F, G, Q
-- and R and for a SNOMED code with no map, so the published diagnosis columns
-- are null whenever that latest record did not resolve. has_diagnosis_record
-- separates that from a person with no dated primary diagnosis on a retained referral.
, latest_diagnosis as (
    select
        r.person_id
        , true as has_diagnosis_record
        , d.icd10_3
        , icd.description as icd10_3_description
        , g.population_category as diagnosis_category
        , d.coded_diag_timestamp as latest_diagnosis_date
    from {{ ref('int_mhsds_currency_primary_diagnosis') }} as d
    inner join {{ ref('fct_mhsds_referral') }} as r
        on d.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_icd10_groups_2627') }} as g
        on d.icd10_3 between g.icd10_range_start and g.icd10_range_end
    left join {{ ref('stg_dictionary_dbo_diagnosis') }} as icd
        on upper(trim(d.icd10_3)) = icd.code
    qualify row_number() over (
        partition by r.person_id
        order by
            d.coded_diag_timestamp desc
            , d.reporting_period_end_date desc
            , d.effective_from desc nulls last
            , d.uniq_submission_id desc
            , d.source_record_number
            , d.source_row_number
            , d.mhs604_uniq_id
    ) = 1
)

-- MHSDS defines no national detained or informal derivation, so detention
-- follows the national code definitions held in
-- mhsds_mh_act_legal_status_classification: the codes defined as formally
-- detained under an Act are detention, and informal admission, guardianship,
-- not applicable and not known are not. nhsd_legal_status is the
-- specification's cleaned form of the submitted code, so it is the join key.
-- follow-up (non-blocking): the NHS England Learning Disability and Autism
-- Mental Health Act measures additionally reclassify a detained patient as
-- informal while a concurrent Conditional Discharge (MHS403), Community
-- Treatment Order (MHS404) or CTO Recall (MHS405) is open. Those raw models
-- hold data but have no staging interface, and the rule belongs to that
-- publication rather than to MHSDS, so this model does not apply it.
, mha_periods as (
    select
        m.person_id
        , m.uniq_mh_act_episode_id
        , m.nhsd_legal_status
        , m.start_date_mh_act_legal_status_class
        , m.end_date_mh_act_legal_status_class
        , m.expiry_date_mh_act_legal_status_class
        , m.reporting_period_end_date
        -- a code outside the national list, which is the -1 sentinel and null,
        -- is not detention
        , coalesce(c.is_detained, false) as is_detained
    from {{ ref('stg_mhsds_mhactperiod') }} as m
    -- one row per nhsd_legal_status in the classification, so the join keeps
    -- the legal-status period grain
    left join {{ ref('mhsds_mh_act_legal_status_classification') }} as c
        on m.nhsd_legal_status = c.nhsd_legal_status
    where m.person_id is not null
)

, latest_detention as (
    select
        person_id
        , nhsd_legal_status as latest_detention_code
        , start_date_mh_act_legal_status_class as latest_detention_start_date
    from mha_periods
    where is_detained
    qualify row_number() over (
        partition by person_id
        order by
            start_date_mh_act_legal_status_class desc nulls last
            , reporting_period_end_date desc nulls last
            , uniq_mh_act_episode_id desc
    ) = 1
)

, mha_aggregates as (
    select
        m.person_id
        , count_if(m.is_detained) > 0 as has_ever_been_detained
        -- a period is current through and including its expiry date: MHSDS
        -- ETOS M401040 defines it as the date the legal status classification
        -- expires. Both tests run as of the latest accepted reporting period,
        -- not the run date, so the feed's six-week lag cannot expire a
        -- detention that was current in the accepted snapshot.
        , count_if(
            m.is_detained
            and m.end_date_mh_act_legal_status_class is null
            and (
                m.expiry_date_mh_act_legal_status_class is null
                or m.expiry_date_mh_act_legal_status_class
                >= p.latest_reporting_period_end_date
            )
            and m.reporting_period_end_date >= dateadd(
                month, -2, p.latest_reporting_period_end_date
            )
        ) > 0 as is_currently_detained
    from mha_periods as m
    cross join latest_reporting_period as p
    group by m.person_id
)

, latest_patient_indicators as (
    select
        person_id
        , cpp
        , lac_status
    from {{ ref('stg_mhsds_patientindicators') }}
    where person_id is not null
    qualify row_number() over (
        partition by person_id
        order by
            reporting_period_end_date desc nulls last
            , effective_from desc nulls last
            , uniq_submission_id desc
            , mhs005_uniq_id desc
    ) = 1
)

, safeguarding_aggregates as (
    select
        person_id
        , upper(cpp) as child_protection_plan_status_code
        , upper(lac_status) as looked_after_child_indicator_code
    from latest_patient_indicators
)

select
    p.person_id
    , b.sk_patient_id
    , coalesce(r.n_referrals_ever, 0) as n_referrals_ever
    , coalesce(r.n_open_referrals, 0) as n_open_referrals
    , r.first_referral_date
    , r.latest_referral_date
    , c.latest_contact_date
    , coalesce(c.has_contact_last_12m, false) as has_contact_last_12m
    , coalesce(c.has_contact_last_90d, false) as has_contact_last_90d
    , coalesce(c.n_attended_contacts_12m, 0) as n_attended_contacts_12m
    , coalesce(c.n_contacts_with_missing_attendance_status_12m, 0)
        as n_contacts_with_missing_attendance_status_12m
    , coalesce(cc.has_crisis_contact_12m, false) as has_crisis_contact_12m
    , coalesce(ci.is_current_inpatient, false) as is_current_inpatient
    , s.latest_admission_date
    , s.latest_discharge_date
    , coalesce(s.n_spells_ever, 0) as n_spells_ever
    , coalesce(s.ever_inpatient, false) as ever_inpatient
    , coalesce(d.has_diagnosis_record, false) as has_diagnosis_record
    , d.icd10_3
    , d.icd10_3_description
    , d.diagnosis_category
    , d.latest_diagnosis_date
    , coalesce(m.has_ever_been_detained, false) as has_ever_been_detained
    , coalesce(m.is_currently_detained, false) as is_currently_detained
    , ld.latest_detention_start_date
    , ld.latest_detention_code
    , mha_status.legal_status_desc as latest_detention_description
    , g.child_protection_plan_status_code
    , cpp.description as child_protection_plan_status_description
    , g.looked_after_child_indicator_code
    , lac.description as looked_after_child_indicator_description
    , case g.looked_after_child_indicator_code
        when 'Y' then true
        when 'N' then false
    end as is_looked_after_child
from population as p
left join {{ ref('stg_mhsds_bridging') }} as b
    on p.person_id = b.person_id
left join referral_aggregates as r
    on p.person_id = r.person_id
left join contact_aggregates as c
    on p.person_id = c.person_id
left join crisis_contact_aggregates as cc
    on p.person_id = cc.person_id
left join current_inpatient_aggregates as ci
    on p.person_id = ci.person_id
left join spell_aggregates as s
    on p.person_id = s.person_id
left join latest_diagnosis as d
    on p.person_id = d.person_id
left join mha_aggregates as m
    on p.person_id = m.person_id
left join latest_detention as ld
    on p.person_id = ld.person_id
-- one row per legal_status_code in the reference, so the label join keeps the
-- one-row-per-person grain
left join {{ ref('stg_ukhfd_mental_health_act_legal_status_classification') }}
    as mha_status
    on ld.latest_detention_code = mha_status.legal_status_code
left join safeguarding_aggregates as g
    on p.person_id = g.person_id
left join {{ ref('mhsds_profile_code_lookup') }} as cpp
    on g.child_protection_plan_status_code = cpp.code
    and cpp.code_set_name = 'child_protection_plan_status'
left join {{ ref('mhsds_profile_code_lookup') }} as lac
    on g.looked_after_child_indicator_code = lac.code
    and lac.code_set_name = 'looked_after_child_indicator'
