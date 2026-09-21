-- Context must retain every submitted contact and preserve explicit team pointers.
with checks as (
    select
        count_if(ctx.mhs201_uniq_id is null) as missing_contacts
        , count_if(c.other_care_prof_team_local_id is not null
            and ctx.service_or_team_local_id is distinct from c.other_care_prof_team_local_id)
            as replaced_additional_teams
        , count_if(c.other_care_prof_team_local_id is null
            and c.care_prof_team_local_id is not null
            and ctx.service_or_team_local_id is distinct from c.care_prof_team_local_id)
            as replaced_legacy_teams
        , count_if(ctx.service_or_team_attribution_basis = 'referral_primary_team'
            and (not coalesce(try_to_decimal(s.dat_set_ver::varchar, 10, 2) >= 6
                and try_to_decimal(s.dat_set_ver::varchar, 10, 2) < 7, false)
                or not ctx.is_same_submission_referral_linked)) as unsupported_primary_teams
    from {{ ref('stg_mhsds_carecontact') }} as c
    left join {{ ref('int_mhsds_care_contact_context') }} as ctx
        on c.mhs201_uniq_id = ctx.mhs201_uniq_id
    left join {{ ref('stg_mhsds_activesubmission') }} as s
        on c.uniq_submission_id = s.uniq_submission_id
)
select * from checks
where missing_contacts > 0 or replaced_additional_teams > 0
    or replaced_legacy_teams > 0 or unsupported_primary_teams > 0
