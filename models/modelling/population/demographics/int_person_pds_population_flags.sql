-- Current registered / resident population flags for the WNL footprint
-- (NCL + NWL). Renamed from int_person_pds_ncl_population_flags and widened:
-- registered uses the WNL practice list; resident covers resident_flag in
-- ('NCL','NWL').
select
    sk_patient_id,
    -- Flag for current WNL registered population
    (
        --Practice is in the WNL list and active
        gp_lu.practice_code is not null and

        --No record of death
        pds.date_of_death is null and

        --No record of the registration being removed
        pds.registered_reason_for_removal is null and

        --The GP registration has no end date (active)
        pds.record_registered_end_date is null
    ) as flag_current_registered,
    pds.record_registered_start_date as record_registered_start_date,

    -- Flag for current WNL resident population
    (
        --LSOA 21 is in NCL or NWL (coalesce so an unmatched LSOA is false, not null)
        coalesce(geo.resident_flag in ('NCL', 'NWL'), false) and

        --No record of death
        pds.date_of_death is null and

        --The residence record has no end date (active)
        pds.record_residence_end_date is null
    ) as flag_current_resident,
    pds.record_residence_start_date as record_residence_start_date

from {{ ref('int_person_pds_latest_record') }} pds

-- WNL practice list from the ODS-derived practice dimension (stable codes),
-- replacing the fragile GP_PRACTICE_WNL surrogate-key view.
left join {{ ref('dim_practice') }} gp_lu
on pds.practice_code = gp_lu.practice_code
and gp_lu.is_wnl_practice
and gp_lu.is_active_practice

left join {{ ref('stg_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025') }} geo
on pds.lsoa_21 = geo.lsoa_2021_code
