with snapshot_dates as (
    select 
        cast(full_date as date) as snapshot_date,
        concat(year(snapshot_date) - 1, '/', year(snapshot_date) - 2000) as snapshot_year
    --Hardcoded as table not in staging
    from {{ref('stg_dictionary_dbo_dates')}} ddd
    --Bound range of dates
    where current_date() > full_date
    --Last day of Financial Year
    and month(full_date) = 3
    and day(full_date) = 31

    qualify row_number() over (
        order by full_date desc
    ) <= 5
),
pds_person as (
    select
        sd.snapshot_year,
        sd.snapshot_date,
        sk_patient_id,
        event_from_date as record_person_start_date,
        event_to_date as record_person_end_date,
        year_month_of_birth,
        gender_code,
        date_of_death,
        preferred_language_code,
        interpreter_required
    from {{ref('stg_pds_person')}} pds

    --Join to get annual snapshots of the data
    inner join snapshot_dates sd
    on sd.snapshot_date between
        pds.event_from_date and
        coalesce(pds.event_to_date, '9999-12-31')

    --Qualify to handle events that transition on the snapshot date without a day in-between
    qualify row_number() over(
        partition by sd.snapshot_date, pds.sk_patient_id
        order by coalesce(pds.event_to_date, '9999-12-31') desc, pds.row_id desc
    ) = 1
),
pds_registered as (
    select
        sd.snapshot_year,
        sd.snapshot_date,
        pds.sk_patient_id,
        pds.event_from_date as record_registered_start_date,
        pds.event_to_date as record_registered_end_date,
        practice_code,
        rfr.reason_for_removal as registered_reason_for_removal
        
    from {{ref('stg_pds_patient_care_practice')}} pds

        
    --Join to get annual snapshots of the data
    inner join snapshot_dates sd
    on sd.snapshot_date between
        pds.event_from_date and
        coalesce(pds.event_to_date, '9999-12-31')

    --Join to get reason for removal
    left join {{ref('stg_pds_reason_for_removal')}} rfr
    on rfr.sk_patient_id = pds.sk_patient_id
    --Snapshot date
    and sd.snapshot_date between
        rfr.event_from_date
        and coalesce(rfr.event_to_date,'9999-12-31')
    --Reason for removal must start after the record exists
    and pds.event_from_date <= rfr.event_from_date

    --Qualify to handle events that transition on the snapshot date without a day in-between
    qualify row_number() over(
        partition by sd.snapshot_date, pds.sk_patient_id
        order by coalesce(pds.event_to_date, '9999-12-31') desc, pds.row_id desc
    ) = 1
),
pds_address as (
    select
        sd.snapshot_year,
        sd.snapshot_date,
        sk_patient_id,
        event_from_date as record_residence_start_date,
        event_to_date as record_residence_end_date,
        postcode_sector,
        lsoa_21
        
    from {{ref('stg_pds_address')}} pds

    --Join to get annual snapshots of the data
    inner join snapshot_dates sd
    on sd.snapshot_date between
        pds.event_from_date and
        coalesce(pds.event_to_date, '9999-12-31')

    --Qualify to handle events that transition on the snapshot date without a day in-between
    qualify row_number() over(
        partition by sd.snapshot_date, pds.sk_patient_id
        order by coalesce(pds.event_to_date, '9999-12-31') desc, pds.row_id desc
    ) = 1
)

--Script to combine the 3 PDS data tables into a single wider table
select 
    pds_person.*,
    pds_registered.* exclude (snapshot_year, snapshot_date, sk_patient_id), 
    pds_address.* exclude (snapshot_year, snapshot_date, sk_patient_id)
from pds_person

left join pds_registered on
pds_person.sk_patient_id = pds_registered.sk_patient_id
and pds_person.snapshot_date = pds_registered.snapshot_date

left join pds_address on
pds_person.sk_patient_id = pds_address.sk_patient_id
and pds_person.snapshot_date = pds_address.snapshot_date
