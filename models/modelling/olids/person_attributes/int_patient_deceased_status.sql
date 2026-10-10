{{
    config(
        materialized='table',
        tags=['intermediate', 'patient', 'deceased'])
}}

-- Patient-level deceased status with approximate death date
-- Single source of truth for death date approximation logic
-- Uses midpoint of death month, or July 1st if only death year is known
-- 02-04-2026 Updated logic to use maximum day of death from OLIDS record across all patient ids
-- Combines death data from OLIDS record with those recorded in the PDS service and in Registries.

-- create CTE to copy OLIDS death records across all instances of patient_ids
with olids_deaths as (
select 
id,
sk_patient_id,
death_month as olids_death_month,
death_year as olids_death_year,
DATEADD(
    DAY,
    FLOOR(DAY(LAST_DAY(TO_DATE(olids_death_year || '-' || olids_death_month || '-01'))) / 2),
    TO_DATE(olids_death_year || '-' || olids_death_month || '-01') ) AS olids_death_date_approx
--FROM MODELLING.DBT_STAGING.STG_OLIDS_PATIENT
FROM {{ ref('stg_olids_patient') }}
where death_year is not null
group by all
)
-- Latest version of each PDS person record that has ever held a death date.
-- A later version can reverse a death notification or correct its date, so an
-- earlier version must not decide death.
,pds_latest_person as (
select
sk_patient_id,
death_status,
date_of_death
FROM {{ ref('stg_pds_person') }}
where sk_patient_id in (
    select sk_patient_id from {{ ref('stg_pds_person') }} where date_of_death is not null
)
qualify row_number() over (
    partition by sk_patient_id
    order by event_from_date desc, event_to_date desc nulls first, row_id desc
) = 1
)
--Whole patient level population with all death records from OLIDS, PDS and Registries.
,POPULATION as (
select 
a.patient_id, 
a.sk_patient_id,
max(olids_death_month) as olids_death_month,
max(olids_death_year) as olids_death_year,
max(olids_death_date_approx) as olids_death_date_approx,
max(pds_death_status) as pds_death_status,
max(pds_date_of_death) as pds_date_of_death,
max(reg_date_of_death) as reg_date_of_death
FROM (
--1. Deaths from OLIDS (choose MAX Date if > death date per person or one patient id has a death as null and the other does not)
select distinct
p.id as patient_id, 
p.sk_patient_id, 
max(od.olids_death_month) as olids_death_month,
max(od.olids_death_year) as olids_death_year,
max(od.olids_death_date_approx) as olids_death_date_approx,
NULL as pds_death_status,
NULL as pds_date_of_death,
NULL as reg_date_of_death
--FROM MODELLING.DBT_STAGING.STG_OLIDS_PATIENT p
FROM {{ ref('stg_olids_patient') }} p
LEFT JOIN olids_deaths od on od.sk_patient_id = p.sk_patient_id 
group by all

UNION
--2. Deaths from PDS 
--Death Status = 1 Informal - death notice received via an update from a local NHS PROVIDER such as GP or NHS Trust OR Death Status = 2 Formal - death notice received from Registrar of Deaths
select  distinct
p.id as patient_id, 
p.sk_patient_id, 
NULL as olids_death_month, 
NULL as olids_death_year, 
NULL olids_death_date_approx,
pds.death_status as pds_death_status,
pds.date_of_death as pds_date_of_death,
NULL as reg_date_of_death
--FROM MODELLING.DBT_STAGING.STG_OLIDS_PATIENT p
FROM {{ ref('stg_olids_patient') }} p
LEFT JOIN pds_latest_person pds on pds.sk_patient_id = p.sk_patient_id
where pds.date_of_death is not null 

UNION
--3. Deaths from registry feed. This is one month behind PDS
select  distinct
p.id as patient_id, 
p.sk_patient_id, 
NULL as olids_death_month, 
NULL as olids_death_year, 
NULL olids_death_date_approx,
NULL as pds_death_status,
NULL as pds_date_of_death,
DATE(d.reg_date_of_death) as reg_date_of_death
--FROM MODELLING.DBT_STAGING.STG_OLIDS_PATIENT p
FROM {{ ref('stg_olids_patient') }} p
--LEFT JOIN MODELLING.DBT_STAGING.STG_REGISTRIES_DEATHS d on d.sk_patient_id = p.sk_patient_id
LEFT JOIN {{ ref('stg_registries_deaths') }} d on d.sk_patient_id = p.sk_patient_id
) a
group by all
)
-- A registry or PDS death dated in or before the birth month is a mis-keyed birth date
-- when another source dates the death later; it is not used for final_date_of_death.
-- Genuine infant deaths keep their dates, as no source contradicts them.
,USABLE_DATES as (
select
pop.*,
CASE WHEN pop.reg_date_of_death <= bm.birth_month_end
    AND GREATEST_IGNORE_NULLS(pop.pds_date_of_death, pop.olids_death_date_approx) > bm.birth_month_end
    THEN NULL ELSE pop.reg_date_of_death END AS usable_reg_date_of_death,
CASE WHEN pop.pds_date_of_death <= bm.birth_month_end
    AND GREATEST_IGNORE_NULLS(pop.reg_date_of_death, pop.olids_death_date_approx) > bm.birth_month_end
    THEN NULL ELSE pop.pds_date_of_death END AS usable_pds_date_of_death
FROM POPULATION pop
LEFT JOIN (
    select
    id as patient_id,
    LAST_DAY(TO_DATE(birth_year || '-' || birth_month || '-01')) as birth_month_end
    FROM {{ ref('stg_olids_patient') }}
    where birth_year is not null and birth_month is not null
) bm on bm.patient_id = pop.patient_id
)
,COMBINED as (
select
patient_id,
sk_patient_id,
CASE 
WHEN (pds_date_of_death IS NOT NULL OR reg_date_of_death IS NOT NULL OR olids_death_date_approx IS NOT NULL) THEN TRUE ELSE FALSE END AS is_deceased,
olids_death_date_approx,
pds_death_status,
pds_date_of_death,
reg_date_of_death,
--choose formal registry date if there is a mismatch where available or choose PDS (informal death status) if registry is null or choose olids_death_date_approx
CASE
WHEN usable_pds_date_of_death IS NOT NULL AND usable_reg_date_of_death IS NOT NULL
and NOT EQUAL_NULL(usable_pds_date_of_death, usable_reg_date_of_death) THEN usable_reg_date_of_death
WHEN usable_pds_date_of_death IS NOT NULL AND usable_reg_date_of_death IS NULL
THEN usable_pds_date_of_death
WHEN olids_death_date_approx is not null and usable_reg_date_of_death is null and usable_pds_date_of_death is null
THEN olids_death_date_approx ELSE usable_reg_date_of_death END AS final_date_of_death,
--Derive death source
CASE 
WHEN olids_death_date_approx is not null and reg_date_of_death is null and pds_date_of_death is null THEN 'OLIDS'
WHEN pds_date_of_death IS NOT NULL and pds_death_status = 1 AND reg_date_of_death IS NULL 
AND olids_death_date_approx is null THEN 'PDS Informal'
WHEN pds_date_of_death IS NOT NULL and pds_death_status = 2 AND reg_date_of_death IS NULL 
AND olids_death_date_approx is null THEN 'PDS Formal'
WHEN pds_date_of_death IS NOT NULL AND reg_date_of_death IS NOT NULL AND olids_death_date_approx is not null
THEN 'PDS Registry OLIDS'
WHEN pds_date_of_death IS NOT NULL AND reg_date_of_death IS NOT NULL AND olids_death_date_approx is null
THEN 'PDS Registry'
WHEN pds_date_of_death is null AND olids_death_date_approx is null AND reg_date_of_death IS NOT NULL THEN 'Registry'
WHEN pds_date_of_death is not null AND olids_death_date_approx is not null AND reg_date_of_death IS NULL THEN 'PDS OLIDS'
WHEN pds_date_of_death is null AND olids_death_date_approx is not null AND reg_date_of_death IS NOT NULL THEN 'Registry OLIDS'
END as death_source_flag
FROM USABLE_DATES
GROUP BY ALL
)
--FINAL 
select 
patient_id, 
sk_patient_id,
is_deceased,
death_source_flag,
--derive columns for final table
MONTH(final_date_of_death) as death_month,
YEAR(final_date_of_death) as death_year,
DATEADD(DAY,
        FLOOR(
        DAY(LAST_DAY(TO_DATE(YEAR(final_date_of_death) || '-' || MONTH(final_date_of_death) || '-01'))) / 2),
        TO_DATE(YEAR(final_date_of_death) || '-' || MONTH(final_date_of_death) || '-01')) 
        AS death_date_approx,
olids_death_date_approx,
pds_death_status,
pds_date_of_death,
reg_date_of_death,
final_date_of_death
from COMBINED
