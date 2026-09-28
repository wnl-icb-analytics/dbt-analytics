{{
    config(
        materialized='table',
        tags=['childhood_imms'])

}}
--Using the person_demographics for anyone under the age of 11 who has been ever been registered with an NCL practice (active or inactive) n~200,000
--Creating a base table for vaccinations for this population to be joined against in further analysis n~2.5 million rows 
SELECT DISTINCT
p.PERSON_ID
--adding age to cross checks against FDP figures. June 2026. Add IMD and Ethnicity to chck against Ardens DEMOGRAPHICS.
,p.AGE
,COALESCE(p.imd_quintile_25, 'Unknown') AS imd_quintile
,CASE
    WHEN p.imd_quintile_25 = 'Most Deprived' THEN 1
    WHEN p.imd_quintile_25 = 'Second Most Deprived' THEN 2
    WHEN p.imd_quintile_25 = 'Third Most Deprived' THEN 3
    WHEN p.imd_quintile_25 = 'Second Least Deprived' THEN 4
    WHEN p.imd_quintile_25 = 'Least Deprived' THEN 5
    ELSE 6
    END AS imdquintile_order
,p.ethnicity_category
,CASE
      WHEN p.ethnicity_category = 'Asian' THEN 1
      WHEN p.ethnicity_category = 'Black' THEN 2
      WHEN p.ethnicity_category = 'Mixed' THEN 3
      WHEN p.ethnicity_category = 'Other' THEN 4
      WHEN p.ethnicity_category = 'White' THEN 5
      WHEN p.ethnicity_category = 'Unknown' THEN 6
    END AS ethcat_order
,p.BOROUGH_REGISTERED
,CASE 
when p.local_authority_code not like 'E09%' then 'Outside London'
when p.local_authority_code is null then 'Unknown'
else p.local_authority_name end as BOROUGH_RESIDENT
,case
    -- all NCL Boroughs
    when p.local_authority_code in ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019') then 'NCL'
    -- all NWL Boroughs
    when p.local_authority_code in ('E09000005','E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033') then 'NWL'
    -- all NEL Boroughs
    when p.local_authority_code in ('E09000002','E09000001','E09000012','E09000016','E09000025','E09000026','E09000030','E09000031') then 'NEL'
    when p.local_authority_code like 'E09%' and p.local_authority_code not in ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019','E09000005', 
    'E09000009','E09000013','E09000015','E09000017','E09000018','E09000020','E09000033','E09000002','E09000001','E09000012',
     'E09000016','E09000025','E09000026','E09000030','E09000031') then 'Other London'
    when p.local_authority_code not like 'E09%' then 'Outside London'
    when p.local_authority_code is null then 'Unknown'
    end as residential_loc
,p.practice_code
,v.VACCINE_ID
,v.VACCINE_NAME
,v.VACCINE_ORDER
,v.EVENT_DATE
,v.EVENT_TYPE
,IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date), YEAR(v.event_date) - 1)
  || '/'
  || LPAD(RIGHT(TO_VARCHAR(IFF(MONTH(v.event_date) >= 4, YEAR(v.event_date) + 1, YEAR(v.event_date))), 2), 2, '0')
    AS FISCAL_YEAR
FROM {{ ref('dim_person_demographics') }} p
--FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS p
LEFT JOIN {{ ref('int_childhood_imms_vaccination_events_historical') }} v using (PERSON_ID)
--LEFT JOIN MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_VACCINATION_EVENTS_HISTORICAL v using (PERSON_ID)
--restrict by AGE to less than 11
WHERE p.age < 11