{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

SELECT 
p.fiscal_year
,p.vaccination_dose
,CASE
WHEN p.vaccination_dose = '6-in-1 (dose 1)' THEN 1
WHEN p.vaccination_dose = '6-in-1 (dose 2)' THEN 2
WHEN p.vaccination_dose = '6-in-1 (dose 3)' THEN 3
WHEN p.vaccination_dose = '6-in-1 (dose 4)' THEN 4
WHEN p.vaccination_dose ='Rotavirus (dose 1)' THEN 5
WHEN p.vaccination_dose ='Rotavirus (dose 2)' THEN 6
WHEN p.vaccination_dose ='MenB (dose 1)' THEN 7
WHEN p.vaccination_dose ='MenB (dose 2)' THEN 8
WHEN p.vaccination_dose ='MenB (dose 3)' THEN 9
WHEN p.vaccination_dose ='PCV (dose 1)' THEN 10
WHEN p.vaccination_dose ='PCV (dose 2)' THEN 11
WHEN p.vaccination_dose ='HibMenC (dose 1)' THEN 12
WHEN p.vaccination_dose ='MMR (dose 1)' THEN 13
WHEN p.vaccination_dose ='MMR (dose 2)' THEN 14
WHEN p.vaccination_dose ='4-in-1 (dose 1)' THEN 15
WHEN p.vaccination_dose ='MMRV (dose 1)' THEN 16
WHEN p.vaccination_dose ='MMRV (dose 2)' THEN 17
END As VACC_ORDER 
,p.borough_registered
,p.borough_resident
,p.residential_loc
,p.practice_code
,p.month_year
,p.month_label
,p.age
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.vaccination_count 
FROM (
------- ALL VACCINATION METRICS FROM VACCINATION COUNT CHILD UNDER 10
--sixin1 Dose 1
select 
 '6-in-1 (dose 1)' as vaccination_dose, SIXIN1_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, sixin1_dose1_sort as month_year, SIXIN1_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where sixin1_dose1_sort is not null
group by all

UNION

--sixin1 Dose 2
select 
 '6-in-1 (dose 2)' as vaccination_dose, SIXIN1_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, sixin1_dose2_sort as month_year, SIXIN1_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where sixin1_dose2_sort is not null
group by all

UNION

--sixin1 Dose 3
select 
 '6-in-1 (dose 3)' as vaccination_dose, SIXIN1_DOSE3_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, sixin1_dose3_sort as month_year, SIXIN1_DOSE3_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where sixin1_dose3_sort is not null
group by all

UNION

--sixin1 Dose 4
select 
 '6-in-1 (dose 4)' as vaccination_dose, SIXIN1_DOSE4_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, sixin1_dose4_sort as month_year, SIXIN1_DOSE4_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where sixin1_dose4_sort is not null
group by all

UNION

--rota Dose 1
select 
 'Rotavirus (dose 1)' as vaccination_dose, ROTA_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, rota_dose1_sort as month_year, ROTA_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where rota_dose1_sort is not null
group by all

UNION

--rota Dose 2
select 
 'Rotavirus (dose 2)' as vaccination_dose, ROTA_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, rota_dose2_sort as month_year, ROTA_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where rota_dose2_sort is not null
group by all

UNION

--menb Dose 1
select 
 'MenB (dose 1)' as vaccination_dose, MENB_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, menb_dose1_sort as month_year, MENB_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where menb_dose1_sort is not null
group by all

UNION

--menb Dose 2
select 
 'MenB (dose 2)' as vaccination_dose, MENB_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, menb_dose2_sort as month_year, MENB_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where menb_dose2_sort is not null
group by all

UNION

--menb Dose 3
select 
 'MenB (dose 3)' as vaccination_dose, MENB_DOSE3_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, menb_dose3_sort as month_year, MENB_DOSE3_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
where menb_dose3_sort is not null
group by all

UNION

--pcv Dose 1
select 
 'PCV (dose 1)' as vaccination_dose, PCV_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, pcv_dose1_sort as month_year, PCV_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE pcv_dose1_sort is not null
group by all

UNION

--pcv Dose 2
select 
 'PCV (dose 2)' as vaccination_dose, PCV_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, pcv_dose2_sort as month_year, PCV_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age,  count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE pcv_dose2_sort is not null
group by all

UNION

--HibMenC Dose 1
select 
 'HibMenC (dose 1)' as vaccination_dose, HIBMC_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, hibmc_dose1_sort as month_year, HIBMC_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE hibmc_dose1_sort is not null
group by all

UNION

--mmr Dose 1
select 
 'MMR (dose 1)' as vaccination_dose, MMR_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, mmr_dose1_sort as month_year, MMR_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE mmr_dose1_sort is not null
group by all

UNION

--mmr Dose 2
select 
 'MMR (dose 2)' as vaccination_dose, MMR_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, mmr_dose2_sort as month_year, MMR_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE mmr_dose2_sort is not null
group by all

UNION

--fourin1 Dose 1
select 
 '4-in-1 (dose 1)' as vaccination_dose, FOURIN1_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, fourin1_dose1_sort as month_year, fourin1_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE fourin1_dose1_sort is not null
group by all

UNION

--mmrv Dose 1
select 
 'MMRV (dose 1)' as vaccination_dose, MMRV_DOSE1_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, mmrv_dose1_sort as month_year, MMRV_DOSE1_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE mmrv_dose1_sort is not null
group by all

UNION

--mmrv Dose 2
select 
 'MMRV (dose 2)' as vaccination_dose, MMRV_DOSE2_FISCAL as fiscal_year,  borough_registered, borough_resident, residential_loc,  practice_code, mmrv_dose2_sort as month_year, MMRV_DOSE2_LABEL as month_label, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, age, count(person_id) as vaccination_count
FROM {{ ref('int_childhood_imms_dose_count_child') }}
--FROM MODELLING.OLIDS_PROGRAMME.INT_CHILDHOOD_IMMS_DOSE_COUNT_CHILD
WHERE mmrv_dose2_sort is not null
group by all

) p









