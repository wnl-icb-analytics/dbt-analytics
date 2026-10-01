{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.fiscal_year_label
,p.vaccination_metric
,'1 YEAR' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 1 Year' THEN 1
WHEN p.Vaccination_metric ='6-in-1 (3 doses) 1 Year' THEN 2
WHEN p.Vaccination_metric ='Rotavirus (2 doses) 1 Year' THEN 3
WHEN p.Vaccination_metric ='Men B (2 doses) 1 Year' THEN 4
WHEN p.Vaccination_metric ='PCV (dose 1) 1 Year' THEN 5
END As VACC_ORDER 
,p.practice_name AS GP_NAME
,p.practice_code
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.numerator 
,p.denominator 
FROM (

------- 1 YEAR METRICS FROM HISTORICAL 
--sixin1_1y 
select 
 '6-in-1 (3 doses) 1 Year' as vaccination_metric, analysis_month, fiscal_year_label,  practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(sixin1_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_1') }}
group by all

UNION
--rotavirus_1y 
select 
 'Rotavirus (2 doses) 1 Year' as vaccination_metric, analysis_month, fiscal_year_label,  practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(rota_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_1') }}
group by all

UNION
--menb_1y 
select 
 'Men B (2 doses) 1 Year' as vaccination_metric, analysis_month, fiscal_year_label,  practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(menb_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_1') }}
group by all

UNION
--pcv_1y 
select 
 'PCV (dose 1) 1 Year' as vaccination_metric, analysis_month, fiscal_year_label,  practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(pcv_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_1') }}
group by all

UNION
--all_vacc1y 
select 
 'All vaccinations 1 Year' as vaccination_metric, analysis_month, fiscal_year_label,  practice_name, practice_code, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(all_comp_by_1) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_1') }}
group by all

) p
