-- One row per calendar year, across active submitted actions.
-- Profile recorded context before treating current person attributes as substitutes.
select year(action_dt_tm) as action_year,
    count(*) as action_rows,
    count(patient_age) as age_rows,
    count_if(patient_age < 0 or patient_age > 120) as unusual_age_rows,
    count(patient_sex_cd) as sex_code_rows,
    count(nullif(trim(patient_sex_desc), '')) as sex_name_rows,
    count(nullif(trim(patients_lsoa), '')) as residence_lsoa_rows,
    count(nullif(trim(patients_reg_gp_practice_id), '')) as registered_practice_rows,
    count(nullif(trim(patients_la_of_residence_id), '')) as residence_borough_rows,
    count(nullif(trim(patients_la_of_residence_name), '')) as residence_borough_name_rows,
    count(nullif(trim(patients_la_of_registration_id), '')) as registration_borough_rows,
    count(nullif(trim(referrer_commissioner_id), '')) as referrer_commissioner_rows,
    count(nullif(trim(referrer_commissioner_name), '')) as referrer_commissioner_name_rows
from {{ ref('stg_ers_ubrn_action') }}
group by action_year
order by action_year
