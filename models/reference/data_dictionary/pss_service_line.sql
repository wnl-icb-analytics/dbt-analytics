--Query to get latest Service Line Description for each Service Line
with sld as (
    select distinct
        service_line,
        service_line_description,
        case
            when file_name like '%201920%' then 2019
            when file_name like '%202021%' then 2020
            when file_name like '%202122%' then 2021
            when file_name like '%202223%' then 2022
            when file_name like '%202324%' then 2023
            when file_name like '%202425%' then 2024
            when file_name like '%202526%' then 2025
            when file_name like '%202627%' then 2026
            when file_name like '%202728%' then 2027
            when file_name like '%202829%' then 2028
            when file_name like '%202930%' then 2029
            when file_name like '%203031%' then 2030
            else  0
        end as file_period
    from {{ ref('stg_ukhfd_pss_service_line_hierarchy') }}

    --Only keep the description from the most recent file
    qualify file_period = max(file_period) over (partition by service_line)
    --To prevent 1 to many mapping, add a default case in situations when a service line maps to multiple desc in the latest year
    and service_line_description = min(service_line_description) over (partition by service_line, file_period)
),

cs as (
    select distinct
    split_part(ps_flag, ' - ', 1) as service_line_number_code,
    initcap(split_part(npo_c_category, ' - ', 1))  as spec_comm_code
    
    from {{ ref('stg_ukhfd_pss_summary_of_id_code_sets') }}

    qualify created_date = max(created_date) over (partition by service_line_number_code)
)

select
    cs.service_line_number_code,
    sld.service_line_description as service_line_number_desc,
    cs.spec_comm_code,
    case 
        when sld.file_period > 0 
        then cast(sld.file_period as string) || '/' || right(cast(sld.file_period + 1 as string), 2) 
        when sld.file_period = 0 
        then 'Prior to 2019-20'
        else null 
    end as source_period
from sld

left join cs
on sld.service_line = cs.service_line_number_code

order by service_line, file_period desc, service_line_description