{{ 
    config(materialized='view') 
}}

with joined_data as (
    select 
        gp_practice_code as practice_code,
        sex,
        dem.lsoa_code,
        effective_snapshot_date,
        size,
        imd.release_year as imd_release_year,
        imd.imd_score
    from {{ ref('stg_ukhfd_registered_patients_by_gp_practice_lsoa_2021') }} dem
    
    inner join {{ ref('imd_2025_lsoa_2021') }} imd
    on imd.release_year = 2025
    and imd.lsoa_code = dem.lsoa_code
)

select 
        practice_code,
        effective_snapshot_date,
        sum(size*imd_score) / sum(size) as  imd_score,
        imd_release_year

    from joined_data

    group by all

order by effective_snapshot_date desc, practice_code