-- ODS epraccur via UKHFD. Rows loaded before the ODS API switch carry the
-- legacy file codes (setting 4, status A/C/D/P); later rows carry ODS role
-- and status names (RO76, ACTIVE/INACTIVE/DORMANT). Both are normalised here.
select
    upper(trim(organisation_code)) as organisation_code
    , organisation_name
    , case status_code
        when 'A' then 'Active'
        when 'ACTIVE' then 'Active'
        when 'C' then 'Closed'
        when 'INACTIVE' then 'Closed'
        when 'D' then 'Dormant'
        when 'DORMANT' then 'Dormant'
        when 'P' then 'Proposed'
        when 'PROPOSED' then 'Proposed'
    end as organisation_status
    , status_code as source_status_code
    , prescribing_setting as source_prescribing_setting
    , prescribing_setting in ('4', 'RO76') as is_gp_practice
    , parent_organisation_code as sub_icb_code
    , high_level_health_authority_code as icb_code
    , national_grouping_code as region_code
    , join_parent_date::date as sub_icb_join_date
    , left_parent_date::date as sub_icb_left_date
    , open_date
    , close_date
    , postcode
    , address_line_1
    , address_line_2
    , address_line_3
    , address_line_4
    , address_line_5
    , contact_telephone_number as contact_phone
    , is_latest = 1 as is_latest
    , effective_from as effective_from_at
    , effective_to as effective_to_at
from {{ ref('raw_ukhfd_gp_practices_and_prescribing_ccs') }}
