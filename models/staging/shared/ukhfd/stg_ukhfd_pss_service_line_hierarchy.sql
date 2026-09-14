select
    service_line,
    service_line_description,
    applicable_in_setting_1,
    applicable_in_setting_2,
    apc_episode_spell_hierarchies,
    apc_percentage_top_up,
    nac_hierarchy,
    file_name,
    import_date,
    created_date
from {{ ref('raw_ukhfd_pss_service_line_hierarchy') }}