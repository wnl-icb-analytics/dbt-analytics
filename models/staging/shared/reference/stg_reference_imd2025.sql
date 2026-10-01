select
    lsoa_code_2021,
    index_of_multiple_deprivation_imd_decile_where_1_is_most_deprived_10_percent_of_lsoas as index_of_multiple_deprivation_decile,
    index_of_multiple_deprivation_imd_score as index_of_multiple_deprivation_score
from {{ ref('raw_reference_imd_2025') }}
