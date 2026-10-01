-- WNL registered neighbourhood: practice -> neighbourhood, NCL + NWL (icb_group).
-- Blank neighbourhood (federated / extended-access non-geographic practices)
-- normalised to null.
select
    practice_code,
    practice_name,
    nullif(trim(neighbourhood_code), '') as neighbourhood_code,
    nullif(trim(neighbourhood_name), '') as neighbourhood_name,
    icb_group
from {{ ref('raw_reference_wnl_gp_practice_neighbourhood') }}
