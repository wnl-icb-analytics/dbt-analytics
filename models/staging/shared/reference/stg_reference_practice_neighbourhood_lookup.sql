select
    practicecode,
    practicename,
    pcncode,
    localauthority,
    practiceneighbourhood
from {{ ref('raw_reference_practice_neighbourhood_lookup') }}
