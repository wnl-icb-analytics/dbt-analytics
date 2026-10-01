select
    sk_organisation_id_practice,
    practice_code,
    practice_name,
    sk_organisation_id_network,
    network_code,
    network_name,
    sk_organisation_id_commissioner,
    commissioner_code,
    commissioner_name,
    sk_organisation_id_stp,
    stp_code,
    stp_name
from {{ ref('raw_dictionary_dbo_organisationmatrixpracticeview') }}