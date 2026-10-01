
select
    ods_code                                as provider_code,
    ods_name                                as provider_name,
    cancer_alliance_name                    as cancer_alliance_name

from {{ ref('provider_ods_to_cancer_alliance') }}
