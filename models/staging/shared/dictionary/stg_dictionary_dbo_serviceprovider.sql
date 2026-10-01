select
    sk_service_provider_id,
    service_provider_code,
    service_provider_name,
    service_provider_type,
    sk_postcode_id,
    start_date,
    end_date,
    date_created,
    date_updated,
    sk_service_provider_group_id,
    is_active,
    is_main_site,
    is_test_organisation,
    is_dormant,
    service_provider_full_code
from {{ ref('raw_dictionary_dbo_serviceprovider') }}