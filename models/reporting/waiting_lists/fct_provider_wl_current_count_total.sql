{{ config(materialized='view') }}

/*
Count of all active waiting lists per provider.

Clinical Purpose:
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    wl.provider_code,
    wl.provider_site_code,
    IFF(p.provider_code IS NOT NULL, TRUE, FALSE) AS "is_ncl_provider",
    COUNT(*) AS wl_current_total_count
FROM {{ ref('int_wl_current') }} wl
LEFT JOIN {{ ref('stg_reference_ncl_provider') }} p ON wl.provider_code = p.reporting_code
    AND p.ROW_TYPE IN ('trust','historic_nmuh')
GROUP BY 
ALL