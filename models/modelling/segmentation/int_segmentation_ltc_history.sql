{{
    config(
        materialized='view',
        tags=['monthly-full']
    )
}}

-- Shared monthly membership for all 17 segmentation LTCs. A view: both parents
-- are already clustered on end_date and person_id, and both consumers rejoin
-- the spine, so a physical copy of the union would earn nothing.

SELECT
    person_id,
    end_date,
    condition_code,
    is_on_register
FROM {{ ref('int_segmentation_ltc_diagnosis_history') }}

UNION ALL

SELECT
    person_id,
    end_date,
    condition_code,
    is_on_register
FROM {{ ref('int_segmentation_ltc_rules_history') }}
