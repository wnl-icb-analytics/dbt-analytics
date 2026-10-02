{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind99('by_month') }}
