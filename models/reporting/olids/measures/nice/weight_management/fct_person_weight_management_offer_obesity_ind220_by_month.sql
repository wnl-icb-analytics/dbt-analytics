{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind220('by_month') }}
