{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind91('by_month') }}
