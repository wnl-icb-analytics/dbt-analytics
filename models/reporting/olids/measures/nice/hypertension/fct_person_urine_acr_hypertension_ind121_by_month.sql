{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind121('by_month') }}
