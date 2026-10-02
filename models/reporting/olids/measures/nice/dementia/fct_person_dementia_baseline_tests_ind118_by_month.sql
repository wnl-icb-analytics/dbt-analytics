{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind118('by_month') }}
