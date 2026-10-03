{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind79('by_month') }}
