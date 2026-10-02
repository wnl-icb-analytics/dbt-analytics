{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind232('by_month') }}
