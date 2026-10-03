{{ config(materialized='view' , tags=['monthly-full', 'nice-history']) }}

{{ nice_ind261('by_month') }}
