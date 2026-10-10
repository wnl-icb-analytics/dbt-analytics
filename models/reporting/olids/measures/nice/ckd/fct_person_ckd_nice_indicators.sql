{{ config(materialized='table') }}

-- Common long-form interface for the NICE chronic kidney disease indicator views.
-- Detail columns a view does not emit are typed nulls for its branch.
{{ nice_ckd_union('current') }}
