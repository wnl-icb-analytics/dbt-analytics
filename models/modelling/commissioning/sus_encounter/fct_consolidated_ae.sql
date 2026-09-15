{{
    config(materialized = 'table')
}}

--------------------------------------------------------------------------------
-- Consolidated SUS A&E activity.
--
-- Migrated from the SQL Server view dbo.consolidated_view_ae, which selected
-- from dbo.fact_ae_current, renamed every column, and joined to the control
-- table dbo.proc_consolidate2.
--
-- WHAT CHANGED, AND WHY
--
-- The legacy view filtered on proc_consolidate2.flag_current = 1. That flag was
-- a VINTAGE SELECTOR, not an approval: SUS data used to arrive three times for
-- the same period (current, then rec, then postrec), and each provider/period
-- combination was flagged for whichever vintage should be used. Sibling views
-- served the rec and postrec combinations, and the three were unioned to make
-- the full consolidated dataset.
--
-- Only 6,487 of the 20,364 AE combinations in proc_consolidate2 are flagged
-- current; 13,648 are postrec and 172 rec. Reproducing the filter literally
-- would therefore drop about two thirds of the history, because there is no
-- rec or postrec source in Snowflake to serve those combinations.
--
-- Rec and postrec have been retired and nothing downstream consumes them, so
-- the flag has nothing left to select between. It is dropped, and every
-- combination flows through. proc_consolidate2 and its supporting lookup
-- (dmic_reference.dbo.SUS_ApplicableCostingPeriod) are consequently NOT
-- migrated.
--
-- The row-count totals the control table carried (current_total, rec_total,
-- postrec_total) were never read by this view -- they existed for whoever was
-- deciding vintages. They are not reproduced here. If the counts are wanted
-- for monitoring, they belong in a separate model.
--
-- COLUMN NAMING
--
-- The legacy view renamed every column to spaceless CamelCase
-- ([Age At CDS Activity Date] -> AgeAtCDSActivityDate). Snake case is used here
-- instead, per repo convention, so the names carry through unchanged from
-- fct_sus_ae_monthly.
--------------------------------------------------------------------------------

with ae as (

    select * from {{ ref('fct_sus_ae_monthly') }}

),

prov_mergers as (

    -- Superseded provider codes and their replacements. distinct is a fan-out
    -- guard: the seed has a uniqueness test, but this join must not depend on
    -- it holding.
    select distinct provfrom, provto
    from {{ ref('ref_prov_mergers') }}

)

select
    ae.*

    ------------------------------------------------------------------
    -- Provider code with mergers applied, from dbo.getProvMerge.
    -- The function returned the merged code where one existed and the
    -- original otherwise, which is what the coalesce does.
    ------------------------------------------------------------------
    ,coalesce(pm.provto, ae.organisation_code_code_of_provider)
        as z_organisation_code_code_of_provider_merged

from ae

left join prov_mergers pm
    on ae.organisation_code_code_of_provider = pm.provfrom

------------------------------------------------------------------
-- The only genuine business filter in the legacy view.
--   0 = out-of-area commissioner
--   1 = in-area commissioner
--   2 = duplicate attendance, excluded here
-- The legacy view also required Applicable Costing Period and
-- Organisation Code (Code of Provider) to be non-null, but that was an
-- artefact of the join to proc_consolidate2 rather than a business rule,
-- so it is not reproduced. See the note below if row counts differ.
------------------------------------------------------------------
where ae.z_commissioning_access in (0, 1)
