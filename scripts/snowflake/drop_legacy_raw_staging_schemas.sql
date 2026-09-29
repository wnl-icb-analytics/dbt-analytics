/*
    Drop legacy raw/staging schemas from MODELLING

    Raw and staging models moved to the STAGING database (DBT_RAW + one schema
    per source system). These schemas only ever fed dbt models, so they are
    safe to drop once a full prod and dev build has completed against STAGING.

    Run as ENGINEER (or any role owning MODELLING). One-off; the
    CLEANUP_ORPHANED_DBT_OBJECTS task would otherwise remove the objects
    after 21 days but leave the empty schemas behind.
*/

DROP SCHEMA IF EXISTS MODELLING.DBT_RAW;
DROP SCHEMA IF EXISTS MODELLING.DBT_STAGING;
DROP SCHEMA IF EXISTS DEV__MODELLING.DBT_RAW;
DROP SCHEMA IF EXISTS DEV__MODELLING.DBT_STAGING;

-- stg_mhcorl previously used an explicit schema override into MODELLING.MH.
-- That schema also holds analyst-managed (non-dbt) objects, so drop only the
-- dbt table, not the schema.
DROP TABLE IF EXISTS MODELLING.MH.STG_MHCORL;
DROP TABLE IF EXISTS DEV__MODELLING.MH.STG_MHCORL;

-- Three cancer staging views were co-located with the analyst-managed
-- CANCER__REF schema via a schema override; they now build to
-- STAGING.REFERENCE. Drop only the dbt views; the schema stays.
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_LSOA_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_PROVIDER_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS MODELLING.CANCER__REF.STG_REFERENCE_CANCER_SUB_ICB_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_LSOA_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_PROVIDER_TO_CANCER_ALLIANCE;
DROP VIEW IF EXISTS DEV__MODELLING.CANCER__REF.STG_REFERENCE_CANCER_SUB_ICB_TO_CANCER_ALLIANCE;

-- The cLTCS EMIS-extract models briefly built to a CLTCS schema before being
-- folded into C_LTCS with the rest of the programme's staging models.
DROP SCHEMA IF EXISTS STAGING.CLTCS;
DROP SCHEMA IF EXISTS DEV__STAGING.CLTCS;

-- SLAM staging models (PR #798) were dev-tested into per-feed schemas in
-- DEV__MODELLING before the STAGING database existed. MODELLING.LSACM/
-- LSDEPLCM/LSDRPLCM hold analyst-managed views reading DATA_LAKE directly,
-- so drop only the dbt objects; the schemas stay.
DROP TABLE IF EXISTS DEV__MODELLING.LSACM.STG_LSACM;
DROP VIEW IF EXISTS DEV__MODELLING.LSACM.STG_LSACM_LATEST;
DROP TABLE IF EXISTS DEV__MODELLING.LSDEPLCM.STG_LSDEPLCM;
DROP VIEW IF EXISTS DEV__MODELLING.LSDEPLCM.STG_LSDEPLCM_LATEST;
DROP TABLE IF EXISTS DEV__MODELLING.LSDRPLCM.STG_LSDRPLCM;
DROP VIEW IF EXISTS DEV__MODELLING.LSDRPLCM.STG_LSDRPLCM_LATEST;
DROP TABLE IF EXISTS DEV__MODELLING.LSPLCM.STG_LSPLCM;
DROP VIEW IF EXISTS DEV__MODELLING.LSPLCM.STG_LSPLCM_LATEST;

-- The acute SLAM per-feed schemas consolidated into STAGING.SLAM (with the MH
-- Local SLAM models, PR #862); the SLAM tables were cloned into STAGING.SLAM
-- so the incrementals continue. Drop the old per-feed schemas once a prod and
-- dev build has completed against STAGING.SLAM. The dev-only MH_SLAM schema
-- predates the consolidation.
DROP SCHEMA IF EXISTS STAGING.LSACM;
DROP SCHEMA IF EXISTS STAGING.LSPLCM;
DROP SCHEMA IF EXISTS STAGING.LSDRPLCM;
DROP SCHEMA IF EXISTS STAGING.LSDEPLCM;
DROP SCHEMA IF EXISTS DEV__STAGING.LSACM;
DROP SCHEMA IF EXISTS DEV__STAGING.LSPLCM;
DROP SCHEMA IF EXISTS DEV__STAGING.LSDRPLCM;
DROP SCHEMA IF EXISTS DEV__STAGING.LSDEPLCM;
DROP SCHEMA IF EXISTS DEV__STAGING.MH_SLAM;

-- MHCORL moved from the retired DATA_LAKE__NCL.SDL copy to DATA_LAKE.SDL
-- (source sdl_wnl, raw_sdl_wnl_mhcorl). The old raw view for the manual
-- 'sdl' source is no longer generated.
DROP VIEW IF EXISTS STAGING.DBT_RAW.RAW_SDL_MHCORL;
DROP VIEW IF EXISTS DEV__STAGING.DBT_RAW.RAW_SDL_MHCORL;
