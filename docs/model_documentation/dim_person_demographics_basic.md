# dim_person_demographics_basic pipeline

## Overview

This documentation covers versioning for the prod state of the dim_person_demographics_basic model ([DIM_PERSON_DEMOGRAPHICS_BASIC | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/REPORTING/schemas/POPULATION/table/DIM_PERSON_DEMOGRAPHICS_BASIC)).

A more raw view of the combined demographic data is available in the intermediate combined model ([INT_PERSON_PMI_COMBINED | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/MODELLING/schemas/POPULATION/table/INT_PERSON_PMI_COMBINED/data-preview)). This table contains the data source and event date for each patient used in the final table.

Currently the following datasets are used in the Demographics Basic table:
- PDS ([INT_PERSON_PMI_DATASET_PDS | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/MODELLING/schemas/POPULATION/table/INT_PERSON_PMI_DATASET_PDS))
- SUS ([INT_PERSON_PMI_DATASET_SUS | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/MODELLING/schemas/POPULATION/table/INT_PERSON_PMI_DATASET_SUS))
- Ethnicity National Data Sets ([INT_PERSON_PMI_DATASET_ETHNICITY_NATIONAL_DATA_SETS | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/MODELLING/schemas/POPULATION/table/INT_PERSON_PMI_DATASET_ETHNICITY_NATIONAL_DATA_SETS))

Note that the logic for the PDS Snapshot table largely follows the same logic as the Demographics Basic table but is limited to data available in PDS only (with ethnicity data supplemented using Ethnicity National Data Sets) ([DIM_SNAPSHOT_PERSON_PDS_DEMOGRAPHICS | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/REPORTING/schemas/POPULATION/table/DIM_SNAPSHOT_PERSON_PDS_DEMOGRAPHICS)).

## Version History
### V1.0.0 - 22/01/2026
* Initial version of dim_person_demographics_basic created.
* Initial data sources included:
    - PDS
    - SUS
    - Ethnicity National Data Sets ([ETHNICITY_NATIONAL_DATA_SETS | Table](https://app.snowflake.com/atkjncu/wnl/#/data/databases/MODELLING/schemas/LOOKUP_NCL/table/ETHNICITY_NATIONAL_DATA_SETS))
* Core demographic attributes (note these core fields are expanded using joins to lookup tables in the final table):
    - SK Patient ID (Pseudonymised NHS Number)
    - Gender
    - Date of Birth
    - Date of Death
    - Ethnicity Code (BK Ethnic)
    - Preferred Language (PDS only)
    - Interpreter Required Flag (PDS only)
    - LSOA 2021 Code of Residence
    - GP Practice Code of Registration
    - NCL Registered Flag (At time of refresh)
    - NCL Resident Flag (At time of refresh)

### V1.1.0 - 26/01/2026
* The logic to determine which dataset to pull from is more advanced:
  * Previous logic: Use PDS where it exists otherwise SUS (expect for ethnicity data where ETHNICITY_NATIONAL_DATA_SETS | Table is used when possible)
  * New logic: Custom per field but typically:
    * Use detailed values over unknowns/nulls (i.e. for gender, use 'Male' 'Female', 'Not specified' over 'Unknown' or NULL if possible) 
    * Use the most recent record across the datasets (2024 SUS data > 2022 PDS data)
* Cleaned up some of the ethnicity codes in the ethnicity data set and truncated to 1 digit (as 95% of codes only used 1 digit simplified ethnicity codes so why mix and match)
* Renamed resident fields to all include residence_ as a prefix for consistency
* Added some logic for registered borough and neighbourhood to replace NULLs with more informative values like 'Non-NCL' or 'Unknown due to practice closure'

### V1.1.1 - 02/03/2026
* Modified the registered GP practice logic to only consider non-PDS sources for the population outside of NCL (in order to reduce known false positives)
* Adjusted logic so that patients registered to a closed GP practice are now listed with no registered practice in the final output
* Added Residence ICB fields in the final output

### V2.0.0 - 26/06/2026
* Widened scope from NCL only to the full WNL footprint (NCL + NWL):
    * The LSOA geography lookup now derives an `NWL` `resident_flag`; in-area is `resident_flag in ('NCL','NWL')` rather than a single ICB code.
    * Registered population uses a WNL GP practice list (`stg_reference_gp_practice`, NCL + NWL).
    * Resident population grows from NCL ~1.88M to WNL ~4.83M; registered ~4.5M (NWL 2.77M + NCL 1.74M).
* Renamed the resident/registered flags to drop the NCL qualifier (source model `int_person_pds_ncl_population_flags` → `int_person_pds_population_flags`):
    * `flag_current_ncl_registered` → `flag_current_registered`
    * `flag_current_ncl_residence` → `flag_current_resident`
* Registered borough and sub-ICB now derived through ODS organisation relationships (`int_organisation_borough_mapping`) instead of a geographic lookup — one borough per practice (contractual, fixes the PCN-spanning-borough fan-out). Added `registered_sub_icb_code` / `registered_sub_icb_name` (93C = NCL, W2U3Z = NWL — the live sub-ICB location codes) so reports can filter back to a single ICB.
* Residence and registered neighbourhood now sourced from WNL reference tables (`WNLNEIGHBOURHOODS`, `WNL_GP_PRACTICE_NEIGHBOURHOOD`) covering all 13 NCL + NWL boroughs (previously NCL-only → null for NWL).
* Residence scope note: the V1.1.1 "non-PDS false positive" restriction now applies across the WNL footprint. Patients with an NWL LSOA who appear only in non-PDS sources (e.g. SUS) and are not in the PDS current-resident set now have a null residence LSOA / ICB group — previously they surfaced as "Other London" with an LSOA. ~1.44M patients, all `flag_current_resident = false`; the current-resident population is unchanged.
* Added an EPD prescribing PMI feeder (`int_person_pmi_dataset_prescribing`) — built but not yet combined into the dim (PDS is ~complete on LSOA/practice for WNL).
