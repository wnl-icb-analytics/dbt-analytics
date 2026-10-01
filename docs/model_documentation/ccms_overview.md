# Cambridge Multimorbidity Score Overview

## Purpose

The pipeline computes a person-level modified Cambridge Comorbidity Score (CCMS) (Ruby et al., 2023; implemented in alignment with the modified score) for adults aged 16+ to support risk stratification and downstream modelling.
This implementation has been migrated from the London AIC version of CCMS. For migration details, assumptions, and change history, review the pull request.

## Outstanding issues
- There is a dependency on an AIC S3 bucket of defintitions [ staged CCMS codelists (`stg_common_ccmc_snomed`, `stg_common_ccmc_dmd`)] that should be reviewed.
- Exact model version that the weights are taken from must be confirmed with the AIC centre
- Local unit testing

## Relevant Literature
- Modified CCMS implemented here, 21-condition version Table 2: Ruby et al., (2023) *Development of a modified Cambridge Multimorbidity Score for use with SNOMED CT: an observational English primary care sentinel network study* [Open source URL](https://bjgp.org/content/73/731/e435) 
- Original CCMS: Payne et al., (2020) *Development and validation of the Cambridge Multimorbidity Score* [Open source URL](https://www.cmaj.ca/content/192/5/E107?ijkey=b4745227e4040e3b66e0da42da9e3ceb3500c3a3&keytype2=tf_ipsecsha)

## Model Flow
The pipeline uses staged CCMS codelists (`stg_common_ccmc_snomed`, `stg_common_ccmc_dmd`) and OLIDS observations/medication data.

1. `int_ccms_medication_orders`
   - Filters medication orders to CCMS dm+d codelists.
   - Applies a 12-month lookback for most medication features, with an all-time exception for lithium.

2. `int_ccms_intermediate_flags`
   - Creates one row per eligible person with raw binary flags from:
     - SNOMED observation signals
     - Medication thresholds
     - eGFR logic (two most recent values, both < 60)
   - Applies CCMS lookback rules (mixed 12-month, 5-year, and all-time logic depending on condition).
   - Filters cohort to >= 16 years

3. `int_ccms_final_flags`
   - Maps raw flags into Cambridge condition groupings used for scoring.
   - Combines related clinical and medication proxies into the final set of score inputs.

4. `int_ccms_score`
   - Applies published weighted coefficients to final flags.
   - Outputs a person-level `cambridge_comorbidity_score`.

5. `dim_person_ccms`
   - Mart for downstream use