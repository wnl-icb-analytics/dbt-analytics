# Reference catalogue

TRUD ingestion belongs in `DATA_LAKE.TERMINOLOGY`. Its raw releases, history,
working tables, run logs and locally inverted maps are inputs, not the analyst
reference catalogue. `scripts/sources/source_mappings.yml` defines the source;
the source-generation scripts create its YAML and raw models. Hand-written
staging models provide the source interfaces used by dbt reference models.

## Schema boundaries

`REFERENCE.DATA_DICTIONARY` contains NHS dataset definitions, administrative
codes and dataset-specific response rules. The CSDS and MHSDS assessment
scales and responses belong here because ETOS defines their published values
and interpretation. The restricted historical CSDS unit aliases also belong
here. Their SNOMED keys do not make these definitions a general terminology.

`REFERENCE.TERMINOLOGY` contains clinical coding systems, classification maps,
measurement terminology and clinical code sets. SNOMED, Read, UCUM and the
patient-referral code set remain here. The general clinical unit lookup stays
with UCUM; the CSDS-specific aliases sit in the data dictionary.

## Analyst-facing terminology names

| Object | One row represents |
|---|---|
| `SNOMED_CONCEPT` | One recognised current or historical concept with its latest retained label |
| `READ_CODE` | One exact-case code within Read v2 or CTV3 |
| `ICD10_CODE` | One undotted ICD-10 code and its latest available label |
| `OPCS4_CODE` | One undotted OPCS-4 code and its latest available label |
| `SNOMED_TO_ICD10_MAP` | One published forward map member, identified by SNOMED code, block, group and priority |
| `SNOMED_TO_OPCS4_MAP` | One published forward map member, also identified by target OPCS version |
| `UCUM_UNIT` | One historical case-sensitive unit code with its latest available definition |
| `UCUM_UNIT_HISTORY` | One unit-code definition revision |
| `CLINICAL_UNIT_OF_MEASUREMENT` | One exact-case unit code or supported alias |
| `PATIENT_REFERRAL_SNOMED_CODES` | One code in the agreed referral code set |
| `PCD_REFSET_LATEST` | One active membership in the latest Primary Care Domain release |
| `PCD_REFSET_SNAPSHOTS` | One Primary Care Domain membership within a release snapshot |

The PCD names distinguish current membership from release snapshots. They do
not imply that every historically valid member remains in the latest set.
The new dbt names omit ingestion suffixes such as `__LATEST`; no aliases for
those landing-table names are published in `REFERENCE`.

Classification labels prefer current TRUD titles. Codes found only in the
warehouse dictionary retain its latest available definition, so a missing
current TRUD title does not discard an older code. `definition_source` tells
analysts which reference supplied the label. This is not a complete historical
archive of classification releases.

The forward maps retain all blocks, groups, priorities, rules and advice.
They do not choose a target for a patient. The
[WHO terminology mapping guide](https://cdn.who.int/media/docs/default-source/classification/who-fic-network/whofic_terminology_mapping_guide.pdf)
describes the UK map-block structure. The
[NHS implementation guide](https://digital.nhs.uk/binaries/content/assets/website-assets/isce/scci0034/0034352016guidance-v2.pdf)
states that the published forward maps cannot be used in reverse. The locally
inverted landing tables therefore have no dbt reference counterpart.

## Deployment

Build the five new terminology models with their dependencies and the five
moved dictionary models. The SNOMED concept reference includes its NHS Digital
staging interfaces and can deploy independently of the longitudinal adapters. Existing `ref()` names for the moved models are
unchanged, so dbt resolves them to `DATA_DICTIONARY` after deployment.

Compile `analyses/reference/retire_old_assessment_references.sql` for the
deployment target, then execute the compiled SQL. It compares counts and
all-column fingerprints before dropping the five old terminology tables.
It creates no compatibility aliases. Run this after the branch is deployed,
so a scheduled build does not recreate models from the old folder paths.

## Validation

The DEV build passed 62 tests across 35 models and two existing seeds. Both
forward maps retain their source counts: 212,089 ICD-10 map members and
147,598 OPCS-4 map members. Every source SNOMED code has a label. Three ICD-10
targets have no supported title; their codes remain with null descriptions.

The code references contain 18,264 ICD-10 and 12,178 OPCS-4 codes, all labelled.
This includes 330 ICD-10 and 56 OPCS-4 codes supplied by the warehouse
dictionary beyond the current TRUD titles. These counts describe the retained
reference data, not patient records.
