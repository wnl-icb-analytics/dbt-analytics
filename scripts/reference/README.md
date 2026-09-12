# MHSDS assessment definitions

Generate the reference seed from the public "MH Assessment Scales" worksheets
in the [current ETOS](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance)
and [archived specifications](https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-sets/mental-health-services-data-set/tools-and-guidance/mental-health-services-data-set-archived-specification).
The committed seed uses v4.1, v5 and ETOS v6.0.7.1. Keep the downloaded workbooks outside
version control.

Run with Python and openpyxl:

```powershell
python scripts/reference/generate_mhsds_assessment_seed.py --spec '4.1=path/to/mhsds_v4.1_tos.xlsm' --spec '5.0=path/to/v5.xlsx' --spec '6.0.7.1=path/to/etos-mhsds-v6.0.7.1.xlsm' --output seeds/mhsds_assessment_scale_definitions.csv
```

The extractor reads merged v4.1/v5 cells and explicit v6 rows. It includes both
active and inactive concept IDs and fails on conflicting definitions within a
version. Each seed row records its source version and worksheet row. Review
the diff against the workbook before building the seed and downstream references.

`mhsds_assessment_scale` supplies the latest available definition for each
observable. `mhsds_assessment_response` retains the latest definition of every
historical concept/response pair. These labels do not validate historical
submissions against a later specification or derive whole-questionnaire totals.

UCUM units use UKHFD through `scripts/sources`, not this seed extractor. Their
lookup is case-sensitive and retains all historical codes. Source absence and
terminology status are separate fields.

# NHS Talking Therapies (IAPT data set) definitions

Generate the outcome measure seed from the "ROM Mapping" sheets of the
[v2.0 TOS](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/iapt/iapt-v2.0-docs/iapt_v2.0_technical_output_specification_v_2.0.26.xlsx)
and the [v2.1 ETOS](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/iapt/iapt-v2.1-docs/iapt_v_2.1_enhanced_technical_output_specification_v2.1.22.xlsx),
and the therapy type seed from the
[terminology mapping guidance](https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/datasets/iapt/iapt-v2.0-docs/iapt_v2_terminology_mapping_guidance_v3.4.xlsx)
plus the v2.1 ETOS therapy derivations:

```powershell
python scripts/reference/generate_iapt_assessment_seed.py --spec '2.0.26=path/to/iapt_v2.0_tos.xlsx' --spec '2.1.22=path/to/iapt_v2.1_etos.xlsx' --output seeds/iapt_assessment_scale_definitions.csv
python scripts/reference/generate_iapt_therapy_type_seed.py --guidance '3.4=path/to/iapt_v2_terminology_mapping_guidance_v3.4.xlsx' --etos '2.1.22=path/to/iapt_v2.1_etos.xlsx' --output seeds/iapt_therapy_type_definitions.csv
```

Each row records the specification that published it, not the revision a provider
implemented: TOS 2.0.27 changed the Diabetes Distress Scale inside data set
version 2.0. Facts read scores against the latest definition and treat values
that fit only an earlier range as historical formats. `Not Applicable` responses and the WSAS work value 9 ("indicates the score is
not available") are marked as non-score responses.
