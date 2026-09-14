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
