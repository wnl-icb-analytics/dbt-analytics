{{
    config(
        description="Raw layer (Imaging diagnostic turnaround-time provider submissions (raw landing)). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TAT.TURNAROUND_TIMES_RAW \ndbt: source(''tat'', ''TURNAROUND_TIMES_RAW'') \nColumns:\n  EthnicCategory -> ethnic_category\n  PersonGender -> person_gender\n  GeneralMedicalPractice -> general_medical_practice\n  PatientSourceType -> patient_source_type\n  ReferrerCode -> referrer_code\n  ReferringOrg -> referring_org\n  ReferringOrganisation -> referring_organisation\n  DiagnosticTestRequestDateTime -> diagnostic_test_request_date_time\n  DiagnosticTestRequestReceivedDateTime -> diagnostic_test_request_received_date_time\n  DiagnosticTestDateTime -> diagnostic_test_date_time\n  ServiceReportIssueDateTime -> service_report_issue_date_time\n  Imaging Code Nicip -> imaging_code_nicip\n  ImagingCodeNicip -> imaging_code_nicip_1\n  Imaging Code Snomed -> imaging_code_snomed\n  ImagingCodeSnomed -> imaging_code_snomed_1\n  Provider Site Code -> provider_site_code\n  ProviderSiteCode -> provider_site_code_1\n  PriorityTypeCode -> priority_type_code\n  CancerPathwayFlag -> cancer_pathway_flag\n  Month -> month\n  SOURCE_FILE -> source_file\n  LOADED_AT -> loaded_at"
    )
}}
select
    "EthnicCategory" as ethnic_category,
    "PersonGender" as person_gender,
    "GeneralMedicalPractice" as general_medical_practice,
    "PatientSourceType" as patient_source_type,
    "ReferrerCode" as referrer_code,
    "ReferringOrg" as referring_org,
    "ReferringOrganisation" as referring_organisation,
    "DiagnosticTestRequestDateTime" as diagnostic_test_request_date_time,
    "DiagnosticTestRequestReceivedDateTime" as diagnostic_test_request_received_date_time,
    "DiagnosticTestDateTime" as diagnostic_test_date_time,
    "ServiceReportIssueDateTime" as service_report_issue_date_time,
    "Imaging Code Nicip" as imaging_code_nicip,
    "ImagingCodeNicip" as imaging_code_nicip_1,
    "Imaging Code Snomed" as imaging_code_snomed,
    "ImagingCodeSnomed" as imaging_code_snomed_1,
    "Provider Site Code" as provider_site_code,
    "ProviderSiteCode" as provider_site_code_1,
    "PriorityTypeCode" as priority_type_code,
    "CancerPathwayFlag" as cancer_pathway_flag,
    "Month" as month,
    "SOURCE_FILE" as source_file,
    "LOADED_AT" as loaded_at
from {{ source('tat', 'TURNAROUND_TIMES_RAW') }}
