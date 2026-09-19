{#
    The 30 active A&E business rules, migrated from dbo.br_rules_updates.

    In SQL Server these lived as SQL text in a Conditions column, executed one
    at a time by a cursor in dbo.processBusinessRules. Here they are ordinary
    predicates: the caller loops over this list once and emits a flag per rule,
    so there is no dynamic SQL, no cursor, and no pivot table.

    Returns a list of dicts, each with:
        code           -- rule name, as it appears in z_business_rule
        contract_type  -- the contract type code the rule maps to
        seqno          -- legacy sequence number, retained for traceability
        condition      -- Snowflake boolean expression

    Only dataset = 'AE' and active = 1 rows are represented. The inactive rows
    in the legacy table are deliberately not carried over.

    COLUMN NOTES
      * organisation_code_code_of_provider is already truncated to 3 characters
        upstream, so the legacy LEFT(...,3) wrappers are dropped. Where a rule
        compared that column to a 5-character value (e.g. 'NTP59', 'AD904') the
        comparison could never be true in the legacy system either; those are
        preserved as written and marked [DEAD].
      * z_provider_site_code and z_financial_year are derived columns, so this
        macro must be called AFTER they are computed.

    STUBBED COLUMNS -- three rules reference columns that are currently NULL in
    int_sus_ae_monthly. Their other branches still fire, so the rules are
    partially live. Marked [TODO] inline; they will populate themselves once
    the columns are sourced.
        applicable_costing_period          -> UCC_ChelWest
        nhs_rid_from_provider              -> WIC_TEDD, UCC_WMX
        org_code_local_patient_identifier  -> UCC_THH

    SUBSTRING SAFETY -- the legacy procedure converted rule names to contract
    type codes by repeated string replace(), which corrupts any name that
    contains another name. No collisions exist among these 30 active rules, so
    the lookup approach used by the caller is exactly equivalent. Note that the
    INACTIVE rule DUP_HMI is a substring of DUPLICATE_HMI: if it is ever
    reactivated, legacy and dbt output will diverge.
#}

{% macro ae_business_rules() %}

{% set rules = [

    {'code': 'MONO_AE_WEYE', 'contract_type': 1, 'seqno': 16,
     'condition': "em_department_type = '02'
        and unique_cds_identifier like 'BRYJ07%'
        and organisation_code_code_of_provider in ('RYJ', 'RJ5')"},

    {'code': 'MIU_MV', 'contract_type': 1, 'seqno': 40,
     'condition': "organisation_code_code_of_provider = 'RAS'
        and em_department_type in ('03', '3')"},

    {'code': 'DUPLICATE_LNWHT', 'contract_type': 6, 'seqno': 72,
     'condition': "(organisation_code_code_of_provider = 'RV8'
            and to_date(arrival_date) between '2015-04-01' and '2015-04-30')
        or (to_date(arrival_date) between '2015-11-01' and '2015-12-31'
            and provider_site_code = 'NTP59')"},

    {'code': 'UCC_CMX', 'contract_type': 3, 'seqno': 73,
     'condition': "(em_department_type in ('03', '3')
            and provider_site_code in ('NTP60', 'RV847', 'R1K12'))
        or (organisation_code_code_of_provider = 'R1K'
            and left(unique_cds_identifier, 5) = 'NTP60')
        or (em_department_type in ('03', '3')
            and provider_site_code = 'AD918'
            and z_financial_year >= '1819')"},

        {'note': '[PARITY] The legacy AND/OR precedence here is almost certainly not what the author intended: the first OR splits the whole expression, so provider_site_code in (RC300,RC368,R1K04) fires on its own without the department type check. Reproduced exactly as written.',
     'code': 'AE_EHT', 'contract_type': 1, 'seqno': 76,
     'condition': "(em_department_type in ('01', '1')
            and organisation_code_code_of_provider = 'RC3')
        or provider_site_code in ('RC300', 'RC368', 'R1K04')
        or (organisation_code_code_of_provider = 'R1K'
            and left(unique_cds_identifier, 2) = 'ED')"},

    {'code': 'AE_NP', 'contract_type': 1, 'seqno': 77,
     'condition': "(em_department_type in ('01', '1')
            and provider_site_code in ('RV820', 'R1K01'))
        or (organisation_code_code_of_provider = 'R1K'
            and left(unique_cds_identifier, 2) = 'NP')"},

        {'note': '[PARITY] Same precedence quirk: the trailing NOT IN (NP,ED) binds only to the second branch, not the first. As written in legacy.',
     'code': 'DUPLICATE_CMX', 'contract_type': 6, 'seqno': 78,
     'condition': "(em_department_type in ('03', '3')
            and organisation_code_code_of_provider = 'RV8'
            and provider_site_code = 'RV847'
            and to_date(arrival_date) between '2012-03-01' and '2012-03-31')
        or (provider_site_code = 'NTP60'
            and to_date(arrival_date) between '2015-11-01' and '2015-12-31'
            and left(unique_cds_identifier, 2) not in ('NP', 'ED'))"},

        {'note': '[TODO] applicable_costing_period is a NULL stub; second branch is dead until the ETL.udf_ACPFromActivityDate migration lands.',
     'code': 'UCC_ChelWest', 'contract_type': 1, 'seqno': 79,
     'condition': "provider_reference_no = 'RQM25'
        or (applicable_costing_period = 'M1 - Apr 2015'
            and organisation_code_code_of_provider = 'RAT')"},

        {'note': '[TODO] nhs_rid_from_provider is a NULL stub; first branch is dead.',
     'code': 'WIC_TEDD', 'contract_type': 2, 'seqno': 81,
     'condition': "(organisation_code_code_of_provider = 'RY9'
            and left(nhs_rid_from_provider, 5) = 'RY902'
            and z_financial_year = '1617')
        or (organisation_code_code_of_provider = 'RY9'
            and provider_site_code = 'RY900'
            and z_financial_year <> '1617')
        or (organisation_code_code_of_provider = 'RY9'
            and provider_site_code = 'RY902'
            and z_financial_year in ('1718', '1819', '1920'))"},

        {'note': '[DATA ISSUE] contract_type 99 collides with the "no rule matched" sentinel, so an attendance matching only this rule is treated as unmatched and resolved by the SLA lookup. Preserved as-is; worth raising with the rules owner. [TODO] nhs_rid_from_provider is a NULL stub; first branch is dead.',
     'code': 'UCC_WMX', 'contract_type': 99, 'seqno': 82,
     'condition': "(organisation_code_code_of_provider = 'RY9'
            and left(nhs_rid_from_provider, 5) = 'RY901'
            and z_financial_year = '1617')
        or (organisation_code_code_of_provider = 'RY9'
            and provider_site_code = 'RY9'
            and z_financial_year <> '1617')
        or (organisation_code_code_of_provider = 'RY9'
            and provider_site_code = 'RY901'
            and z_financial_year >= '1718')"},

        {'note': '[PARITY] AND binds tighter than OR, so the financial year list applies only to the first branch. As written in legacy.',
     'code': 'UCC_NP', 'contract_type': 3, 'seqno': 83,
     'condition': "(organisation_code_code_of_provider in ('RV8', 'R1K')
            and provider_site_code in ('RV846', 'R1K11')
            and z_financial_year in ('1213', '1314', '1415', '1516', '1617'))
        or (provider_site_code = 'AD906' and z_financial_year >= '1718')"},

        {'note': '[DEAD] NTP59 is 5 characters and the provider column holds 3, so that list member can never match. Preserved from legacy.',
     'code': 'UCC_EHT', 'contract_type': 3, 'seqno': 89,
     'condition': "em_department_type in ('03', '3')
        and (organisation_code_code_of_provider in ('RC3', 'NTP59')
            or provider_site_code in ('RC304', 'R1K14', 'AD915')
            or (organisation_code_code_of_provider = 'R1K'
                and left(unique_cds_identifier, 5) = 'NTP59'))"},

        {'note': '[TODO] org_code_local_patient_identifier is a NULL stub. [DEAD] AD904 is 5 characters against a 3-character provider column. [PARITY] the department type check binds only to the final branch.',
     'code': 'UCC_THH', 'contract_type': 3, 'seqno': 90,
     'condition': "org_code_local_patient_identifier = 'AD904'
        or organisation_code_code_of_provider = 'AD904'
        or provider_code_original_data = 'AD904'
        or (provider_site_code = 'AD904'
            and organisation_code_code_of_provider = 'AD9'
            and em_department_type in ('03', '3'))"},

    {'code': 'AE_ChelWest', 'contract_type': 1, 'seqno': 91,
     'condition': "organisation_code_code_of_provider in ('RQM', 'RFW')
        and provider_reference_no = 'RQM01'"},

        {'note': '[PARITY] the final OR is outside the provider check in legacy.',
     'code': 'AE_WMX', 'contract_type': 1, 'seqno': 92,
     'condition': "(organisation_code_code_of_provider in ('RQM', 'RFW')
            and (provider_reference_no = 'RFW01'
                or unique_cds_identifier like '%RFW00%'
                or provider_reference_no = 'RQM91'))
        or provider_site_code = 'RQM91'"},

    {'code': 'UCC_STCharles', 'contract_type': 1, 'seqno': 113,
     'condition': "provider_code_original_data = 'RYX01'
        or provider_site_code = 'RYX01'"},

    {'code': 'WIC_EDG', 'contract_type': 1, 'seqno': 114,
     'condition': "provider_code_original_data = 'RYX24'
        or provider_site_code = 'RYX24'"},

    {'code': 'WIC_FINCH', 'contract_type': 1, 'seqno': 115,
     'condition': "provider_code_original_data = 'RYX23'
        or provider_site_code = 'RYX23'"},

    {'code': 'WIC_PGREEN', 'contract_type': 1, 'seqno': 116,
     'condition': "provider_code_original_data = 'RYX03'
        or provider_site_code in ('RYX03', 'RYX11')"},

    {'code': 'WIC_SOHO', 'contract_type': 1, 'seqno': 117,
     'condition': "provider_code_original_data = 'RYX02'
        or provider_site_code = 'RYX02'"},

    {'code': 'AE_GSST', 'contract_type': 1, 'seqno': 118,
     'condition': "organisation_code_code_of_provider = 'RJ1'
        and em_department_type in ('01', '02', '1', '2')"},

    {'code': 'AE_KCH', 'contract_type': 1, 'seqno': 119,
     'condition': "organisation_code_code_of_provider = 'RJZ'
        and em_department_type in ('01', '02', '1', '2')"},

    {'code': 'AE_UCLH', 'contract_type': 1, 'seqno': 120,
     'condition': "organisation_code_code_of_provider = 'RRV'
        and em_department_type in ('01', '02', '1', '2')"},

    {'code': 'UCC_GSST', 'contract_type': 1, 'seqno': 121,
     'condition': "organisation_code_code_of_provider = 'RJ1'
        and em_department_type in ('03', '3', '04', '4')"},

        {'note': '[PARITY] legacy compared [EM Department Type] = 1 as a NUMBER, not the string 01. T-SQL coerced the varchar column, so 1 and 01 both matched but 1  or non-numeric values would error. Written here as an explicit two-value list, which is the same set in practice.',
     'code': 'AE_SMH', 'contract_type': 1, 'seqno': 122,
     'condition': "z_provider_site_code like 'RYJ01%'
        and em_department_type in ('01', '1')"},

    {'code': 'AE_CX', 'contract_type': 1, 'seqno': 123,
     'condition': "z_provider_site_code like 'RYJ02%'
        and em_department_type in ('01', '1')"},

        {'note': '[PARITY] legacy used LIKE with no wildcard, i.e. an exact match.',
     'code': 'UCC_CX', 'contract_type': 1, 'seqno': 124,
     'condition': "z_provider_site_code = 'RYJ02'
        and em_department_type = '03'"},

    {'code': 'UCC_HH', 'contract_type': 1, 'seqno': 125,
     'condition': "z_provider_site_code = 'RYJ03'
        and em_department_type = '03'"},

        {'note': '[PARITY] legacy compared zFinancialYear to unquoted 1617, a numeric literal against a varchar column; T-SQL coerced the column to int. Kept as string comparison, which is equivalent for 4-digit values.',
     'code': 'UCC_SMH', 'contract_type': 1, 'seqno': 126,
     'condition': "(z_provider_site_code = 'RYJ01' and em_department_type = '03')
        or (z_financial_year = '1617' and z_provider_site_code = 'NLO03'
            and em_department_type = '03')
        or (z_financial_year > '1617'
            and organisation_code_code_of_provider = 'NLO'
            and em_department_type = '03')"},

        {'note': '[DEAD] HMI against a 3-character provider column is valid, unlike the 5-character cases above -- this one can fire.',
     'code': 'DUPLICATE_HMI', 'contract_type': 1, 'seqno': 127,
     'condition': "organisation_code_code_of_provider = 'HMI'"}

] %}

{{ return(rules) }}

{% endmacro %}