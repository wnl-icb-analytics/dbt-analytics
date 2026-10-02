# ECDS QuickReport crosswalk and aggregate checks

15 September 2026. Read-only review of Hana Ali's `ECDS - Quick Report.sql` attachment against the current production dbt models. No model or warehouse table was changed. The proposed output is one row per ECDS attendance. The legacy SQL Server view has not been queried here, so these are dbt-side baselines, not a completed reconciliation.

## Field and measure crosswalk

| QuickReport item | Current dbt source | Intended implementation and decision |
| --- | --- | --- |
| Registered practice | `obt_encounter_uec.reg_practice_at_event` | Use the practice submitted for the attendance. |
| PCN | `stg_reference_primary_care_practice_all` | Use the practice submitted for the attendance and its latest maintained PCN mapping. This deliberately does not reconstruct historical PCN membership. |
| INT / neighbourhood team | `stg_reference_primary_care_practice_all` | Hana means integrated neighbourhood team. Use the practice submitted for the attendance and its latest maintained neighbourhood mapping. |
| Mental health flag | `int_sus_uec_referred_to_service.referred_to_service_ecds_group1` | Flag attendances with a psychiatric referred-to service. Reduce referral records to distinct `visit_occurrence_id` before joining to attendances. The old view took its group from `REF_REFERRED_TO_SERVICE`, not from investigations. Confirm whether any matching referral is intended; the old view held the first referral. |
| CYP mental health flag | `obt_encounter_uec.age_at_event`, chief complaint and injury intent; `int_sus_uec_diagnosis.source_concept_code` | Restrict to age under 18 at arrival. Hana agreed to use any recorded diagnosis. Reduce matching diagnoses to distinct attendances before joining. Use the supplied code list only after its owner and version are recorded. |
| Attendances | One `obt_encounter_uec` row per attendance | Reporting count is 1 except the agreed historical Totally site/department/discharge combination, when it is 0. Retain all source attendance rows. |
| Admitted and non-admitted | `obt_encounter_uec.discharge_destination_code`; `stg_dictionary_ecds_dischargedestination.ecds_group1` | The old admitted rule uses the `Admitted`/`Transfer` groups and two short-stay codes. The separate non-admitted expression excludes null or unmapped destinations, while the text classification labels them non-admitted. Agree the unknown category before publishing complementary measures. Check lookup uniqueness before joining. |
| Over and under 12 hours | `obt_encounter_uec.duration`, department type, attendance category, discharge status and departure date/time | `duration` is submitted departure time since arrival in minutes. The old filter's `OR` admits every non-null department type, contradicting its exclusion list. Hana must confirm the intended department population and the 720-minute boundary. |
| Over 72 hours | `obt_encounter_uec.duration` | The old numeric measure uses `> 4320` minutes and applies no other exclusions. Confirm whether its intended population matches the 12-hour measures. |
| Assessed within/not within 15 minutes | `obt_encounter_uec.initial_assessment_time_since_arrival` | Old rules use `<= 15` and `> 15` minutes. Missing assessment time is in neither measure. |
| Time in department, admitted/non-admitted | `obt_encounter_uec.duration` and discharge destination | Carry the submitted minutes only for the agreed destination category. Preserve null when time or category is unknown. |
| Four-hour breach | `obt_encounter_uec.duration` | The old label uses `>= 240` minutes, unlike the 12-hour numeric measure's strict `> 720`. Confirm whether this is a local reporting measure or a national performance measure before naming it. |

## Production aggregate baselines

These checks returned only broad counts from the production dbt models. They do not establish that each Totally record has a duplicate emergency-department attendance, and they do not compare with the legacy view.

| Check | Attendance count |
| --- | ---: |
| Current ECDS attendance rows | 18,101,786 |
| Rows matching Hana's Totally adjustment | 286,680 |
| Under-18 attendance rows | 3,598,335 |
| Under-18 CYP flag using only the primary diagnosis, plus chief complaint and injury intent | 35,419 |
| Under-18 CYP flag using any diagnosis, plus chief complaint and injury intent | 35,824 |
| Attendances with at least one psychiatric referred-to service | 166,259 |
| Over 12 hours under the old non-null-department `OR` condition | 676,624 |
| Over 12 hours if the apparent exclusion is implemented with `AND` | 641,386 |

The revised any-diagnosis CYP rule adds 405 under-18 attendances to the primary-only rule. Changing the 12-hour department condition removes 35,238 attendances from the over-12-hour count. These are material definition changes, not SQL tidy-ups.

## Decisions and next checks

1. Hana confirmed that any psychiatric referral qualifies and that the time measures should be calculated for all department types, leaving department selection to consumers.
2. Hana confirmed that the submitted practice should be combined with its latest PCN and neighbourhood mapping rather than historical membership.
3. Reconcile unknown discharge destinations and the measure boundaries against the analyst extract; the old view's text and numeric classifications are not fully consistent.
4. Compare broad monthly and department-type totals with Hana's QuickReport extract when received, and explain material differences.
5. Keep the reporting measure, code list, descriptions and grain test together. Do not add these local reporting rules to the shared `obt_encounter_uec` attendance model.
