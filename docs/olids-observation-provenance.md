# Expanded OLIDS observations

The proposed `DATA_LAKE.OLIDS.OBSERVATION` includes native observations, allergy records and
referral requests. Each row represents one source entity record, not a unique
clinical occurrence. Test requests, procedure requests and medication records
are outside this expansion.

The dbt-olids conformed and stable models own the population and ID derivation.
This project reads that shared output once. Staging continues to exclude records
marked deleted and records without a person identifier.

## Release blocker: clinical interpretation

Preserving native columns and IDs does not establish that downstream measures
remain correct. Existing consumers often interpret a matching code as a confirmed
condition or completed care, without checking the source entity. The expanded
population makes that assumption unsafe until the relevant clinical definitions
establish which entities each consumer may use. Keep both changes in draft.

Read-only checks on 9 September 2026 found these added records matching current
combined reference code sets after the staging deletion and person filters:

| Added entity | Reference code set | Matching records | Existing consumer interpretation |
| --- | --- | ---: | --- |
| Referral request | PCD `FOOTEXAM_COD` | 10,466 | Left and right foot checked flags in `int_foot_examination_all` |
| Referral request | UKHSA `ASTADM_COD` | 7,639 | Asthma admission evidence, sufficient alone for the COVID asthma criterion within its date window |
| Allergy | UKHSA `DXT_CHEMO_COD` | 1,764 | Recent chemotherapy/radiotherapy evidence within the campaign date window |
| Allergy | PCD `AST_COD` | 5,983 | Asthma diagnosis evidence |
| Allergy | UKHSA `DIAB_COD` | 1,328 | Diabetes evidence for vaccine eligibility |
| Allergy | ECL `ALCOHOL_MISUSE_DISORDERS` | 104 | Historical alcohol-misuse disorder when the record is not an active problem |

These counts are eligible code matches, not additional patients or measured
changes. Campaign models use versioned code lists and date and population rules;
the current-list counts do not establish campaign impact. Code sets overlap, so
their counts must not be added.

The matched referral concepts have the public SNOMED descriptions
"Refer to diabetic foot screener (procedure)" and
"Emergency hospital admission for asthma (procedure)" respectively. The first
explicitly describes a referral, yet the foot-examination consumer derives
checked flags from it. This exposes a pre-existing consumer interpretation defect:
that rule applies regardless of source entity. It does not make the referral
record invalid or establish that it should be removed from the combined feed.
The second describes an admission but occurs in a referral
request entity; that source context needs resolution. The matched allergy asthma, diabetes
and chemotherapy code groups are disorders, not medication products or substances.
This does not resolve why those concepts occur in an allergy entity or establish
that a referral proves completed care. No new clinical inclusion rules have been
implemented. Clinical owners need to agree entity eligibility for the affected
consumers before release. Retaining native observations for confirmed-condition
or completed-care measures is the proposed interim rule where that authority
remains unresolved; it is not yet an approved change.

The numerical-result check found no added `ALC_COD` matches and no matches in
numeric-result value sets among the 90 value sets used by
`get_ltc_lcs_observations_latest`, on either its mapped-code or source-code path.
There is therefore no observed null-result displacement from this expansion in
those checks. The macro still selects the latest matching record before a caller
can require a numerical result, so this is evidence about the current added
population, not a permanent guarantee. Some allergy matches are directly relevant
to existing adverse-reaction and contraindication consumers. Blanket exclusion
would also need a definition decision.

The Valproate duplicate-reading bug below is a separate, demonstrated defect that
this companion fixes. Its successful reconciliation does not clear these clinical
interpretation blockers.

## Identifiers and clinical context

`id` remains the identifier for joining the expanded observation feed. Native
observation IDs are unchanged. Added entities receive deterministic UUIDs that
include their source entity, so equal original IDs can remain separate records.
Do not deduplicate across entities by person, date, concept or source ID.

`source_entity` identifies `observation`, `allergy_intolerance` or
`referral_request`. `source_record_id` is the original UUID within that entity.
It is distinct from `lds_source_record_id`, which the feed already supplied.
Use the entity and original ID together when tracing a source record.

`allergy_medication_name` retains the medication name on an allergy record. It
does not represent a prescription or medication order. Added entities do not
acquire observation result values, units or problem flags. Null flags mean that
the source has no applicable value; they do not establish a negative result.

Raw and staging expose all three new fields. `get_observations`,
`get_ltc_lcs_observations` and `get_ltc_lcs_observations_latest` carry them into
their results. Existing code-set matching and date rules remain in place.
Concept-specific models that project only their existing fields can still join
their observation ID back to staging for provenance.

The C-LTCS observation feed includes the new fields and retains its existing
one-year window and limit of 100 recent records per patient. That limit applies
across all three entities. A newer allergy or referral can therefore displace an
older observation. A referral request is not evidence that its requested care
was delivered.

## Valproate referral events

`int_valproate_araf_referral_events` previously combined observations and
referral requests itself. It now reads only the expanded observation feed,
avoiding a second copy of every qualifying referral request.

`araf_referral_id` preserves existing event IDs: original referral-request IDs
for referral requests and unchanged observation IDs for native observations.
New allergy rows use their conformed observation ID. The ID alone need not
distinguish original entities. The model's grain uses person and
`araf_referral_observation_id`, which joins directly to staging. Source entity
and original ID remain available. Existing person-level arrays of legacy event
IDs keep their meaning and are not an event count.

## Deployment and historical rebuilds

Use a coordinated release window. Scheduled analytics must not run against the
expanded upstream feed before this companion is deployed: the old valproate
model would read each referral twice. Publish the upstream conformed and stable
changes, including all three new columns, then complete DEV validation and
deploy the companion before the next analytics run. If that sequence cannot
fit between scheduled runs, use the existing job scheduling controls to hold
analytics runs until both changes are ready. This change does not alter those
controls or publish upstream data.

There is no runtime fallback to the old schema. Source YAML describes the
agreed upstream columns so the companion can compile before publication.

After publication, use the existing `dev` target and shared `DEV__` layers:

1. Build the changed observation raw and staging models, valproate referral
   events, their person-level aggregate and C-LTCS observations. Run their tests.
2. Compile the full project and inspect the downstream selection with
   `dbt ls --select stg_olids_observation+`. Macros affect more models than their
   direct callers suggest. Profile changes by source entity and code-set group,
   including register membership, latest-event dates, the C-LTCS cap and the
   global data-refresh date.
3. Rebuild affected table consumers. COVID and flu incremental models retain
   closed campaigns during routine runs. Use the existing
   `covid_flu_rebuild_closed: true` variable or a targeted full refresh when
   restating those campaigns against the expanded feed.
4. `person_month_analysis_base` normally replaces only the latest completed
   month. Its established full-refresh process is needed to restate affected
   older person-months.

Do not apply a blanket downstream full refresh. In particular,
`cltcs_population_monthly_capture` is an append-only record of the population
captured each month. Recreating it would erase historical captures rather than
recompute historical membership.

The scoped raw-generation run uses the repository's
`scripts/sources/3_generate_raw_models.py` with only the OBSERVATION source in a
scratch project. It regenerates the existing raw model without changing other
raw models. Warehouse validation remains dependent on upstream publication;
this companion does not create alternate databases, schemas or source views.

## Companion validation

The full project compiled on 9 September 2026 using the standard `dev` target:
2,100 models, 4,812 tests, 33 snapshots, 39 seeds and 47 analyses, plus hooks,
after merging the latest main branch.
The existing unused-configuration warning was the only warning.

Local synthetic validation executed the rendered valproate model with native,
allergy and referral records sharing an original UUID. It retained three
conformed events, preserved the old native and referral IDs and excluded a
non-referral code. The new source-identity regression passed and detected a
deliberately changed referral ID. This checks the transformation without
patient data; it does not replace the warehouse build after upstream publication.


The [pre-publication impact queries](../scripts/snowflake/profile_olids_observation_companion.sql)
also ran against existing production outputs and conformed source entities.
The 519 existing Valproate events for 284 people all match the 519 qualifying
referral requests by person and original event ID. Dates, concept codes and
labels have zero differences; the retained legacy ID sets are unchanged.
No allergy records match the programme's REFERRAL code category. This comparison
uses the existing materialised Valproate output and the added entities, without
rescanning native observation history.

The global refresh date remains 7 September 2026. A bounded check of native,
allergy and referral dates after that date found no later date supported by at
least 150 practices. These aggregate checks ran before upstream publication;
a warehouse build and its tests remain outstanding.
