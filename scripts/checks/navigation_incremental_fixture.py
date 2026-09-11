# /// script
# dependencies = ["jinja2"]
# ///
"""Emit a synthetic Snowflake lifecycle check using the repository macros.

Run with uv run scripts/checks/navigation_incremental_fixture.py > logs/fixture.sql,
then execute that file in one Snowflake session. All tables are temporary in the
existing DEV schema. The final query returns aggregate checks, not clinical data.
"""
from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[2]
SOURCE = "DEV__MODELLING.ERS.NAVIGATION_FIXTURE_SOURCE"
TARGET = "DEV__MODELLING.ERS.NAVIGATION_FIXTURE_TARGET"
BATCH = "DEV__MODELLING.ERS.NAVIGATION_FIXTURE_BATCH"
CHECKS = "DEV__MODELLING.ERS.NAVIGATION_FIXTURE_CHECKS"
macros = "\n".join(
    (ROOT / "macros/transformations" / name).read_text(encoding="utf-8")
    for name in ("navigation_delivery_filter.sql", "navigation_remove_withdrawn_records.sql")
)


def render(expression, incremental=True):
    return Environment().from_string(macros + expression).render(
        is_incremental=lambda: incremental,
        this=TARGET,
        ref=lambda name: SOURCE,
    ).strip()


def selection(incremental=True):
    return "\nunion all\n".join(
        f"select * from {SOURCE} where source_record_type = '{kind}' "
        + render("{{ navigation_delivery_filter('source_received_at', '" + kind + "') }}", incremental)
        for kind in ("contact", "referral")
    )


def increment():
    print(f"create or replace temporary table {BATCH} as {selection()};")
    # dbt delete+insert replaces every milestone of each delivered source key.
    print(f"delete from {TARGET} using {BATCH} where "
          f"{TARGET}.source_record_type={BATCH}.source_record_type and "
          f"{TARGET}.source_record_id={BATCH}.source_record_id;")
    print(f"insert into {TARGET} select * from {BATCH};")
    print(render("{{ navigation_remove_withdrawn_records([('contact', 'fixture', 'source_record_id'), "
                 "('referral', 'fixture', 'source_record_id')]) }}") + ";")


print(f"create temporary table {SOURCE} as select column1::varchar as source_record_type, "
      "column2::varchar as source_record_id, column3::varchar as event_id, "
      "column4::timestamp_ntz as source_received_at, column5::date as event_date from values "
      "('contact','kept','kept','2026-01-01','2025-12-01'),"
      "('contact','changed','changed-start','2026-01-02','2025-12-02'),"
      "('contact','changed','changed-end','2026-01-02','2025-12-03'),"
      "('contact','boundary','boundary','2026-01-10','2025-12-04'),"
      "('contact','withdrawn','withdrawn','2026-01-03','2025-12-05'),"
      "('referral','slow','slow','2026-01-02','2025-12-06');")
print(f"create temporary table {TARGET} as {selection(False)};")
print(f"delete from {SOURCE} where event_id in ('withdrawn','changed-end');")
print(f"update {SOURCE} set source_received_at='2026-01-11', event_date='2025-12-09' "
      "where source_record_id='changed';")
print(f"insert into {SOURCE} values "
      "('contact','historical','historical','2026-01-11','1902-01-01'),"
      "('contact','equal-boundary','equal-boundary','2026-01-10','2025-12-07'),"
      "('referral','slow-new','slow-new','2026-01-03','2025-12-08'),"
      "('contact','null-receipt','null-receipt',null,'2025-12-09'),"
      "('contact','late-old','late-old','2026-01-05','2025-12-10');")
increment()
print(f"create temporary table {CHECKS} as select 'incremental_population' as check_name, "
      f"count(*)=8 and count(distinct event_id)=8 as passed from {TARGET} union all "
      f"select 'changed_milestone', count_if(event_id='changed-start' and event_date='2025-12-09')=1 "
      f"and count_if(event_id in ('changed-end','withdrawn','late-old'))=0 from {TARGET};")
print(f"insert into {CHECKS} select 'repeat_fingerprint', true;")
print(f"set before_fingerprint=(select hash_agg(*) from {TARGET});")
increment()
print(f"update {CHECKS} set passed=(select hash_agg(*)=$before_fingerprint from {TARGET}) "
      "where check_name='repeat_fingerprint';")
print(f"create or replace temporary table {TARGET} as {selection(False)};")
print(f"insert into {CHECKS} select 'monthly_reconciliation', count(*)=9 "
      f"and count_if(event_id='late-old')=1 from {TARGET};")
print(f"select check_name, passed from {CHECKS} order by check_name;")
