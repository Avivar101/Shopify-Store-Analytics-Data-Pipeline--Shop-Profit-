import dagster as dg
from .assets import shopify_ingestion, dbt_build,  dbt_tests

all_assets = [shopify_ingestion, dbt_build, dbt_tests]

sync_job = dg.define_asset_job(
    name="shopify_pipeline_job",
    selection=dg.AssetSelection.assets(
        shopify_ingestion, dbt_build, dbt_tests
    ),
)

daily_schedule = dg.ScheduleDefinition(
    job=sync_job,
    cron_schedule="0 * * * *", # hourly
)

defs = dg.Definitions(
    assets=all_assets,
    jobs=[sync_job],
    schedules=[daily_schedule],
)