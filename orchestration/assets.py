import subprocess
import sys
import dagster as dg

@dg.asset
def shopify_ingestion(context: dg.AssetExecutionContext):
    context.log.info("Starting Shopify Ingestion")
    result = subprocess.run(
        [sys.executable, "-u", "ingestion/run_shopify_ingestion.py"],
        capture_output=True,
        text=True,
    )

    if result.returncode !=0:
        context.log.error(result.stdout)
        context.log.error(result.stderr)
        raise Exception("Shopify ingestion failed")
    
    context.log.info(result.stdout)
    return "Ingestion complete"

@dg.asset(deps=[shopify_ingestion])
def dbt_build(context: dg.AssetExecutionContext):
    context.log.info("Starting dbt run")
    result = subprocess.run(
        ["uv", "run", "dbt", "run", "--project-dir", "."],
        cwd="dbt_transform",
        capture_output=True,
        text=True,
    )

    if result.returncode !=0:
        context.log.error(result.stdout)
        context.log.error(result.stderr)
        raise Exception("dbt run failed")
    
    context.log.info(result.stdout)
    return "dbt run complete"

@dg.asset(deps=[dbt_build])
def dbt_tests(context: dg.AssetExecutionContext):
    context.log.info("Starting dbt tests")
    result = subprocess.run(
        ["uv", "run", "dbt", "test", "--project-dir", "."],
        cwd="dbt_transform",
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.warning(result.stdout)
        context.log.warning(result.stderr)
        return "dbt tests failed, but continuing with pipeline"
    return "dbt tests complete"