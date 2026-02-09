from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt_src"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def gold_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
