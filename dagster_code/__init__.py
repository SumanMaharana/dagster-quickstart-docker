"""
Dagster Code Location

Defines the Dagster repository with assets, jobs, schedules, and resources.
"""

from dagster import Definitions, load_assets_from_modules, FilesystemIOManager
from dagster_code import assets
from dagster_code.resources import get_source_db_resource, get_warehouse_db_resource
from dagster_code.jobs import all_jobs, all_schedules


# Load all assets
all_assets = load_assets_from_modules([assets])

# Define Dagster definitions
defs = Definitions(
    assets=all_assets,
    jobs=all_jobs,
    schedules=all_schedules,
    resources={
        "source_db": get_source_db_resource(),
        "warehouse_db": get_warehouse_db_resource(),
        "io_manager": FilesystemIOManager(base_dir="/opt/dagster/dagster_home/storage"),
    },
)
