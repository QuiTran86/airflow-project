from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.sql import Table, Metadata
from astro.files import File
from astro.constants import FileType

from include.dbt.comos_config import DBT_PROFILE_CONFIG, DBT_PROJECT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

@dag(
    start_date=datetime(year=2024, month=8, day=19),
    catchup=False, schedule=None, tags=['retail']
)
def retail():
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src="/usr/local/airflow/include/dataset/online_retail.csv",
        dst="raw/online_retail.csv",
        bucket="quitran-mario-bucket",
        gcp_conn_id='gcp', mime_type="text/csv"
    )
    create_empty_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail', gcp_conn_id='gcp'
    )
    gcs_to_bigquery_dataset = aql.load_file(
        task_id='push_data_from_gcs_to_bigquery_dataset',
        input_file=File('gs://quitran-mario-bucket/raw/online_retail.csv', conn_id='gcp', filetype=FileType.CSV),
        output_table=Table(name='raw_invoices', conn_id='gcp', metadata=Metadata(schema='retail')),
        use_native_support=False
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    check_load()

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    check_transform()

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)


retail()
