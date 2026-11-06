import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

from datetime import datetime


def cecj_transform(tmp_dir_name):
    from cecj_data_transform import cecj_aggregateed_data

    cecj_aggregateed_data.run()


with DAG(
    "generate_infographics",
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:
    generate_cecj_aggregated_data = PythonOperator(
        task_id="generate_cecj_aggregated_data",
        python_callable=cecj_transform,
        email_on_failure=True,
        email=Variable.get("mail_zulip"),
    )
