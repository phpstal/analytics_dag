import logging
import pendulum
import sys
from airflow.models.dag import DAG
from airflow.decorators import task
from sql_queries_for_intermediate_aggregation import sql_queries_for_intermediate_aggregation
from sql_queries_for_metrics import sql_queries_for_metrics

@task
def test_import():
    result = sql_queries_for_intermediate_aggregation()
    logging.info(result)
    result = sql_queries_for_metrics()
    logging.info(result) #


with DAG(
    dag_id='daggit2',
    start_date=pendulum.datetime(2025, 1, 1),
    schedule=None,
):
    task_test = test_import()

    task_test
