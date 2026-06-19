import pendulum
from typing import Dict, List

from airflow.models import Variable
from airflow import DAG
from airflow.models import Variable
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreatePysparkJobOperator,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.yandex.hooks.yandexcloud_dataproc import DataprocHook


env_name = "stage"
CONFIG = {
    "service_account_id": "aje5d4bug7fni2ihh2n2",
    "zone": "ru-central1-a",
    "subnet_id": "e9bb0rps101ifv8o9bn2",
    "bucket": "dagsairflow",
    "folder_id": "b1g6g21tng8n1gbrpkcl",
    "public_ssh_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEKZrShx28CD+c1o2SDir2d4LQyhRRhol1vk37rXggpv vrmy87@vrmy87-osx2",
    "log_group_id": "enp7lbceuipq6h9tqtf5" if env_name == "prod" else "enp7lbceuipq6h9tqtf5",
    "datanode_count": 32 if env_name == "prod" else 6,
    "env_name": env_name,
}
default_args = {
    "owner": "DE",
    "start_date": pendulum.datetime(2024, 12, 31, tz="Europe/Moscow"),
    "retries": 5,
}

tags = ["lakehouse", "spark_pipeline", "dataproc"]

class PySparkJobConfig:
    def __init__(self):
        self.name: str = None
        self.main_python_file_uri: str = None
        self.python_file_uris: List[str] = []
        self.file_uris: List[str] = []
        self.properties: Dict[str, str] = {}
        self.args: List[str] = []

    def as_dict(self):
        return vars(self)


class PySparkConfigBuilder:
    def __init__(self):
        self._pyspark_job_conf = PySparkJobConfig()
        self.init_default()

    @property
    def pyspark_job_conf(self):
        return self._pyspark_job_conf

    def reset(self):
        self._pyspark_job_conf = PySparkJobConfig()
        return self

    def init_default(self):
        self._pyspark_job_conf.properties = {"spark.submit.deployMode": "cluster"}

    def set_properties(self, properties):
        self._pyspark_job_conf.properties = properties
        return self

    def set_args(self, **kwargs):
        self._pyspark_job_conf.args = [
            f"--{arg_name.replace('_', '-')}={arg_value}" for arg_name, arg_value in kwargs.items()
        ]
        return self

    def set_libraries(self, python_file_uris):
        if isinstance(python_file_uris, list):
            self._pyspark_job_conf.python_file_uris = python_file_uris
        else:
            self._pyspark_job_conf.python_file_uris = [python_file_uris]
        return self

    def set_runner(self, run_script):
        self._pyspark_job_conf.main_python_file_uri = run_script
        return self

    def set_job_name(self, name):
        self._pyspark_job_conf.name = name
        return self

    def set_extra_files(self, extra_files):
        self._pyspark_job_conf.file_uris = extra_files
        return self

    def build(self):
        return self.pyspark_job_conf


class DataProcClusterSensor(BaseSensorOperator):
    def __init__(
        self,
        cluster_name,
        folder_id,
        yandex_conn_id=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.yandex_conn_id = yandex_conn_id
        self.cluster_name = cluster_name
        self.folder_id = folder_id

    def poke(self, context):
        self.log.info(f"Poking for cluster : {self.cluster_name}")
        hook = DataprocHook(yandex_conn_id=self.yandex_conn_id)
        client = hook.client.sdk.client(ClusterServiceStub)
        request = ListClustersRequest(folder_id=self.folder_id)
        response = client.List(request)

        clusters = response.clusters
        for cluster in clusters:
            self.log.info(cluster)
            self.log.info(f"{cluster.name}, {cluster.status}")
            if cluster.name == self.cluster_name and cluster.status == 2:
                context["ti"].xcom_push(key="return_value", value=cluster.id)
                return True
            elif cluster.name.startswith("reserv_dataproc") and cluster.status == 2:
                context["ti"].xcom_push(key="return_value", value=cluster.id)
                return True
        return None



class CustomDataprocCreatePysparkJobOperator(DataprocCreatePysparkJobOperator):
    template_fields = DataprocCreatePysparkJobOperator.template_fields + ("args",)


def create_daily_ingest_dag(dag_id):
    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False,
        tags=tags,
        max_active_tasks=16,
        max_active_runs=1,
    )
    with dag:

        cluster_sensor = DataProcClusterSensor(
            task_id="waiting_cluster",
            folder_id=CONFIG["folder_id"],
            cluster_name="asdsads",
            mode="reschedule",
            #on_failure_callback=task_failure_callback,
        )

        pyspark_conf = (
            PySparkConfigBuilder()
            .set_runner(f"s3a://{CONFIG['bucket']}/spark_pipeline/raw_runners/run_load_main_sources.py")
            .set_job_name("ingest_target_table")
            .set_args(target_table='table1', source="1c")
            .build()
            .as_dict()
        )
        dataproc_task = CustomDataprocCreatePysparkJobOperator(
            task_id="run_spark_pipeline",
            cluster_id="{{ ti.xcom_pull(task_ids='waiting_cluster') }}",
            retries=3,
            #on_failure_callback=task_failure_callback,
            **pyspark_conf,
        )

        cluster_sensor >> dataproc_task
    return dag



dag_id = "daily_ingest_lh_raw"
globals()[dag_id] = create_daily_ingest_dag(dag_id)
