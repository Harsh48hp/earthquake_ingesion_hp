from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG


default_args = {
    'owner': 'harshal',
    'start_date': datetime(2024, 11, 6),
    'retries': 1
}

dag = DAG(
    dag_id='job_submit_loading_historical_data_earthquake_analysis_by',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    description='Load earthquake historical data, transform, and store in BigQuery through GCS'
)

start = EmptyOperator(task_id='start', dag=dag)

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id='earthquake-analysis-440806',
    cluster_name='harshal-pyspark-job-cluster',
    gcp_conn_id='airflow_gcs_admin_all_permission',
    region='us-central1',
    cluster_config={
        'master_config': {
            'num_instances': 1,
            'machine_type_uri': 'n2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 40},
        },
        'worker_config': {
            'num_instances': 2,
            'machine_type_uri': 'n2-standard-2',
            'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 40},
        }
    },
    dag=dag
)


# Submit PySpark job
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='run-dataproc-pyspark',
    job= {
        'reference': {'project_id': 'earthquake-analysis-440806'},
        'placement':{'cluster_name': 'harshal-pyspark-job-cluster'},
        'pyspark_job':{
                    "main_python_file_uri": 'gs://earthquake_analysis_by_hp_24/composer/historical_data/historical_data_dataproc_pyspark_composer.py',
                    'python_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/util.py'],
                    'jar_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/spark-3.2-bigquery-0.41.0.jar'],
                }
    },
    region='us-central1',
    project_id='earthquake-analysis-440806',
    dag=dag
)


# Delete the Dataproc cluster
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete-dataproc-cluster',
    project_id='earthquake-analysis-440806',
    gcp_conn_id='airflow_gcs_admin_all_permission',
    cluster_name='harshal-pyspark-job-cluster',
    region='us-central1',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED, dag=dag)

# Define task dependencies
start >> create_cluster >> submit_pyspark_job >> delete_cluster >> end
  























































# import os
# import datetime
# from airflow import models
# from airflow.providers.google.cloud.operators.dataproc import (
#    DataprocCreateClusterOperator,
#    DataprocSubmitJobOperator
# )
# from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
# from airflow.utils.dates import days_ago

# PROJECT_ID = "give your project id"
# CLUSTER_NAME =  "your dataproc cluster name that you want to create"
# REGION = "us-central1"
# ZONE = "us-central1-a"
# PYSPARK_URI = "GCS location of your PySpark Code i.e gs://[input file]"

# YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

# default_dag_args = {
#     'start_date': YESTERDAY,
# }

# # Cluster definition
# # [START how_to_cloud_dataproc_create_cluster]

# CLUSTER_CONFIG = {
#    "master_config": {
#        "num_instances": 1,
#        "machine_type_uri": "n1-standard-4",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
#    },
#    "worker_config": {
#        "num_instances": 2,
#        "machine_type_uri": "n1-standard-4",
#        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
#    },
# }

# with models.DAG(
#    "dataproc",

#    schedule_interval=datetime.timedelta(days=1),
#    default_args=default_dag_args) as dag:

#    # [START how_to_cloud_dataproc_create_cluster_operator]
#    create_cluster = DataprocCreateClusterOperator(
#        task_id="create_cluster",
#        project_id=PROJECT_ID,
#        cluster_config=CLUSTER_CONFIG,
#        region=REGION,
#        cluster_name=CLUSTER_NAME,
#    )

#    PYSPARK_JOB = {
#    "reference": {"project_id": PROJECT_ID},
#    "placement": {"cluster_name": CLUSTER_NAME},
#    "pyspark_job": {"main_python_file_uri": 'gs://earthquake_analysis_by_hp_24/composer/historical_data/historical_data_dataproc_pyspark_composer.py',
#                    'python_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/util.py'],
#                     'jar_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/spark-3.2-bigquery-0.41.0.jar']},
#    }

#    pyspark_task = DataprocSubmitJobOperator(
#        task_id="pyspark_task", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID
#    )

#    create_cluster >>  pyspark_task          


# from datetime import datetime, timedelta
# from airflow.operators.empty import EmptyOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocCreateBatchOperator, DataprocDeleteClusterOperator
# from airflow.utils.dates import days_ago
# from airflow.utils.trigger_rule import TriggerRule
# from airflow import DAG
# import logging, string, random

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# def generate_batch_id():
#     return ''.join(random.choices(string.ascii_lowercase + string.digits + '-', k=10))
 
# default_args = {
#     'owner': 'harshal',
#     'start_date': datetime(2024, 11, 6),
#     'retries': 1
# }

# dag = DAG(
#     dag_id='job_submit_loading_historical_data_earthquake_analysis_by',
#     default_args=default_args,
#     schedule_interval='@once',
#     catchup=False,
#     description='Load earthquake historical data, transform, and store in BigQuery through GCS'
# )

# start = EmptyOperator(task_id='start', dag=dag)

# # # Create Dataproc cluster
# # create_cluster = DataprocCreateClusterOperator(
# #     task_id='create_dataproc_cluster',
# #     project_id='earthquake-analysis-440806',
# #     cluster_name='harshal-pyspark-job-cluster',
# #     gcp_conn_id='airflow_gcs_admin_all_permission',
# #     region='us-central1',
# #     cluster_config={
# #         'master_config': {
# #             'num_instances': 1,
# #             'machine_type_uri': 'n2-standard-2',
# #             'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
# #         },
# #         'worker_config': {
# #             'num_instances': 2,
# #             'machine_type_uri': 'n2-standard-2',
# #             'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
# #         }
# #     },
# #     dag=dag
# # )

# # Submit PySpark job using DataprocCreateBatchOperator with timeout and retry settings
# submit_pyspark_batch_job = DataprocCreateBatchOperator(
#     task_id='run-dataproc-pyspark-batch',
#     project_id='earthquake-analysis-440806',
#     region='us-central1',
#     batch={
#         'runtime_config': {
#             'version': '2.2',  # Specify the version of Dataproc to use for this batch
#         },
#         'pyspark_batch': {
#             'main_python_file_uri': 'gs://earthquake_analysis_by_hp_24/composer/historical_data/historical_data_dataproc_pyspark_composer.py',
#             'python_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/util.py'],
#             'jar_file_uris': ['gs://earthquake_analysis_by_hp_24/composer/historical_data/spark-3.2-bigquery-0.41.0.jar'],
#         },
#         'environment_config': {
#             'peripherals_config': {
#                 'spark_resource_limits': {
#                     'cores_per_task': 1,  # Adjust CPU allocation per task
#                     'tasks_per_core': 2,  # Specify tasks per core
#                 }
#             }
#         }
#     },
#     gcp_conn_id='airflow_gcs_admin_all_permission',
#     batch_id=generate_batch_id(),
#     retries=3,  # Increased retries for better fault tolerance
#     retry_delay=timedelta(minutes=10),  # Increased retry delay for stability
#     max_retry_delay=timedelta(minutes=30),  # Maximum retry delay
#     dag=dag
# )

# # # Delete the Dataproc cluster
# # delete_cluster = DataprocDeleteClusterOperator(
# #     task_id='delete-dataproc-cluster',
# #     project_id='earthquake-analysis-440806',
# #     gcp_conn_id='airflow_gcs_admin_all_permission',
# #     cluster_name='harshal-pyspark-job-cluster',
# #     region='us-central1',
# #     trigger_rule=TriggerRule.ALL_DONE,
# #     dag=dag
# # )

# end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED, dag=dag)

# # Define task dependencies
# # start >> create_cluster >> submit_pyspark_batch_job >> delete_cluster >> end
# start >> submit_pyspark_batch_job >> end
