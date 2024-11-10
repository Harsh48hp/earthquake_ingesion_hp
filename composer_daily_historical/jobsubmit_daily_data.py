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
    dag_id='job_submit_loading_daily_data_earthquake_analysis',
    default_args=default_args,
    schedule_interval = '0 8 * * *',
    catchup=False,
    description='Load earthquake daily data, transform, and apeend in historical data in BigQuery through GCS'
)

start = EmptyOperator(task_id='start', dag=dag)

# Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    project_id='earthquake-analysis-440806',
    cluster_name='harshal-pyspark-job-cluster-daily',
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
        'placement':{'cluster_name': 'harshal-pyspark-job-cluster-daily'},
        'pyspark_job':{
                    "main_python_file_uri": 'gs://earthquake_analysis_by_hp_24/composer/historical_data/daily_data_dataproc_pyspark_composer.py',
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
    cluster_name='harshal-pyspark-job-cluster-daily',
    region='us-central1',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

end = EmptyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED, dag=dag)

# Define task dependencies
start >> create_cluster >> submit_pyspark_job >> delete_cluster >> end
  





































