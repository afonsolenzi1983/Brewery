from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'afonso',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='brewery_landing_dag',
    default_args=default_args,
    start_date=datetime(2025, 9, 19),
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'ingestion', 'landing'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='fetch_api_and_load_to_minio',
        conn_id='spark_default',
        application='/opt/spark/work-dir/app/brewery_landing.py',
        jars=(
            "/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar"
        ),
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client',
            'spark.hadoop.fs.s3a.access.key': '{{ var.value.MINIO_ROOT_USER }}',
            'spark.hadoop.fs.s3a.secret.key': '{{ var.value.MINIO_ROOT_PASSWORD }}',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
        },
        env_vars={
            'MINIO_ROOT_USER': '{{ var.value.MINIO_ROOT_USER }}',
            'MINIO_ROOT_PASSWORD': '{{ var.value.MINIO_ROOT_PASSWORD }}',
        },
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=False,
    )
