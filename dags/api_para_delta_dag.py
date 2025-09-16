from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'afonso',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='api_para_delta_lake',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/30 * * * *',
    catchup=False,
    tags=['spark', 'delta', 'api'],
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='processar_api_e_salvar_no_minio',
        conn_id='spark_default',
        application='/opt/bitnami/spark/app/brewery_bronze.py',
        jars=_(
            "/opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar,"
            "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/bitnami/spark/jars/delta-storage-3.2.0.jar"
            ),
        conf={
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.hadoop.fs.s3a.access.key': '{{ var.value.MINIO_ROOT_USER }}',
            'spark.hadoop.fs.s3a.secret.key': '{{ var.value.MINIO_ROOT_PASSWORD }}',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.delta.logStore.class': 'org.apache.spark.sql.delta.storage.S3SingleDriverLogStore',
        },
        env_vars={
            'MINIO_ROOT_USER': '{{ var.value.MINIO_ROOT_USER }}',
            'MINIO_ROOT_PASSWORD': '{{ var.value.MINIO_ROOT_PASSWORD }}',
        },
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=False,
    )
