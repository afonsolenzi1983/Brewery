from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta
from datetime import datetime

with DAG(
    dag_id='api_para_delta_lake',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['spark', 'delta', 'api'],
) as dag:
    
    submit_spark_job = SparkSubmitOperator(
        task_id='processar_api_e_salvar_no_minio',
        # Conexão com o Spark Master dentro da rede Docker
        conn_id='spark_default',
        # Caminho para o script Python dentro do container do Spark
        application='/opt/bitnami/spark/app/brewery_bronze.py',
        # Pacotes necessários para o Spark se comunicar com o S3 (MinIO) e usar Delta Lake
        #packages='io.delta:delta-spark_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4',
        packages='io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4',
        # Configurações passadas para o ambiente do Spark (ex: credenciais do MinIO)
        env_vars={
            'MINIO_ROOT_USER': '{{ var.value.MINIO_ROOT_USER }}',
            'MINIO_ROOT_PASSWORD': '{{ var.value.MINIO_ROOT_PASSWORD }}'
        },
        # Aumenta o timeout para dar tempo ao job rodar
        execution_timeout=timedelta(minutes=10),
    )