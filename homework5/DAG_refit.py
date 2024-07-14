import uuid
from datetime import datetime, timedelta
from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    InitializationAction,
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,

)

import configs

# Создание подключения для Object Storage
session = settings.Session()
ycS3_connection = Connection(
    conn_id='yc-s3',
    conn_type='s3',
    host='https://storage.yandexcloud.net/',
    extra={
        "aws_access_key_id": Variable.get("S3_KEY_ID"),
        "aws_secret_access_key": Variable.get("S3_SECRET_KEY"),
        "host": "https://storage.yandexcloud.net/"
    }
)


if not session.query(Connection).filter(Connection.conn_id == ycS3_connection.conn_id).first():
    session.add(ycS3_connection)
    session.commit()

ycSA_connection = Connection(
    conn_id='yc-SA',
    conn_type='yandexcloud',
    extra={
        "extra__yandexcloud__public_ssh_key": Variable.get("DP_PUBLIC_SSH_KEY"),
        "extra__yandexcloud__service_account_json_path": Variable.get("DP_SA_PATH")
    }
)

if not session.query(Connection).filter(Connection.conn_id == ycSA_connection.conn_id).first():
    session.add(ycSA_connection)
    session.commit()


with DAG(
        dag_id = 'ModelTraining',
        schedule_interval='@daily',
        start_date=datetime(year = 2024,month = 6,day = 20),
        max_active_runs=1,
        catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        folder_id=configs.YC_DP_FOLDER_ID,
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        cluster_description='Temporary cluster for Spark processing under Airflow orchestration',
        subnet_id=configs.YC_DP_SUBNET_ID,
        s3_bucket=configs.YC_DP_LOGS_BUCKET,
        service_account_id=configs.YC_DP_SA_ID,
        ssh_public_keys=Variable.get("SSH_PUBLIC"),
        zone=configs.YC_DP_AZ,
        cluster_image_version='2.0.43',
        masternode_resource_preset='s3-c2-m8',
        masternode_disk_type='network-ssd',
        masternode_disk_size=128,
        computenode_resource_preset='s3-c4-m16',
        computenode_disk_type='network-ssd',
        computenode_disk_size=64,
        computenode_count=4,
        datanode_resource_preset='s3-c4-m16',
        datanode_disk_type='network-ssd',
        datanode_disk_size=64,
        datanode_count=0,
        services=['YARN', 'SPARK', 'TEZ'],          
        connection_id=ycSA_connection.conn_id,
        properties={
            "pip:mlflow": "2.14.1",
            "pip:urllib3": "1.26.0"},
        initialization_actions=[InitializationAction(uri=f's3a://{configs.YC_SOURCE_BUCKET}/init.sh', args=[], timeout=300)],
        dag=ingest_dag
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{configs.YC_SOURCE_BUCKET}/train.py',
        python_file_uris=[f's3a://{configs.YC_SOURCE_BUCKET}/create_features.py'],
        connection_id = ycSA_connection.conn_id,
        properties={
                    "spark.submit.deployMode": "cluster",
                    "spark.yarn.appMasterEnv.MLFLOW_S3_ENDPOINT_URL": Variable.get("MLFLOW_S3_ENDPOINT_URL"),
                    "spark.yarn.appMasterEnv.MLFLOW_TRACKING_URI": Variable.get("MLFLOW_TRACKING_URI"),
                    "spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
                    "spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
                    "spark.yarn.appMasterEnv.SAVE_MODEL_BUCKET": configs.SAVE_MODEL_BUCKET},
        dag=ingest_dag
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=ingest_dag
    )
    # Формирование DAG из указанных выше этапов
    create_spark_cluster >>  poke_spark_processing >> delete_spark_cluster