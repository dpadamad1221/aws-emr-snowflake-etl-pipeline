from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


AWS_REGION = "ap-south-1"
S3_BUCKET = "dijo-emr-project"

JOB_FLOW_OVERRIDES = {
    "Name": "airflow-emr-snowflake-etl-cluster",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2KeyName": "emr-key",
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "VisibleToAllUsers": True,
}

SPARK_STEPS = [
    {
        "Name": "Step 1 - API to S3 File1",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                f"s3://{S3_BUCKET}/scripts/01_api_to_s3_file1.py",
            ],
        },
    },
    {
        "Name": "Step 2 - Generate File2 to Snowflake",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                f"s3://{S3_BUCKET}/scripts/02_generate_file2_to_snowflake.py",
            ],
        },
    },
    {
        "Name": "Step 3 - Snowflake to S3 File2",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                f"s3://{S3_BUCKET}/scripts/03_snowflake_to_s3_file2.py",
            ],
        },
    },
    {
        "Name": "Step 4 - Final Join File1 and File2",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                f"s3://{S3_BUCKET}/scripts/04_join_file1_file2_final.py",
            ],
        },
    },
]

default_args = {
    "owner": "dijo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aws_emr_snowflake_etl_pipeline",
    default_args=default_args,
    description="Create EMR cluster, run PySpark ETL steps, and terminate EMR cluster",
    start_date=datetime(2026, 5, 15),
    schedule=None,
    catchup=False,
    tags=["aws", "emr", "pyspark", "snowflake", "s3"],
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
    )

    add_spark_steps = EmrAddStepsOperator(
        task_id="add_spark_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=SPARK_STEPS,
        aws_conn_id="aws_default",
    )

    watch_step_1 = EmrStepSensor(
        task_id="watch_step_1_api_to_s3",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[0] }}",
        aws_conn_id="aws_default",
    )

    watch_step_2 = EmrStepSensor(
        task_id="watch_step_2_file2_to_snowflake",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[1] }}",
        aws_conn_id="aws_default",
    )

    watch_step_3 = EmrStepSensor(
        task_id="watch_step_3_snowflake_to_s3",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[2] }}",
        aws_conn_id="aws_default",
    )

    watch_step_4 = EmrStepSensor(
        task_id="watch_step_4_final_join",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_steps')[3] }}",
        aws_conn_id="aws_default",
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",
    )

    (
        create_emr_cluster
        >> add_spark_steps
        >> watch_step_1
        >> watch_step_2
        >> watch_step_3
        >> watch_step_4
        >> terminate_emr_cluster
    )
