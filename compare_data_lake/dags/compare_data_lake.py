from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from gd_emr_operator import (CreateEMRClusterOperator, RunEMRStepOperator,
                             TerminateEMRClusterOperator)

default_args = {
        'owner': 'CKP Martech',
        'depends_on_past': False,
        'start_date': datetime(2019, 6, 17),
        'email': ['CKP-Marketing-Data@godaddy.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }


DAG_ID = 'DagCompareDataLake'
ENV = '{{ var.value.mwaa_env }}'
AWS_CONN_ID = f'mktgdata-{ENV}'

dag_configs = dict(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
)


def get_spark_output(**kwargs):
    aws_env_label = Variable.get('mwaa_env')
    spark_run_id = kwargs['ts_nodash']
    s3_bucket = f'gd-mktgdata-{aws_env_label}-hadoop-migrated'
    s3_prefix = f'comparison/{spark_run_id}'
    print(f'Trying to get s3://{s3_bucket}/{s3_prefix}/spark_output')
    s3_client = boto3.client('s3')
    response = s3_client.get_object(
        Bucket=s3_bucket,
        Key=f'{s3_prefix}/spark_output',
    )
    print(f'Successfully loaded s3://{s3_bucket}/{s3_prefix}/spark_output')

    spark_job_info = '------------- Spark Output -------------\n'
    spark_output = response['Body'].read().decode()
    spark_job_info += spark_output
    spark_job_info += '\n----------------------------------------'
    print(spark_job_info)


with DAG(**dag_configs) as dag:
    create_emr_cluster = CreateEMRClusterOperator(
        task_id='create_emr',
        aws_conn_id=AWS_CONN_ID,
        cluster_name='DagCompareMDM-{{ ts_nodash }}-{{ ti.prev_attempted_tries }}',
        release_label='emr-6.6.0',
        master_instance_type='m5.4xlarge',
        core_instance_type='r5.4xlarge',
        number_of_core_instances=2,
        bootstrap_action_file_path=f's3://gd-mktgdata-{ENV}-code/gdcorp-dna/de-marketing-mdm/utils/bootstrap.sh',
        tags_dict={
            'dataDomain': 'marketing',
            'dataSubDomain': 'product',
            'teamName': 'MDPE-MarketingOps',
            'onCallGroup': 'Marketing-Franchise-Oncall',
            'teamSlackChannel': 'marketing-data-products-help',
            'managedByMWAA': 'true',
            'doNotShutDown': 'true',
            'dataPipeline': 'DagCompareMDM'
        },
        dag=dag
    )

    submit_emr_step = RunEMRStepOperator(
        task_id='EMRCompareStep',
        aws_conn_id=AWS_CONN_ID,
        create_emr_task_id='create_emr',
        step_cmd='spark-submit --master yarn --deploy-mode client --conf spark.driver.memoryOverhead=16G ' +
                 f's3://gd-mktgdata-{ENV}-code/gdcorp-dna/de-marketing-mdm/' +
                 'compare_data_lake/spark/compare_data_lake.py ' +
                 '--run-id {{ ts_nodash }} ' +
                 f'-e {ENV} ' +
                 "--table1-name {{ dag_run.conf['table1_name'] }} " +
                 "--table1-columns {{ dag_run.conf['table1_columns'] }} " +
                 "--table1-primary-keys {{ dag_run.conf['table1_primary_keys'] }} " +
                 "--table2-name {{ dag_run.conf['table2_name'] }} " +
                 "--table2-columns {{ dag_run.conf['table2_columns'] }} " +
                 "--table2-primary-keys {{ dag_run.conf['table2_primary_keys'] }}"
    )

    terminate_emr_step = TerminateEMRClusterOperator(
        task_id='terminate_emr',
        aws_conn_id=AWS_CONN_ID,
        create_emr_task_id='create_emr',
        dag=dag
    )

    get_spark_output_task = PythonOperator(
        task_id='get_spark_output_task',
        python_callable=get_spark_output,
        provide_context=True,
        trigger_rule="one_success",
        dag=dag,
    )

    create_emr_cluster >> submit_emr_step >> terminate_emr_step >> get_spark_output_task
