from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from common.lib.ecs_utils import TTUECSOperator
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


DAG_ID = 'DagCompareMDM_test'
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
    response = s3_client.list_objects_v2(
        Bucket=f'gd-mktgdata-{aws_env_label}-hadoop-migrated',
        Prefix=f'comparison/marketing_mart_local/{spark_run_id}/',
        Delimiter='/'
    )
    table_prefixes = [elem['Prefix'].split('/')[-2] for elem in response['CommonPrefixes']]
    if spark_output != 'Both datasets matched!':
        if table_prefixes:
            spark_job_info += '\nAthena tables created:'
        for table in table_prefixes:
            spark_job_info += f'\n{table}'
    spark_job_info += '\n----------------------------------------'
    print(spark_job_info)


with DAG(**dag_configs) as dag:

    ttuEcsOperatorExport = TTUECSOperator(
         job_type='teradata_s3',
         task_id='export_teradata_s3',
         dag=dag,
         cluster='gd-marketing-ecs-ttu',
         service_name='ecsServiceDeploymentTTU',
         task_definition='ecsServiceDeploymentTTUTaskDefinition',
         teradata_user="""{{ dag_run.conf['teradata_user'] if dag_run != None and dag_run.conf != None
                                            and 'teradata_user' in dag_run.conf else 'P_BTCH_AWSIMPORT' }}""",
         region_name='us-west-2',
         database="{{ dag_run.conf['teradata_db'] }}",
         table="{{ dag_run.conf['teradata_table'] }}",
         bucket=f'gd-mktgdata-{ENV}-hadoop-migrated',
         prefix="comparison/{{ dag_run.conf['teradata_table'] }}/",
         s3_object='data',
         truncate_destination=True
    )

    iam_role = 'emr-ec2-default-role-lambda' if Variable.get('mwaa_env') == 'dev-private' else 'emr-ec2-default-role'

    create_emr_cluster = CreateEMRClusterOperator(
        task_id='create_emr',
        aws_conn_id=AWS_CONN_ID,
        cluster_name='DagCompareMDM-{{ ts_nodash }}-{{ ti.prev_attempted_tries }}',
        custom_job_flow_role_name_suffix=iam_role,
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
        step_cmd='spark-submit --master yarn --deploy-mode cluster --conf spark.driver.memoryOverhead=16G ' +
                 f's3://gd-mktgdata-{ENV}-code/gdcorp-dna/de-marketing-mdm/compare_mdm/spark/compare.py ' +
                 '--run-id {{ ts_nodash }} ' +
                 f'-e {ENV} ' +
                 "--aws-table {{ dag_run.conf['aws_table'] }} " +
                 "--aws-columns {{ dag_run.conf['aws_columns'] }} " +
                 '--teradata-table-location ' +
                 f's3://gd-mktgdata-{ENV}-hadoop-migrated/comparison/' + "{{ dag_run.conf['teradata_table'] }}/data/ " +
                 "--teradata-columns {{ dag_run.conf['teradata_columns'] }} " +
                 "--teradata-db {{ dag_run.conf['teradata_db'] }} " +
                 "--teradata-table {{ dag_run.conf['teradata_table'] }}"
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

    ttuEcsOperatorExport >> create_emr_cluster >> submit_emr_step >> terminate_emr_step >> get_spark_output_task
