import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from check_dependencies import DependencyCheckSensor
from data_lake_airflow_plugins.lake_api.success_notification_operator_v2 import \
    SnapshotSuccessNotificationOperator
from gd_emr_operator import (CreateEMRClusterOperator, RunEMRStepOperator,
                             TerminateEMRClusterOperator)

documentation_markdown = """
### General Information
- Dataset Name         : customer_product_count
- Purpose              : MDM Rearchitecture
- Confluence Link      :
- Detailed Runbook Link:
- Alerts Slack         :
- Dev Group Slack      :
- OnCall Group Slack   :
- OnCall Group Email   :
- SLA                  :
- Data Tier            :
- Stakeholders         :
- Dependency for       :

### OnCall Run book
- This pipeline creates a single EMR cluster and performs a stage data load, main data load and a data extract
- By default `step-extract-data` will extract delta from previous run, to extract full data snapshot run with config:
`{"load_type": "initial"}`
"""

DAG_ID = 'customer_product_count'
ENV = '{{ var.value.mwaa_env }}'
AWS_CONN_ID = f'mktgdata-{ENV}'

DAG_CONFIG = Variable.get(f'{DAG_ID}_config', default_var=json.dumps({'output_suffix': '_metl'}))
DEPENDENCIES = [
    'ckp_martech_airflow_local.mdm_cpc_siena_egress'
]

CODE_BUCKET = f's3://gd-mktgdata-{ENV}-code'
CODE_PATH = f'{CODE_BUCKET}/gdcorp-dna/de-marketing-mdm'
BASE_SPARK_COMMAND = (
                      'spark-submit '
                      '--deploy-mode client '
                      '--master yarn '
                      '--driver-memory 15g '
                      '--conf spark.driver.maxResultSize=0'
                     )
PY_FILES = f'--py-files {CODE_PATH}/customer_product_count/spark/sql/*,{CODE_PATH}/utils/*'


with DAG(
        dag_id=DAG_ID,
        max_active_runs=1,
        catchup=False,
        schedule_interval=None if Variable.get('mwaa_env') == 'dev-private' else '0 16 * * *',
        template_searchpath=[f'dags/{DAG_ID}/'],
        doc_md=documentation_markdown,
        tags=['marketing', 'mdm', 'marketing'],
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': datetime(2022, 5, 1),
            'retries': 0,
            'email_on_failure': True,
            'email_on_retry': False,
            'concurrency': 4,
            'max_active_runs': 1,
            'retry_delay': timedelta(minutes=2),
            'provide_context': True
        }
) as dag:
    start_dag = DummyOperator(task_id='start_dag')
    data_lake_dependency_check = []
    for dependency in DEPENDENCIES:
        data_lake_dependency_check.append(
            DependencyCheckSensor(
                dag=dag,
                task_id=f"dep_{dependency.split('.')[-1]}",
                s3_bucket=f'gd-mktgdata-{ENV}-success-files',
                table_name=dependency,
                mode='reschedule',
                poke_interval=timedelta(minutes=5).seconds,
                timeout=timedelta(hours=6).seconds))
    # EMR related variables
    emr_task_id = f'emr_{DAG_ID}'

    create_emr = CreateEMRClusterOperator(
        task_id=emr_task_id,
        cluster_name=f'{DAG_ID}-{{{{ ts_nodash }}}}-{{{{ ti.prev_attempted_tries }}}}',
        aws_conn_id=AWS_CONN_ID,
        release_label='emr-6.6.0',
        emr_sc_provisioning_artifact_name='1.16.0',
        ssm_param_ami_image_id='/GoldenAMI/gd-amzn2/latest',
        master_instance_type='m5.8xlarge',
        core_instance_type='m5.8xlarge',
        number_of_core_instances=2,
        bootstrap_action_file_path=f'{CODE_PATH}/utils/bootstrap.sh',
        mandatory_tag_keys=[
            'dataDomain',
            'dataPipeline',
            'doNotShutDown'
        ],
        tags_dict={
            'dataDomain': 'marketing',
            'dataSubDomain': 'other',
            'teamName': 'MDPE-MarketingOps',
            'onCallGroup': 'Marketing-Franchise-Oncall',
            'teamSlackChannel': 'marketing-data-products-help',
            'managedByMWAA': 'true',
            'doNotShutDown': 'true',
            'dataPipeline': DAG_ID
        }
    )

    ingest_data_stg = RunEMRStepOperator(
        task_id='emr_ingest_data_stg',
        create_emr_task_id=emr_task_id,
        aws_conn_id=AWS_CONN_ID,
        retries=1,
        retry_delay=timedelta(minutes=10),
        waiter_poke_interval=60,
        waiter_poke_max_attempts=120,
        step_name='step_ingest_data_stg',
        step_cmd=(
                  f'{BASE_SPARK_COMMAND} '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/{DAG_ID}/spark/{DAG_ID}.py '
                  '-d mdm_local '
                  '-t customer_product_count '
                  f'-e {ENV}'
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    # Send Success notification to GD DataLake and update data location.
    utcnow = datetime.utcnow()
    s3_datalake = f's3://gd-mktgdata-{ENV}-egress/mdm_local'
    location = f'{s3_datalake}/customer_product_count/'

    data_lake_success = SnapshotSuccessNotificationOperator(
        task_id='data_lake_success',
        db_name='marketing_egress_mart',
        table_name='customer_product_count',
        success_file_year=utcnow.strftime('%Y'),
        success_file_month=utcnow.strftime('%m'),
        success_file_day=utcnow.strftime('%d'),
        env='prod' if Variable.get('mwaa_env') == 'prod' else 'dev',
        location=location,
        trigger_rule='none_failed_min_one_success'
    )

    terminate_emr = TerminateEMRClusterOperator(
        task_id='emr_terminate',
        aws_conn_id=AWS_CONN_ID,
        create_emr_task_id=emr_task_id,
        trigger_rule='all_done'
    )

    end_dag = DummyOperator(
        task_id='end_dag',
        trigger_rule='none_skipped'
    )

# Pipeline Orchestration
chain(
    start_dag,
    data_lake_dependency_check,
    create_emr,
    ingest_data_stg,
    [data_lake_success, terminate_emr],
    end_dag
)
