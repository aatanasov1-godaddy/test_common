import json
import shlex
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from aws_utils import skip_check
from check_dependencies import DependencyCheckSensor
from data_lake_airflow_plugins.lake_api.success_notification_operator_v2 import \
    SnapshotSuccessNotificationOperator
from gd_emr_operator import (CreateEMRClusterOperator, RunEMRStepOperator,
                             TerminateEMRClusterOperator)

documentation_markdown = """
### General Information
- Dataset Name         : net_promoter_score
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

DAG_ID = 'net_promoter_score'
ENV = '{{ var.value.mwaa_env }}'
AWS_CONN_ID = f'mktgdata-{ENV}'
SUCCESS_DATE = "{{ data_interval_end.in_timezone('America/Phoenix').format('YYYY/MM/DD') }}"
LOAD_TYPE = """{{ dag_run.conf['load_type'] if dag_run != None and dag_run.conf != None
                                            and 'load_type' in dag_run.conf else 'delta' }}"""
SKIP_STG = """{{ dag_run.conf['skip_stg'] if dag_run != None and dag_run.conf != None
                                            and 'skip_stg' in dag_run.conf else 'False' }}"""
SKIP_MAIN = """{{ dag_run.conf['skip_main'] if dag_run != None and dag_run.conf != None
                                            and 'skip_main' in dag_run.conf else 'False' }}"""
SKIP_EXTRACT = """{{ dag_run.conf['skip_extract'] if dag_run != None and dag_run.conf != None
                                            and 'skip_extract' in dag_run.conf else 'False' }}"""
DAG_CONFIG = Variable.get(f'{DAG_ID}_config', default_var=json.dumps({'output_suffix': '_metl'}))
DEPENDENCIES = [
    'analytic_feature.customer_type',
    'analytic_feature.shopper_churn',
    'analytic_feature.shopper_ltv',
    'analytic_feature.shopper_tenure',
    'dp_enterprise.uds_product_billing',
    'fortknox.fortknox_shopper_snap',
    'marketing_mart.customer_consent',
    'marketing_mart.marketable_customer',
    'venture_legacy.customer_value_history'
]

CODE_BUCKET = f's3://gd-mktgdata-{ENV}-code'
CODE_PATH = f'{CODE_BUCKET}/gdcorp-dna/de-marketing-mdm'
PY_FILES = f'--py-files {CODE_PATH}/net_promoter_score/spark/sql/*,{CODE_PATH}/utils/*'
CUSTOM_RENAME = (
                 '{"customer_id": '
                 '"customer_id", '
                 '"market_id": '
                 '"marketID", '
                 '"first_name": '
                 '"FirstName", '
                 '"last_name": '
                 '"LastName", '
                 '"email": '
                 '"Email", '
                 '"phone": '
                 '"Phone", '
                 '"churn_utc_date": '
                 '"churn_date", '
                 '"churn_category": '
                 '"churn", '
                 '"unsubscribed_flag": '
                 '"Unsubscribed", '
                 '"unsubscribed_utc_date": '
                 '"unsubscribed_date", '
                 '"customer_loc_state_code": '
                 '"state", '
                 '"tenure_months": '
                 '"tenure_mon", '
                 '"revenue_per_year_usd_category": '
                 '"rev_p_yr", '
                 '"total_gcr_usd_category": '
                 '"ltv", '
                 '"do_not_call_category": '
                 '"dnc", '
                 '"do_not_call_utc_date": '
                 '"dnc_date", '
                 '"company_name": '
                 '"company", '
                 '"sub_brand": '
                 '"sub-brand", '
                 '"country_name": '
                 '"country", '
                 '"country_long": '
                 '"country_long", '
                 '"region1": '
                 '"region1", '
                 '"region2": '
                 '"region2", '
                 '"region3": '
                 '"region3", '
                 '"hvc_segment": '
                 '"hvc_segment", '
                 '"hvc_segment_utc_date": '
                 '"hvc_segment_date", '
                 '"customer_type_name": '
                 '"customer_type_name", '
                 '"customer_type_utc_date": '
                 '"customer_type_name_date", '
                 '"customer_type_reason_desc": '
                 '"customer_type_reason_desc", '
                 '"customer_type_reason_utc_date": '
                 '"customer_type_reason_desc_date", '
                 '"own_api_reseller_category": '
                 '"own_API_Reseller", '
                 '"own_api_reseller_utc_date": '
                 '"own_API_Reseller_date", '
                 '"own_basic_reseller": '
                 '"own_Basic_Reseller", '
                 '"own_basic_reseller_utc_date": '
                 '"own_Basic_Reseller_date", '
                 '"own_pro_reseller": '
                 '"own_Pro_Reseller", '
                 '"own_pro_reseller_utc_date": '
                 '"own_Pro_Reseller_date", '
                 '"own_super_reseller": '
                 '"own_Super_Reseller", '
                 '"own_super_reseller_utc_date": '
                 '"own_Super_Reseller_date", '
                 '"language_name": '
                 '"languagename", '
                 '"market_id_language": '
                 '"Language", '
                 '"language_code": '
                 '"Q_Language", '
                 '"external_data_reference": '
                 '"ExternalDataReference", '
                 '"ltv_raw_usd_amt": '
                 '"ltv_raw", '
                 '"last_survey_utc_date": '
                 '"LastSurveyDate", '
                 '"power_seller_category": '
                 '"PowerSeller", '
                 '"power_seller_utc_date": '
                 '"PowerSeller_date", '
                 '"etl_delete_flag": '
                 '"EtlDeleteFlag", '
                 '"etl_first_load_utc_ts": '
                 '"EtlFirstLoadTS", '
                 '"etl_last_refresh_utc_ts": '
                 '"EtlLastRefreshTS"}'
                )


with DAG(
        dag_id=DAG_ID,
        max_active_runs=1,
        catchup=False,
        schedule_interval=None if Variable.get('mwaa_env') == 'dev-private' else '0 14 * * *',
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
        master_instance_type='m5.12xlarge',
        core_instance_type='m5.12xlarge',
        number_of_core_instances=20,
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

    stg_check = BranchPythonOperator(
        task_id='stg_check',
        python_callable=skip_check,
        op_args=[SKIP_STG, 'main_check', 'emr_ingest_data_stg'],
        provide_context=True
    )

    main_check = BranchPythonOperator(
        task_id='main_check',
        python_callable=skip_check,
        op_args=[SKIP_MAIN, 'extract_check', 'emr_ingest_data_main'],
        provide_context=True,
        trigger_rule='none_failed_min_one_success'
    )

    extract_check = BranchPythonOperator(
        task_id='extract_check',
        python_callable=skip_check,
        op_args=[SKIP_EXTRACT, 'emr_terminate', 'emr_extract_data'],
        provide_context=True,
        trigger_rule='none_failed_min_one_success'
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
                  'spark-submit '
                  '--deploy-mode client '
                  '--master yarn '
                  '--driver-memory 15g '
                  '--conf spark.driver.maxResultSize=0 '
                  '--conf spark.driver.memoryOverhead=16G '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/{DAG_ID}/spark/{DAG_ID}_history.py '
                  '-d mdm_local '
                  '-t net_promoter_score_history '
                  f'-e {ENV}'
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    ingest_data_main = RunEMRStepOperator(
        task_id='emr_ingest_data_main',
        create_emr_task_id=emr_task_id,
        aws_conn_id=AWS_CONN_ID,
        retries=1,
        retry_delay=timedelta(minutes=10),
        waiter_poke_interval=120,
        waiter_poke_max_attempts=120,
        step_name='step_ingest_data',
        step_cmd=(
                  'spark-submit '
                  '--deploy-mode client '
                  '--master yarn '
                  '--driver-memory 15g '
                  '--conf spark.driver.maxResultSize=0 '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/utils/spark/ingest_data_main.py '
                  '-d mdm_local '
                  '-t net_promoter_score '
                  f'-e {ENV}'
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    # Send Success notification to GD DataLake and update data location.
    utcnow = datetime.utcnow()
    s3_datalake = f's3://gd-mktgdata-{ENV}-egress/mdm_local'
    location = f'{s3_datalake}/net_promoter_score/partition_utc_date={utcnow.strftime("%Y-%m-%d")}/'

    data_lake_success = SnapshotSuccessNotificationOperator(
        task_id='data_lake_success',
        db_name='marketing_egress_mart',
        table_name='customer_net_promoter_score',
        success_file_year=utcnow.strftime('%Y'),
        success_file_month=utcnow.strftime('%m'),
        success_file_day=utcnow.strftime('%d'),
        env='prod' if Variable.get('mwaa_env') == 'prod' else 'dev',
        location=location,
        trigger_rule='none_failed_min_one_success'
    )

    extract_data = RunEMRStepOperator(
        task_id="emr_extract_data",
        create_emr_task_id=emr_task_id,
        aws_conn_id=AWS_CONN_ID,
        retries=1,
        retry_delay=timedelta(minutes=10),
        waiter_poke_interval=60,
        waiter_poke_max_attempts=120,
        step_name='step_extract_data',
        step_cmd=(
                  'spark-submit '
                  '--deploy-mode client '
                  '--master yarn '
                  '--driver-memory 15g '
                  '--conf spark.driver.maxResultSize=0 '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/utils/spark/extract_data.py '
                  '-d mdm_local '
                  '-t net_promoter_score '
                  f'-e {ENV} '
                  f'-l {LOAD_TYPE} '
                  f'-dc {shlex.quote(DAG_CONFIG)} '
                  f'-cr {shlex.quote(CUSTOM_RENAME)} '
                  '--final-rename'
                 ),
        trigger_rule='none_failed_min_one_success',
    )

    terminate_emr = TerminateEMRClusterOperator(
        task_id='emr_terminate',
        aws_conn_id=AWS_CONN_ID,
        create_emr_task_id=emr_task_id,
        trigger_rule='all_done'
    )

    end_dag = DummyOperator(
        task_id='end_dag'
    )

# Pipeline Orchestration
chain(
    start_dag,
    data_lake_dependency_check,
    create_emr,
    stg_check,
    [ingest_data_stg, main_check],
)
chain(
    ingest_data_stg,
    main_check
)
chain(
    main_check,
    [ingest_data_main, extract_check]
)
chain(
    ingest_data_main,
    [data_lake_success, extract_check]
)
chain(
    extract_check,
    [extract_data, terminate_emr]
)
chain(
    [extract_data, data_lake_success],
    terminate_emr,
    end_dag
)
