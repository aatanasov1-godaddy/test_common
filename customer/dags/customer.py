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
- Dataset Name         : customer
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

DAG_ID = 'customer'
ENV = '{{ var.value.mwaa_env }}'
AWS_CONN_ID = f'mktgdata-{ENV}'
SUCCESS_DATE = "{{ data_interval_end.in_timezone('America/Phoenix').format('YYYY/MM/DD') }}"
TEST_KEY = """{{ dag_run.conf['test_key'] if dag_run != None and dag_run.conf != None
                                            and 'test_key' in dag_run.conf else 'delta' }}"""
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
    'marketing_mart.marketable_customer',
    'enterprise.dim_subscription',
    'enterprise.dim_entitlement'
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
PY_FILES = f'--py-files {CODE_PATH}/customer/spark/sql/*,{CODE_PATH}/utils/*'
"""need to add email_marketing_optedin_flag to datalake ddl"""
CUSTOM_RENAME = (
                 '{"etl_first_load_utc_ts": '
                 '"etl_first_load_ts", '
                 '"etl_last_refresh_utc_ts": '
                 '"etl_last_refresh_ts", '
                 '"account_create_utc_ts": '
                 '"account_create_ts", '
                 '"attrite_utc_ts": '
                 '"attrite_ts", '
                 '"churn_utc_ts": '
                 '"churn_ts", '
                 '"first_purchase_utc_ts": '
                 '"first_purchase_ts", '
                 '"likely_godaddy_pro_utc_ts": '
                 '"likely_godaddy_pro_ts", '
                 '"abandoned_landing_page_utc_ts": '
                 '"abandoned_landing_page_ts", '
                 '"account_marked_fraud_flag": '
                 '"is_account_marked_as_fraud", '
                 '"active_product_owner_flag": '
                 '"is_active_product_owner", '
                 '"call_marketing_opted_in_flag": '
                 '"is_call_marketing_optedin", '
                 '"client_of_webpro_flag": '
                 '"is_client_of_webpro", '
                 '"do_not_call_flag": '
                 '"is_do_not_call", '
                 '"email_account_summary_opted_in_flag": '
                 '"is_email_account_summary_optedin", '
                 '"email_marketing_optedin_flag": '
                 '"is_email_marketing_optedin", '
                 '"email_notification_opted_in_flag": '
                 '"is_email_notification_optedin", '
                 '"global_holdout_flag": '
                 '"is_global_holdout", '
                 '"global_standard_suppression_flag": '
                 '"is_global_standard_suppression", '
                 '"high_value_customer_tier_flag": '
                 '"is_high_value_customer_tier", '
                 '"high_value_customer_tier_holdout_flag": '
                 '"is_high_value_customer_tier_holdout", '
                 '"internal_email_address_flag": '
                 '"is_internal_email_address", '
                 '"mail_marketing_opted_in_flag": '
                 '"is_mail_marketing_optedin", '
                 '"notification_suppression_flag": '
                 '"is_notification_suppression", '
                 '"owner_of_private_label_id_flag": '
                 '"is_owner_of_private_label_id", '
                 '"private_label_13_suppression_flag": '
                 '"is_private_label_13_suppression", '
                 '"reseller_customer_flag": '
                 '"is_reseller_customer", '
                 '"reseller_marketing_opted_in_flag": '
                 '"is_reseller_marketing_opted_in", '
                 '"small_business_owner_flag": '
                 '"is_small_business_owner", '
                 '"smart_line_only_customer_flag": '
                 '"is_smart_line_only_customer", '
                 '"sms_marketing_opted_in_flag": '
                 '"is_sms_marketing_optedin", '
                 '"social_marketing_opted_in_flag": '
                 '"is_social_marketing_optedin", '
                 '"standard_suppression_flag": '
                 '"is_standard_suppression", '
                 '"unique_email_all_customers_flag": '
                 '"is_unique_email_all_customers", '
                 '"unique_email_godaddy_customers_flag": '
                 '"is_unique_email_godaddy_customers", '
                 '"unique_email_reseller_customers_flag": '
                 '"is_unique_email_reseller_customers", '
                 '"webpro_flag": '
                 '"is_webpro", '
                 '"webpro_not_signed_nds_flag": '
                 '"is_webpro_not_signed_nds", '
                 '"wechat_marketing_opted_in_flag": '
                 '"is_wechat_marketing_optedin", '
                 '"whatsapp_marketing_opted_in_flag": '
                 '"is_whatsapp_marketing_opted_in", '
                 '"likely_godaddy_pro_score": '
                 '"likely_godaddy_pro", '
                 '"likely_godaddy_pro_mst_ts": '
                 '"likely_godaddy_pro_ts", '
                 '"valid_phone1_flag": '
                 '"is_valid_phone1", '
                 '"valid_phone2_flag": '
                 '"is_valid_phone2", '
                 '"social_retarget_opted_in_flag": '
                 '"is_social_retarget_opted_in", '
                 '"employees_likely_flag": '
                 '"is_employees_likely", '
                 '"churned_flag": '
                 '"is_churned", '
                 '"partners_experience_flag": '
                 '"is_partners_experience", '
                 '"do_not_track_flag": '
                 '"is_do_not_track", '
                 '"pick_best_id": '
                 '"pick_best", '
                 '"ncs_30_suppression_flag": '
                 '"is_ncs30_suppression"}'
                )


with DAG(
        dag_id=DAG_ID,
        max_active_runs=1,
        catchup=False,
        schedule_interval=None if Variable.get('mwaa_env') == 'dev-private' else '0 7 * * *',
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
                poke_interval=timedelta(minutes=10).seconds,
                timeout=timedelta(hours=6).seconds,
                retries=18))

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
        number_of_core_instances=8,
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
                  f'{BASE_SPARK_COMMAND} '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/{DAG_ID}/spark/{DAG_ID}_history.py '
                  '-d mdm_local '
                  '-t customer_history '
                  f'-e {ENV} '
                  f'-k {TEST_KEY} '
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    ingest_data_main = RunEMRStepOperator(
        task_id='emr_ingest_data_main',
        create_emr_task_id=emr_task_id,
        aws_conn_id=AWS_CONN_ID,
        retries=1,
        retry_delay=timedelta(minutes=10),
        waiter_poke_interval=60,
        waiter_poke_max_attempts=120,
        step_name='step_ingest_data',
        step_cmd=(
                  f'{BASE_SPARK_COMMAND} '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/utils/spark/ingest_data_main.py '
                  '-d mdm_local '
                  '-t customer '
                  f'-e {ENV}'
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    # Send Success notification to GD DataLake and update data location.
    utcnow = datetime.utcnow()
    s3_datalake = f's3://gd-mktgdata-{ENV}-egress/mdm_local'
    location = f'{s3_datalake}/customer/partition_utc_date={utcnow.strftime("%Y-%m-%d")}/'

    data_lake_success = SnapshotSuccessNotificationOperator(
        task_id='data_lake_success',
        db_name='marketing_egress_mart',
        table_name='marketable_customer_ext',
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
                  f'{BASE_SPARK_COMMAND} '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/utils/spark/extract_data.py '
                  '-d mdm_local '
                  '-t customer '
                  '-m 60000000000 '
                  '-np 10 '
                  f'-e {ENV} '
                  f'-l {LOAD_TYPE} '
                  f'-dc {shlex.quote(DAG_CONFIG)} '
                  f'-cr {shlex.quote(CUSTOM_RENAME)}'
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
