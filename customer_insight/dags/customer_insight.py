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
- Dataset Name         : customer_insight
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

DAG_ID = 'customer_insight'
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
    'marketing_mart.marketable_customer',
    'marketing_mart_local.ads_product',
    'marketing_mart_local.payment_profile_snap',
    'venture_mart.domain_footprint_history',
    'venture_mart.dns_nameserver_records_cln',
    'venture_mart.dns_a_records_cln',
    'venture_mart.venture_footprint_history',
    'callcenterreporting.rptfactc3inboundcall_snap',
    'analytic_feature.customer_type'
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
PY_FILES = f'--py-files {CODE_PATH}/customer_insight/spark/sql/*,{CODE_PATH}/utils/*'
CUSTOM_RENAME = (
                 '{"next_auto_renewing_expiration_utc_ts": '
                 '"next_auto_renewing_expiration_ts", '
                 '"next_payment_expiration_utc_ts": '
                 '"next_payment_expiration_ts", '
                 '"latest_site_visit_utc_ts": '
                 '"latest_site_visit_ts", '
                 '"latest_professional_search_page_visit_utc_ts": '
                 '"latest_professional_search_page_visit_ts", '
                 '"next_msoffice_expiration_utc_ts": '
                 '"next_msoffice_expiration_ts", '
                 '"next_ssl_expiration_utc_ts": '
                 '"next_ssl_expiration_ts", '
                 '"next_hosting_expiration_utc_ts": '
                 '"next_hosting_expiration_ts", '
                 '"next_site_builder_expiration_utc_ts": '
                 '"next_site_builder_expiration_ts", '
                 '"latest_seo_search_page_visit_utc_ts": '
                 '"latest_seo_search_page_visit_ts", '
                 '"purchased_email_migration_last_21_days_count": '
                 '"purchased_email_migration_last_21_days_count", '
                 '"life_time_gross_total_spend_usd_amt": '
                 '"life_time_gross_total_spend", '
                 '"purchased_hosting_migration_last_20_days_count": '
                 '"purchased_hosting_migration_last_20_days_count", '
                 '"next_expiring_active_payment_profile_utc_ts": '
                 '"next_expiring_active_payment_profile_ts", '
                 '"expired_domain_attached_to_contact_email_flag": '
                 '"is_expired_domain_attached_to_contact_email", '
                 '"expiring_domain_attached_to_contact_email_flag": '
                 '"is_expiring_domain_attached_to_contact_email", '
                 '"product_expiring_next_90_days_with_expired_payment_flag": '
                 '"is_product_expiring_next_90_days_with_expired_payment", '
                 '"unparked_domain_count": '
                 '"unparked_domains_count", '
                 '"o365_products_not_tied_to_domain_count": '
                 '"o365_products_not_tied_to_domain_count", '
                 '"expired_domain_within_90_days_protected_dop_flag": '
                 '"is_expired_domain_within_90_days_protected_dop", '
                 '"next_manual_renewal_expiring_with_list_price_over_20_utc_ts": '
                 '"next_manual_renewal_expiring_with_list_price_over_20_ts", '
                 '"open_exchange_product_count": '
                 '"open_exchange_product_count", '
                 '"website_express_protection_product_count": '
                 '"website_express_protection_product_count", '
                 '"website_deluxe_protection_product_count": '
                 '"website_deluxe_protection_product_count", '
                 '"website_firewall_protection_product_count": '
                 '"website_firewall_protection_product_count", '
                 '"website_ultimate_protection_product_count": '
                 '"website_ultimate_protection_product_count", '
                 '"hosted_domain_count": '
                 '"hosted_domains_count", '
                 '"hosted_internally_domain_count": '
                 '"hosted_internally_domains_count", '
                 '"new_account_product_count": '
                 '"count_of_new_account_products", '
                 '"last_failed_billing_product_utc_ts": '
                 '"last_failed_billing_product_ts", '
                 '"domain_discount_club_count": '
                 '"domain_discount_club_count", '
                 '"active_delegate_count": '
                 '"active_delegate_count", '
                 '"dot_com_domain_expire_ts_within_30_days_count": '
                 '"dot_com_domains_expire_ts_within_30_days_count", '
                 '"dot_net_domain_expire_ts_within_30_days_count": '
                 '"dot_net_domains_expire_ts_within_30_days_count", '
                 '"dot_org_domain_expire_ts_within_30_days_count": '
                 '"dot_org_domains_expire_ts_within_30_days_count", '
                 '"website_design_makeover_flag": '
                 '"is_website_design_makeover", '
                 '"website_design_intent_flag": '
                 '"is_website_design_intent", '
                 '"domain_expired_in_last_10_days_count": '
                 '"domains_expired_in_last_10_days_count", '
                 '"word_press_basic_deluxe_count": '
                 '"word_press_basic_deluxe_count", '
                 '"web_security_express_deluxe_ultimate_count": '
                 '"web_security_express_deluxe_ultimate_count", '
                 '"business_hosting_count": '
                 '"business_hosting_count", '
                 '"customer_active_venture_count": '
                 '"customer_active_venture_count", '
                 '"latest_expired_hosting_expiration_utc_ts": '
                 '"latest_expired_hosting_expiration_ts", '
                 '"full_domain_privacy_and_protection_count": '
                 '"full_domain_privacy_and_protection_count", '
                 '"ultimate_domain_protection_and_security_count": '
                 '"ultimate_domain_protection_and_security_count", '
                 '"latest_inbound_call_utc_ts": '
                 '"latest_inbound_call_ts", '
                 '"pnl_pillar_name": '
                 '"customer_pillar_daily", '
                 '"virtual_private_hosting_count": '
                 '"virtual_private_hosting_count", '
                 '"websites_plus_marketing_services_count": '
                 '"websites_plus_marketing_services_count", '
                 '"latest_websites_plus_marketing_services_order_utc_ts": '
                 '"latest_websites_plus_marketing_services_order_ts", '
                 '"latest_websites_plus_marketing_free_build_order_utc_ts": '
                 '"latest_websites_plus_marketing_free_build_order_ts", '
                 '"website_essential_protection_product_count": '
                 '"website_essential_protection_product_count", '
                 '"education_vertical_classification_count": '
                 '"education_vertical_classification_count", '
                 '"government_vertical_classification_count": '
                 '"government_vertical_classification_count", '
                 '"healthcare_vertical_classification_count": '
                 '"healthcare_vertical_classification_count", '
                 '"restaurant_vertical_classification_count": '
                 '"restaurant_vertical_classification_count", '
                 '"recreational_vertical_classification_count": '
                 '"recreational_vertical_classification_count", '
                 '"engineering_vertical_classification_count": '
                 '"engineering_vertical_classification_count", '
                 '"etl_first_load_utc_ts": '
                 '"etl_first_load_ts", '
                 '"etl_last_refresh_utc_ts": '
                 '"etl_last_refresh_ts"}'
                )


with DAG(
        dag_id=DAG_ID,
        max_active_runs=1,
        catchup=False,
        schedule_interval=None if Variable.get('mwaa_env') == 'dev-private' else '15 13 * * *',
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
        number_of_core_instances=4,
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
                  '-t customer_insight_history '
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
        waiter_poke_interval=60,
        waiter_poke_max_attempts=120,
        step_name='step_ingest_data',
        step_cmd=(
                  f'{BASE_SPARK_COMMAND} '
                  f'{PY_FILES} '
                  f'{CODE_PATH}/utils/spark/ingest_data_main.py '
                  '-d mdm_local '
                  '-t customer_insight '
                  f'-e {ENV}'
                 ),
        trigger_rule='none_failed_min_one_success'
    )

    # Send Success notification to GD DataLake and update data location.
    utcnow = datetime.utcnow()
    s3_datalake = f's3://gd-mktgdata-{ENV}-egress/mdm_local'
    location = f'{s3_datalake}/customer_insight/partition_utc_date={utcnow.strftime("%Y-%m-%d")}/'

    data_lake_success = SnapshotSuccessNotificationOperator(
        task_id='data_lake_success',
        db_name='marketing_egress_mart',
        table_name='customer_insight',
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
                  '-t customer_insight '
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
