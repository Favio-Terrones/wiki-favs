from airflow import DAG
from util.plugins_composer_silver_plugins import get_scripts_in_folder, get_parameters, build_template, get_current_file_script
from util.plugins_composer_golden_plugins import get_months_last_days_list, get_periodos_for_dia_corte
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from pathlib import Path
import os
from datetime import datetime, timedelta
import time
import util.migration_utils
from util.custom_operators.error_handler_operator import ErrorHandlerOperator

# -- Constantes:
CARPETA_MODELO_DATOS = "demo_carga_mixta"  # i.e.: slv_modelo_venta_odoo_mk_dex, gld_cliente_sidex
COD_CAPA_DATOS = "SLV"  # i.e.: SLV, GLD, DLV, DTG

# -- Ambiente : dev(desarrollo) / dd(data domain) / prod(produccion)
ambiente = os.environ["COD_AMBIENTE"]
dataset_ambiente = os.environ["ENV_DATASET"]
base_data_folder = os.environ["BASE_DATA_FOLDER"]

# -- Variables de Proyectos
id_brz = "BRZ_"+ambiente.upper()+"_PROJECT_ID"
id_brz_dependencies = "BRZ_"+ambiente.upper()+"_PROJECT_ID"
id_slv = "SLV_"+ambiente.upper()+"_PROJECT_ID"
id_gld = "GLD_"+ambiente.upper()+"_PROJECT_ID"
id_dtg = "DTG_"+ambiente.upper()+"_PROJECT_ID"

if COD_CAPA_DATOS == 'SLV':
    cod_layer = 'SLV'
    gsa_suffix = 'BRZ_TO_SLV'
elif COD_CAPA_DATOS == 'GLD':
    cod_layer = 'GLD'
    gsa_suffix = 'SLV_TO_GLD'
elif COD_CAPA_DATOS == 'DLV':
    cod_layer = 'GLD'
    gsa_suffix = 'GLD_TO_DLV'
elif COD_CAPA_DATOS == 'DTG':
    cod_layer = 'DTG'
    gsa_suffix = 'DLK_TO_DTG'

id_layer = f"{cod_layer}_{ambiente.upper()}_PROJECT_ID"

bronze_project_id = os.environ[id_brz]
bronze_project_id_dependencies = os.environ[id_brz_dependencies]
silver_project_id = os.environ[id_slv]
golden_project_id = os.environ[id_gld]
datagov_project_id = os.environ[id_dtg]
layer_project_id = os.environ[id_layer]

# -- Impersonalización de service accounts (GSAs)
id_email_sa = f"GSA_EMAIL_{ambiente.upper()}_{gsa_suffix}"
id_dag_execution_sa = 'GSA_DAG_EXECUTION'
impersonation_chain_sa = util.migration_utils.load_gsa_email(id_email_sa)
impersonation_chain_sa_dag_execution = util.migration_utils.load_gsa_email(id_dag_execution_sa)

# -- Carpeta del modelo de datos (DML)
stg_modelo_path = f'{ambiente}/{CARPETA_MODELO_DATOS}/*.sql'

# -- Leer "parameters.json"
parameters = get_parameters(stg_modelo_path)
parameter_dag = parameters['dag_id']
parameter_dag_id = parameter_dag.replace("{env}", ambiente)

write_dispositions = parameters['write_dispositions']
mails_recipients = parameters['mail_responsables']

retries = int(parameters['retries']) if 'retries' in parameters else 1
retry_delay = int(parameters['retry_delay']) if 'retry_delay' in parameters else 5

dependencies_tables = parameters['dependencies_table'].replace("{bronze_project_id}", bronze_project_id_dependencies).replace("{dataset_env}",dataset_ambiente)
cod_historico = parameters['historico']

scripts_sql_partition_load = [
    script
    for write_disposition in parameters['write_dispositions']
    for script in write_disposition['scripts']
    if write_disposition['load_type'] == 'PARTITION_LOAD'
]

scripts_sql_full_load = [
    script
    for write_disposition in parameters['write_dispositions']
    for script in write_disposition['scripts']
    if write_disposition['load_type'] == 'FULL_LOAD'
]

# -- Obtener meses de ejecución (considerar mes anterior si aplica la fecha de corte)
if cod_historico == "1":
    # se hace un query por periodo entre periodoinicio y periodofin
    periodoinicio = parameters['periodoinicio']
    periodofin = parameters['periodofin']
    month_list = get_months_last_days_list(periodoinicio, periodofin)
elif cod_historico == "2":
    # se ejecutan los ultimos 6 meses
    nowdate = datetime.datetime.today()
    previousdate = nowdate - datetime.timedelta(days=180)

    periodofin = str(nowdate.strftime("%Y-%m"))
    periodoinicio = str(previousdate.strftime("%Y-%m"))
    month_list = get_months_last_days_list(periodoinicio, periodofin)
else:
    # se ejecutan el mes actual y el mes anterior hasta el dia de corte, luego solo el mes actual
    dia_corte = parameters['dia_corte']
    month_list = get_periodos_for_dia_corte(dia_corte)
    if len(month_list) > 1:
        sleep_time = 60
    else:
        sleep_time = 0


# -- Configuracion DAG
STATUS_FAILED = 'FAILED'
STATUS_SUCCEEDED = 'SUCCEEDED'
YESTERDAY = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_dag_args = {
    'start_date': YESTERDAY,
    'email': mails_recipients,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': retries,
    'retry_delay': timedelta(minutes=retry_delay),
    'project_id': layer_project_id
}

with DAG(dag_id=parameter_dag_id, schedule_interval=None, default_args=default_dag_args) as dag:

    # -- Definir operadores para task "all_failed" y "all_success"
    folder_templates = get_scripts_in_folder(os.environ["FOLDER_TEMPLATES"])
    template_script = get_current_file_script(folder_templates[0])
    failed_query_template = build_template(template_script, dependencies_tables, STATUS_FAILED, parameter_dag_id)
    succeeded_query_template = build_template(template_script, dependencies_tables, STATUS_SUCCEEDED, parameter_dag_id)

    all_failed = ErrorHandlerOperator(
        task_id='update_config_table_with_{0}'.format(STATUS_FAILED),
        query=failed_query_template,
        impersonation_chain=impersonation_chain_sa_dag_execution,
        project_id=bronze_project_id,
        trigger_rule="one_failed",
        dag=dag,
        alert_code='DAG_EXECUTION'
    )

    all_success = BigQueryInsertJobOperator(
        task_id='update_config_table_with_{0}'.format(STATUS_SUCCEEDED),
        job_id="{{ ts_nodash }}-" + STATUS_SUCCEEDED,
        configuration={"query": {"query": succeeded_query_template, "useLegacySql": "False"}},
        gcp_conn_id='bigquery_custom',
        impersonation_chain=impersonation_chain_sa_dag_execution,
        project_id=bronze_project_id,
        trigger_rule="all_success",
        dag=dag)

    sleep_task = PythonOperator(
        task_id='sleep_task',
        python_callable=lambda: time.sleep(sleep_time)
    )

    task_dict = dict()

    # FULL LOAD
    # -- Definir operadores para task para ejecución de scripts SQL
    for current_script_sql in scripts_sql_full_load:
        sql_script_path = f'{base_data_folder}/{ambiente}/{CARPETA_MODELO_DATOS}/{current_script_sql}.sql'
        sql_script_string = get_current_file_script(sql_script_path)
        sql_script_string = sql_script_string\
            .replace("{bronze_project_id}", bronze_project_id)\
            .replace("{silver_project_id}", silver_project_id)\
            .replace("{golden_project_id}", golden_project_id)\
            .replace("{datagov_project_id}", datagov_project_id)

        script_name = Path(current_script_sql).stem
        task_id = script_name
        task_dict[task_id] = BigQueryInsertJobOperator(
            task_id=task_id,
            job_id="{{ ts_nodash }}-" + task_id,
            execution_timeout=timedelta(minutes=120),
            configuration={
                "query": {
                    "query": sql_script_string,
                    "useLegacySql": "False",
                    "timeoutMs": 7200000
                }
            },
            gcp_conn_id="bigquery_custom",
            impersonation_chain=impersonation_chain_sa,
            dag=dag
        )

    # PARTITION LOAD
    # -- Definir operadores para task para ejecución de scripts SQL
    for month in month_list:
        for current_script_sql in scripts_sql_partition_load:
            sql_script_path = f'{base_data_folder}/{ambiente}/{CARPETA_MODELO_DATOS}/{current_script_sql}.sql'
            sql_script_string = get_current_file_script(sql_script_path)
            sql_script_string = sql_script_string\
                .replace("{bronze_project_id}", bronze_project_id)\
                .replace("{silver_project_id}", silver_project_id)\
                .replace("{golden_project_id}", golden_project_id)\
                .replace("{datagov_project_id}", datagov_project_id)

            sql_script_string = sql_script_string.replace("{periodo}", month)

            script_name = Path(current_script_sql).stem
            task_id = f"{script_name}-{month}"
            task_dict[task_id] = BigQueryInsertJobOperator(
                task_id=task_id,
                job_id="{{ ts_nodash }}-" + task_id,
                execution_timeout=timedelta(minutes=120),
                configuration={
                    "query": {
                        "query": sql_script_string,
                        "useLegacySql": "False",
                        "timeoutMs": 7200000
                    }
                },
                gcp_conn_id="bigquery_custom",
                impersonation_chain=impersonation_chain_sa,
                dag=dag
            )

    # SLEEP TASK: Evitar ejecucion concurrente para DAGs con fecha de corte
    sleep_task >> task_dict[f"script_01-{month}"]

    # "PARTITION LOAD" TASKs dependencies
    for month in month_list:
        task_dict[f"script_01-{month}"] >> task_dict[f"script_02-{month}"]
        # Linkear el último task de "partition load" al primer task "full load"
        task_dict[f"script_02-{month}"] >> task_dict['script_03']

    # "FULL LOAD" TASKs dependencies
    task_dict['script_03'] >> task_dict['script_04_final'] >> [all_success, all_failed]
