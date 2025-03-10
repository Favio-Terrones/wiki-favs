from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator 
from airflow.operators.python import BranchPythonOperator
import pytz

from util.custom_operators.error_handler_operator import ErrorHandlerOperator
from util import dag_config_parameters

# -- Constantes:
DAG_ID = 'alicorp-pe-supply-prod-trv-transportes'
EMAIL_RECIPIENTS = ['ext_mdelgadoa@alicorp.com.pe', 'ext_fterronesc@alicorp.com.pe']
SCHEDULE_INTERVAL = '0 */4 * * *'

MAPPING_CARPETAS = [
    {
        'carpeta_scripts_sql': 'slv_modelo_transporte_s4',
        'cod_capa_datos': 'SLV',
        'sql_scripts': [
            's4_documento_entrega_cabecera',
            's4_documento_entrega_detalle',
            's4_documento_envio_cabecera',
            's4_documento_envio_detalle',
            's4_documento_envio_etapa'
        ]
    },
    {
        'carpeta_scripts_sql': 'slv_modelo_ventas_s4',
        'cod_capa_datos': 'SLV',
        'sql_scripts': [
            's4_documento_cabecera',
            's4_documento_cabecera_aux',
            's4_documento_detalle',
            's4_documento_detalle_aux',
            's4_pedido_cabecera',
            's4_pedido_detalle',
            's4_pedido_reparto',
            's4_flujo_documento',
            's4_pedido_detalle_comercial'
        ]
    },
    {
        'carpeta_scripts_sql': 'slv_modelo_transporte_tms',
        'cod_capa_datos': 'SLV',
        'sql_scripts': [
            'tms_orden_flete_base',
            'tms_orden_flete_consolidado',
            'tms_orden_flete_gestion'
        ]
    },
    {
        'carpeta_scripts_sql': 'gld_transporte_s4',
        'cod_capa_datos': 'GLD',
        'sql_scripts': [
            's4_avance_control_facturacion_letra'
        ]
    },
      {
        'carpeta_scripts_sql': 'gld_transporte_tms',
        'cod_capa_datos': 'GLD',
        'sql_scripts': [
            'tms_avance_control_facturacion'
        ]
    } 
    ,
    {
        'carpeta_scripts_sql': 'delivery_transporte',
        'cod_capa_datos': 'DLV',
        'sql_scripts': [
            'cr_avance_control_facturacion_historico',
            'cr_avance_control_facturacion',
            'cr_avance_control_facturacion_actualizado',
        ]
    }
]


# -- Configuracion DAG
tz = pytz.timezone('America/Lima')

STATUS_FAILED = 'FAILED'
STATUS_SUCCEEDED = 'SUCCEEDED'
YESTERDAY = datetime.combine(datetime.now(tz=tz).date() - timedelta(days=1), datetime.min.time())

periodo_actual = datetime.now(tz=tz).replace(day=1)
periodo_anterior = (periodo_actual - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')

default_dag_args = {
    'start_date': YESTERDAY,
    'email': EMAIL_RECIPIENTS,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag_config_parameters = dag_config_parameters.generate_dag_config_parameters(dag_id=DAG_ID, dag_folder_config=MAPPING_CARPETAS)

# -- PythonFunctions
def fn_validar_horario():
    tz = pytz.timezone('America/Lima')  
    ahora = datetime.now(tz)  # Usa la zona horaria configurada
    if ahora.hour in range(0, 4):   
        return "DLV_cr_avance_control_facturacion_historico"
    else:
        return "update_config_table_with_SUCCEEDED"



with DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_dag_args,
    catchup=False
) as dag:

    # -- Definir operadores para task "all_failed" y "all_success"
    status_config = dag_config_parameters['dag_status_config']

    all_failed = ErrorHandlerOperator(
        task_id='update_config_table_with_{0}'.format(STATUS_FAILED),
        query=status_config['query'][STATUS_FAILED],
        impersonation_chain=status_config['service_account_to_impersonate'],
        project_id=status_config['bigquery_project_id'],
        trigger_rule="one_failed",
        dag=dag,
        alert_code='DAG_EXECUTION'
    )

    all_success = BigQueryInsertJobOperator(
        task_id='update_config_table_with_{0}'.format(STATUS_SUCCEEDED),
        job_id="{{ ts_nodash }}-" + STATUS_SUCCEEDED,
        configuration={"query": {"query": status_config['query'][STATUS_SUCCEEDED], "useLegacySql": "False"}},
        impersonation_chain=status_config['service_account_to_impersonate'],
        project_id=status_config['bigquery_project_id'],
        trigger_rule="none_failed",
        dag=dag)
    

    # -- Definir operadores para task para ejecución de scripts SQL
    scripts_config = dag_config_parameters['dag_scripts_config']

    task_dict = dict()
    for folder_name, folder_config in scripts_config.items():
        for sql_script_name, sql_script_content in folder_config['sql_scripts_data'].items():
            task_id = f'{folder_config["cod_capa_datos"]}_{sql_script_name}'

            sql_script_content = sql_script_content.replace('{periodo}', periodo_anterior)

            task_dict[task_id] = BigQueryInsertJobOperator(
                task_id=task_id,
                job_id="{{ ts_nodash }}-" + task_id,
                configuration={"query": {"query": sql_script_content, "useLegacySql": "False"}},
                impersonation_chain=folder_config['service_account_to_impersonate'],
                project_id=folder_config['bigquery_project_id'],
                dag=dag
            )


    # -- Definir dependencias entre scripts SQL
    task_dict["SLV_s4_documento_entrega_cabecera"] >> task_dict["SLV_s4_documento_envio_cabecera"]
    task_dict["SLV_s4_documento_envio_cabecera"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_documento_envio_cabecera"] >> task_dict["GLD_tms_avance_control_facturacion"]

    task_dict["SLV_s4_documento_entrega_detalle"] >> task_dict["SLV_s4_documento_envio_detalle"]
    task_dict["SLV_s4_documento_envio_detalle"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_documento_envio_detalle"] >> task_dict["GLD_tms_avance_control_facturacion"]

    task_dict["SLV_s4_documento_envio_etapa"] >> task_dict["SLV_s4_documento_detalle"]
    task_dict["SLV_s4_documento_detalle"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_documento_detalle"] >> task_dict["GLD_tms_avance_control_facturacion"]

    task_dict["SLV_s4_documento_cabecera"] >> task_dict["SLV_s4_documento_detalle_aux"]
    task_dict["SLV_s4_documento_detalle_aux"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_documento_detalle_aux"] >> task_dict["GLD_tms_avance_control_facturacion"]

    task_dict["SLV_s4_documento_cabecera_aux"] >> task_dict["SLV_s4_pedido_cabecera"]
    task_dict["SLV_s4_pedido_cabecera"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_pedido_cabecera"] >> task_dict["GLD_tms_avance_control_facturacion"]


    task_dict["SLV_s4_pedido_detalle"] >>  task_dict["SLV_s4_pedido_detalle_comercial"]
    task_dict["SLV_s4_pedido_detalle_comercial"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_pedido_detalle_comercial"] >> task_dict["GLD_tms_avance_control_facturacion"]

    task_dict["SLV_s4_pedido_reparto"] >>  task_dict["SLV_s4_flujo_documento"]
    task_dict["SLV_s4_flujo_documento"] >> task_dict["SLV_tms_orden_flete_base"]
    task_dict["SLV_s4_flujo_documento"] >> task_dict["GLD_tms_avance_control_facturacion"]


    task_dict["SLV_tms_orden_flete_base"] >> task_dict["SLV_tms_orden_flete_consolidado"]
    task_dict["SLV_tms_orden_flete_consolidado"] >> task_dict["GLD_tms_avance_control_facturacion"]
 
    task_dict["SLV_tms_orden_flete_gestion"] >> task_dict["GLD_tms_avance_control_facturacion"]
    task_dict["GLD_tms_avance_control_facturacion"] >> task_dict["GLD_s4_avance_control_facturacion_letra"]

    task_dict["GLD_s4_avance_control_facturacion_letra"] >> task_dict["DLV_cr_avance_control_facturacion_actualizado"]


    validacion_horario = BranchPythonOperator(
        task_id = 'validacion_horario',
        python_callable=fn_validar_horario,
        dag=dag
    ) 

    task_dict["DLV_cr_avance_control_facturacion_actualizado"] >> validacion_horario

    # -- Configurar dependencias del BranchPythonOperator
    validacion_horario >> task_dict["DLV_cr_avance_control_facturacion_historico"]
    task_dict["DLV_cr_avance_control_facturacion_historico"] >> task_dict["DLV_cr_avance_control_facturacion"]

    

    # Tareas finales           
    [all_success, all_failed] << validacion_horario
    [all_success, all_failed] << task_dict["DLV_cr_avance_control_facturacion"]