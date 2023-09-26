
from datetime import datetime, timedelta, timezone, date
from airflow import DAG

from airflow.models import XCom,Variable
from airflow.models.dagrun import DagRun, DagRunState
from airflow.sensors.sql import SqlSensor


default_args = {
    'owner':  'Lili Danilovich',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
}


dag_id = 'CWP_PREPAID_RCHG_HST'


   

def choose_branch(task_instance, dag_run):
    
    # Controlo en datalake fechas para PREPAID_RCHG_HST posteriores a la data ya descargada en LANDING
    
    hookDL = OracleHook(oracle_conn_id="OracleLLA")
    
    
    # ultima fecha en el datalake
    max_dl = hookDL.get_first(sql="SELECT to_char(max(day_key)) FROM DATALAKE.DL_PREPAID_RCHG_HST")
    if max_dl[0] is None:
       max_dl_dt = date.today()
    else:
       max_dl_dt = max_dl[0]
    print(max_dl_dt) 
    dl_dt_max = datetime.strptime(max_dl_dt,'%Y%m%d').date()
    # fecha esperada
    fecha = date.today()-timedelta(days=1) 
    print('la fecha esperada es {}'.format(fecha.strftime('%Y%m%d')))


    # ultima fecha en landing... si está vacía asumo la fecha actual -1
    hook = OracleHook(oracle_conn_id="Oracle")
    max_la = hook.get_first(sql="SELECT to_char(max(day_key)) FROM LANDING.LA_PREPAID_RCHG_HST")
    if max_la[0] is None:
       max_la_dt = fecha.strftime('%Y%m%d')
    else:
       max_la_dt = max_la[0]

    print('max_la: {}'.format(max_la_dt)) 
    la_dt_max = datetime.strptime(max_la_dt,'%Y%m%d').date()
    print(la_dt_max)

    if la_dt_max >= dl_dt_max :
       print('todo actualizado')
       return 'finalizar'
    else:   
       dif = dl_dt_max - la_dt_max
       print(dif)
       return 'esperar_y_relanzar'


with DAG(
    dag_id,
    start_date=datetime(year=2023, month=2, day=2),
    tags=['CWP VM'],
    default_args=default_args,
    description='RECHARGE Prepaid - CWP',
    schedule_interval="00 13 * * *",
    
    ) as dag:

    from airflow.operators.dummy import DummyOperator
    from airflow.operators.python import BranchPythonOperator,PythonOperator
    from airflow.utils.dates import days_ago
    from airflow.operators.bash_operator import BashOperator
    from airflow.providers.oracle.hooks.oracle import OracleHook
    from airflow.operators.bash_operator import BashOperator
    from airflow.providers.ssh.hooks.ssh import SSHHook
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")


    jobStart = DummyOperator(task_id=f'Start_{dag_id}', dag=dag)

   
       
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=choose_branch
    )
 
    finalizar = BashOperator(
        task_id='finalizar',
        bash_command='sleep 10',
        dag=dag,
    )

    esperar_y_relanzar = BashOperator(
        task_id='esperar_y_relanzar',
        bash_command='sleep 180',
        dag=dag,
    )
    
    relanzar = TriggerDagRunOperator(
        task_id='relanzar',
        trigger_dag_id='CWP_PREPAID_RCHG_HST',
        wait_for_completion=False,
        reset_dag_run=False
        
    )


    jobEnd = DummyOperator(task_id=f'End_{dag_id}', dag=dag)
    
    jobStart >>  branch 
    branch >>  [finalizar, esperar_y_relanzar]
    finalizar >> jobEnd
    esperar_y_relanzar >> relanzar >> jobEnd
