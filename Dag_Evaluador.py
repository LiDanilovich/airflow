
from datetime import datetime, timedelta, timezone, date
from airflow import DAG

from airflow.models import XCom,Variable
from airflow.models.dagrun import DagRun, DagRunState


default_args = {
    'owner':  'Lili Danilovich',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
}


dag_id = 'EVALUADOR'


   

def choose_branch(task_instance, dag_run):
    
    # Controlo en datalake fechas para PREPAID_RCHG_HST posteriores a la data ya descargada en LANDING
    
    
    hookDL = OracleHook(oracle_conn_id="OracleLLA")
    # ultima fecha en el datalake
    max_dl = hookDL.get_first(sql="SELECT max(day_key) FROM DATALAKE.DL_PREPAID_RCHG_HST")
    if max_dl[0] is None:
       max_dl_dt = date.today()
    else:
       max_dl_dt = max_dl[0]
    print(max_dl_dt) 
   
    # fecha esperada
    fecha = date.today()-timedelta(days=1) 
    print('la fecha esperada es {}'.format(fecha.strftime('%Y%m%d')))

    hook = OracleHook(oracle_conn_id="Oracle")
    # ultima fecha en landing... si está vacía asumo la fecha actual -1
    max_la = hook.get_first(sql="SELECT max(day_key) FROM LANDING.LA_PREPAID_RCHG_HST")
    if max_la[0] is None:
       max_la_dt = 0
    else:
       max_la_dt = max_la[0]

    print('max_la: {}'.format(max_la_dt)) 
    la_dt_max = datetime.strptime(str(max_la_dt),'%Y%m%d').date()
    print(la_dt_max)

    if la_dt_max >= max_dl_dt :
       print('todo actualizado')
       return 'finalizar'
    else:   
       dif = max_dl_dt - la_dt_max
       print(dif)
       return 'esperar_y_relanzar'


with DAG(
    dag_id,
    start_date=datetime(year=2023, month=2, day=2),
    tags=['CWP VM'],
    default_args=default_args,
    description='Evalua condición de inicio',
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
 
    consumir = TriggerDagRunOperator(
        task_id='consumir',
        trigger_dag_id='CWP_PREPAID_RCHG_HST',
        wait_for_completion=False,
        reset_dag_run=False
        
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

    hora_limite = BashOperator(
        task_id='hora_limite',
        bash_command='sleep 180',
        dag=dag,
    )

    jobEnd = DummyOperator(task_id=f'End_{dag_id}', dag=dag)
    
    jobStart >>  branch 
    branch >>  [hora_limite, consumir, esperar_y_relanzar, ]
    consumir >> finalizar
    hora_limite >> finalizar >> jobEnd
    esperar_y_relanzar >> relanzar >> jobEnd
