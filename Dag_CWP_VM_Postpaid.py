
from datetime import datetime, timedelta, timezone
#import json
import time
import airflow
from airflow import DAG
from airflow.models import XCom,Variable
from airflow.models.dagrun import DagRun, DagRunState
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner':  'Lili Danilovich',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


dag_id = 'CWP_VM_Postpaid'

# variables para triggers sucesivos
branch_id = 'DNA_HoraLímite' 
trigger_dag_id = 'CWP_Compare_DNA_VM_Postpaid'


nro_ejecucion = Variable.get('CWP_VM_POSTPAID_EJEC')

def control_hora(horaLim, minLim):
    wtr = datetime.now(timezone.utc)
    #wtr = datetime.now()
    lim  = datetime(year=wtr.year, month=wtr.month, day=wtr.day, hour=horaLim, minute=minLim, tzinfo=timezone.utc)
    print(wtr, lim)
    if lim < wtr:
       return 1
    else: return 0   
    

def choose_branch(task_instance, dag_run):
    # si es hora límite actualiza y termina
    #check_hora = control_hora(int(Variable.get('CWC_VM_FIXED_HORA_LIM').split(':')[0]),int(Variable.get('CWC_VM_FIXED_HORA_LIM').split(':')[1])) 
    
    #controlo resultado del triggered_dag

    actual_ejecucion = dag_run.execution_date
    actual_conf = dag_run.conf
    
    print('config {}'.format(actual_conf))

    if 'ejecucion' in actual_conf:
        prox_ejecucion = int(actual_conf['ejecucion']) + 1
    else:
        prox_ejecucion = 1   
    
    print('prox_ejecucion {}'.format(prox_ejecucion))
    
    Variable.update(key='CWP_VM_POSTPAID_EJEC',value=prox_ejecucion)
   

    dag_runs = DagRun.find(dag_id='CWP_Compare_DNA_VM_Postpaid', state=DagRunState.SUCCESS)
  
    check_hora = control_hora(int(Variable.get('CWP_VM_POSTPAID_HORA_LIM').split(':')[0]),int(Variable.get('CWP_VM_POSTPAID_HORA_LIM').split(':')[1])) 
    print('check hora {}'.format(check_hora))

    if len(dag_runs) >= 1:
       last = dag_runs[-1]
       print('Esta ejecución{}'.format(actual_ejecucion))    
       print('ejecución del triggered {}'.format(last.execution_date))
       # tiene que ser la ejecución lanzada x este dag
       if last.execution_date > actual_ejecucion :
            # veo si obtuvo nuevo DNA o no
            valueKey = XCom.get_one(run_id = last.run_id,
                                   key = 'newDNA',
                                   task_id = 'controlNovedades',
                                   dag_id = 'CWP_Compare_DNA_VM_Postpaid',
                                   )
            print('valueKey {}'.format(valueKey))
            if valueKey == '1':
                print('procesar dna')
                return  'trigger_VM_Postpaid_Base'

            elif valueKey == '0' and check_hora != 1:
                # tiene que reintentar no llegó dna ni es hora límite
                print('reintentar ejecucion {}'.format(prox_ejecucion))
                return 'sleep_task'
            
            elif valueKey == '2' or check_hora == 1:
                print('ya se actualizó primer parte DNA o es hora límite')
                return 'trigger_VM_Postpaid_Resto'
            
            else:        
                print('problemas ...{}'.format(valueKey))
                return f'Error_{dag_id}'

       elif last.execution_date == actual_ejecucion:
             print('problemas con la ejecución de controlNovedadesDag')
             return f'Error_{dag_id}' 
       else: 
             print('esta ejecución es mayor - ver controlNovedadesDag')
             return f'Error_{dag_id}'




with DAG(
    dag_id,
    start_date=days_ago(2),
    tags=['CWP VM'],
    default_args=default_args,
    description='VM Postpaid - CWP',
    schedule_interval="00 13-15 * * *"
    ) as dag:
   


    jobStart = DummyOperator(task_id=f'Start_{dag_id}', dag=dag)

   
    checkDNA = TriggerDagRunOperator(
        task_id='checkDNA',
        trigger_dag_id = 'CWP_Compare_DNA_VM_Postpaid',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30               
    )
    
    branch = BranchPythonOperator(
        task_id=branch_id,
        python_callable=choose_branch
    )
 
    trigger_VM_Postpaid_Base = TriggerDagRunOperator(
        task_id='trigger_VM_Postpaid_Base',
        trigger_dag_id = 'CWP_VM_Postpaid_Base',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
    )

    trigger_VM_Postpaid_Resto = TriggerDagRunOperator(
        task_id='trigger_VM_Postpaid_Resto',
        trigger_dag_id = 'CWP_VM_Postpaid_Resto',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
    )
    
    trigger_VM_Postpaid_Backups = TriggerDagRunOperator(
        task_id='trigger_VM_Postpaid_Backups',
        trigger_dag_id = 'CWP_VM_Postpaid_Backups',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
    )      
    sleep_task = BashOperator(
        task_id='sleep_task',
        bash_command='sleep 120',
        dag=dag,
    )

    trigger_repeat = TriggerDagRunOperator(
        task_id='repeat_task',
        trigger_dag_id=dag_id,
        conf={ 'ejecucion': Variable.get('CWP_VM_POSTPAID_EJEC') }
    )
    
    jobError = DummyOperator(task_id=f'Error_{dag_id}', dag=dag)
    jobEnd = DummyOperator(task_id=f'End_{dag_id}', dag=dag)
    
    jobStart >> checkDNA >> branch 
    branch >>  [trigger_VM_Postpaid_Base, trigger_VM_Postpaid_Resto, sleep_task, jobError ]
    sleep_task >> trigger_repeat
    trigger_VM_Postpaid_Base >> trigger_VM_Postpaid_Resto
    trigger_VM_Postpaid_Resto >> trigger_VM_Postpaid_Backups >> jobEnd
