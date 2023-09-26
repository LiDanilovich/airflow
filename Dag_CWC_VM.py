
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


dag_id = 'CWC_VMs'

# variables para triggers sucesivos
branch_id = 'DNA_HoraLímite' 
trigger_dag_id = 'CWC_Compare_DNA_VM_Fixed'

#prox_ejecucion = -1 
nro_ejecucion = Variable.get('CWC_VM_FIXED_EJEC')

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
    #print('check hora {}'.format(check_hora))
    #if check_hora == 1:
    #   print('hora límite') 
    #   return 'trigger_VM_Fixed'
    #else:   
    
    #controlo resultado del triggered_dag

    actual_ejecucion = dag_run.execution_date
    actual_conf = dag_run.conf
    
    print('config {}'.format(actual_conf))

    if 'ejecucion' in actual_conf:
        prox_ejecucion = int(actual_conf['ejecucion']) + 1
    else:
        prox_ejecucion = 1   
    
    print('prox_ejecucion {}'.format(prox_ejecucion))
    
    Variable.update(key='CWC_VM_FIXED_EJEC',value=prox_ejecucion)
   

    dag_runs = DagRun.find(dag_id='CWC_Compare_DNA_VM_Fixed', state=DagRunState.SUCCESS)
  
    check_hora = control_hora(int(Variable.get('CWC_VM_FIXED_HORA_LIM').split(':')[0]),int(Variable.get('CWC_VM_FIXED_HORA_LIM').split(':')[1])) 
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
                                   dag_id = 'CWC_Compare_DNA_VM_Fixed',
                                   )
            print('valueKey {}'.format(valueKey))
            if valueKey == '1':
                print('procesar dna')
                return  'trigger_VM_Fixed_Base'

            elif valueKey == '0' and check_hora != 1:
                # tiene que reintentar no llegó dna ni es hora límite
                print('reintentar ejecucion {}'.format(prox_ejecucion))
                return 'sleep_task'

            elif valueKey == '1':
                print('procesar dna')
                return  'trigger_ProcesaDNA_Fixed'
            
            elif valueKey == '2' or check_hora == 1:
                print('ya se actualizó primer parte DNA o es hora límite')
                return 'trigger_VM_Fixed'
            
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
    tags=['CWC VM'],
    default_args=default_args,
    description='VM Fixed & Postpaid - CWC',
    schedule_interval="40 3 * * *"
    ) as dag:
   


    jobStart = DummyOperator(task_id=f'Start_{dag_id}', dag=dag)

   
    checkDNA = TriggerDagRunOperator(
        task_id='checkDNA',
        trigger_dag_id = 'CWC_Compare_DNA_VM_Fixed',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30               
    )
    
    branch = BranchPythonOperator(
        task_id=branch_id,
        python_callable=choose_branch
    )
 
    trigger_VM_Fixed_Base = TriggerDagRunOperator(
        task_id='trigger_VM_Fixed_Base',
        trigger_dag_id = 'CWC_VM_Fixed_Base',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
    )


    trigger_VM_Fixed = TriggerDagRunOperator(
        task_id='trigger_VM_Fixed',
        trigger_dag_id = 'CWC_VM_Fixed',
        #execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = False,
        trigger_rule="none_failed_min_one_success",
        poke_interval=30        
    )

    # espera 10 minutos y lanza una nueva ejecución a la espera del dna  
    sleep_task = BashOperator(
        task_id='sleep_task',
        bash_command='sleep 600',
        dag=dag,
    )

    trigger_repeat = TriggerDagRunOperator(
        task_id='repeat_task',
        trigger_dag_id=dag_id,
        conf={ 'ejecucion': Variable.get('CWC_VM_FIXED_EJEC') }
    )
    
    jobError = DummyOperator(task_id=f'Error_{dag_id}', dag=dag)
    jobEnd = DummyOperator(task_id=f'End_{dag_id}', dag=dag)
    
    jobStart >> checkDNA >> branch 
    branch >>  [trigger_VM_Fixed_Base, trigger_VM_Fixed, sleep_task, jobError ]
    sleep_task >> trigger_repeat
  
    trigger_VM_Fixed_Base >> trigger_VM_Fixed
    trigger_VM_Fixed >> jobEnd
