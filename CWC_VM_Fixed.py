
import airflow 
import boto3
import pandas as pd
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from airflow import DAG
from urllib.parse import urlparse

#from airflow.contrib.operators.python_operator import PythonOperator

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")
# a hook can also be defined directly in the code:
# sshHook = SSHHook(remote_host='server.com', username='admin', key_file='/opt/airflow/keys/ssh.key')

default_args = {
'owner': 'Lili Danilovich',                 # name de autor dag contact for support 
'depends_on_past': False,
'start_date': datetime(year=2022, month=8, day=29),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}


Dag_Name = "CWC_VM_Fixed"
strName = "Caribe - VM Fixed"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWC VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

      trigger_DNA_Postpaid = TriggerDagRunOperator(
        task_id='trigger_DNA_Postpaid',
        trigger_dag_id = 'CWC_Compare_DNA_VM_Postpaid',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )

      
      stg3 = SSHOperator(
        task_id=f"stg3_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_POSTPAID_A_FIX.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      trigger_feedback_Fixed = TriggerDagRunOperator(
        task_id='trigger_feedback_Fixed',
        trigger_dag_id = 'CWC_feedbacks_Fixed',
        #execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )


      trigger_resto_Postpaid = TriggerDagRunOperator(
        task_id='trigger_resto_Postpaid',
        trigger_dag_id = 'CWC_calculadas_Postpaid',
        #execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval=30 
      )

      scores = SSHOperator(
        task_id=f"scores_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_SCORES_API.sas  /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
                  
      stg4 = SSHOperator(
        task_id=f"stg4_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_RESP.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      vm_fix = SSHOperator(
        task_id='vm_fix',
        command= "/bin/bash /home/sasdemo/./avasql.sh ",
        ssh_hook = sshHook,
        dag=dag
      )  
      

      evMail  = SSHOperator(
        task_id=f"evMail_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_EVENTOSMAIL.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
    
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> trigger_DNA_Postpaid >> stg3 >> scores >> stg4
      jobStart >> trigger_feedback_Fixed >> stg4 
      trigger_DNA_Postpaid >> trigger_resto_Postpaid
      stg4 >> vm_fix >> jobEnd
      jobStart >> evMail >> jobEnd
     

