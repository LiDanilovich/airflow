
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
from airflow.utils import trigger_rule

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


Dag_Name = "CWC_calculadas_Postpaid"
strName = "Caribe - Calculadas Postpaid"


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
        #execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )


      stg_evMail = SSHOperator(
        task_id=f"stg_evMail_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_EVENTOSMAIL_POSTPAID.sas  /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )


      stg_vm = SSHOperator(
        task_id=f"stg_vm_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_VM_SUSCRIBER_POSTPAID.sas  /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
      

      vm = SSHOperator(
        task_id=f"vm_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_VM_SUSCRIBER_POSTPAID.sas  /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )


      trigger_feedback_Postpaid = TriggerDagRunOperator(
        task_id='trigger_feedback_Postpaid',
        trigger_dag_id = 'CWC_feedbacks_Postpaid',
        #execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )
                  
            
      vm_hst = SSHOperator(
        task_id='vm_hst',
        command= "/bin/bash /home/sasdemo/./postpaid_hst_sql.sh ",
        ssh_hook = sshHook,
        dag=dag
      )  
      
      
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)
      jobEnd.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

      jobStart >> trigger_DNA_Postpaid >>  stg_vm >> vm >> vm_hst 
      jobStart >> trigger_feedback_Postpaid >> stg_vm 
      jobStart >> stg_evMail >> jobEnd
      vm_hst >> jobEnd
     

