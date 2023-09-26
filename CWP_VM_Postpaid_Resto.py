
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
'start_date': datetime(year=2022, month=12, day=20),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}


Dag_Name = "CWP_VM_Postpaid_Resto"
strName = "Panama - VM Postpaid Calculadas"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWP VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

      plan = SSHOperator(
        task_id=f"plan_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_POSPAID_PLAN.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )      
      
      pre_subs = SSHOperator(
        task_id=f"pre_subs_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_PRE_SUBSCRIBER_POSTPAID_v1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )  
      
      
      dm_subs = SSHOperator(
        task_id=f"dm_subs_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_SUBSCRIBER_POSTPAID_v1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )  

      dm_subs_hst = SSHOperator(
        task_id=f"dm_subs_hst_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_SUBSCRIBER_POST_STS_HST.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )  


      
      rate = SSHOperator(
        task_id=f"rate_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_POSTPAID_RATEPLANCHG.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

            
      vm_postpaid = SSHOperator(
        task_id='vm_postpaid',
        command= "/bin/bash /home/sasdemo/./vmcwp_postpaid.sh ",
        ssh_hook = sshHook,
        dag=dag
      )  
      
    
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> plan >> pre_subs >> dm_subs >> dm_subs_hst >> rate >> vm_postpaid >> jobEnd
     
     

