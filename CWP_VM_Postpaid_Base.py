
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
'start_date': datetime(year=2023, month=1, day=7),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}


Dag_Name = "CWP_VM_Postpaid_Base"
strName = "Panama - VM Postpaid"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWP VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

     
      stg1 = SSHOperator(
        task_id=f"stg1_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_SUBSC_POSTPAID_STG_v1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> stg1 >> jobEnd
      

     

