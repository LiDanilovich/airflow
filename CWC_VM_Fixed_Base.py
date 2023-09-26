
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from datetime import datetime, timedelta
from airflow import DAG
from urllib.parse import urlparse

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


Dag_Name = "CWC_VM_Fixed_Base"
strName = "Caribe - Procesa DNA Fixed"

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWC VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

      
      
      dnaContingencia = SSHOperator(
        task_id=f"dnaCont_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_DNA_FIX_CONTINGENCIA.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
      
       
      stg = SSHOperator(
        task_id=f"stg_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      stg1 = SSHOperator(
        task_id=f"stg1_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      stg2 = SSHOperator(
        task_id=f"stg2_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_2.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

            
      churn = SSHOperator(
        task_id=f"churn_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_CHRN.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

        
      

      
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >>  dnaContingencia >> stg >> stg1 >> stg2  >> jobEnd
      jobStart >> churn >> jobEnd
     

