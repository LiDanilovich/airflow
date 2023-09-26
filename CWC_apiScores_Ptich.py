import airflow 

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta
from airflow import DAG

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

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


Dag_Name = "CWC_apiScores_Ptich"
strName = "Caribe - API Scores PTich"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWC VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)
     
      obtener_scores = SSHOperator(
        task_id='obtener_scores',
        command= "/bin/bash /lla/dags/bash_codes/downscores_api_fix.sh",
        ssh_hook = sshHook,
        dag=dag
      )

      landing_scores = SSHOperator(
        task_id='landing_scores',
        command= "/bin/bash /lla/dags/bash_codes/j_lla_api_scores_copy.sh",
        ssh_hook = sshHook,
        dag=dag
      )

      
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> obtener_scores >> landing_scores >>  jobEnd


