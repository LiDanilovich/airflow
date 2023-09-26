from datetime import datetime, timedelta
import airflow 
from airflow import DAG
from urllib.parse import urlparse
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook



sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")


default_args = {
'owner': 'Lili Danilovich',                 # name de autor dag contact for support 
'depends_on_past': False,
'start_date': datetime(year=2023, month=1, day=25),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}


Dag_Name = "CWP_SRE_Recomendador"
strName = "Panama - SRE Recomendador"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWP VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

     from airflow.operators.dummy import DummyOperator
  
     jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

     landing_sre =  SSHOperator(
        task_id="landing_sre",
        command= '/bin/bash /lla/dags/bash_codes/downFromS3.sh sre_cwp_pre_offers.csv  s3://sre-cwp-prepaid-dev-serving/batch_inference/ /compartido/librerias/landing/wrk/ ',
        ssh_hook = sshHook,
        dag=dag
      )

     stg_sre = SSHOperator(
        task_id=f"stg_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_CWP_PRE_OFFERS.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

     jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

     jobStart >> landing_sre >> stg_sre >> jobEnd

  
