
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

default_args = {
'owner': 'Lili Danilovich',                 # name de autor dag contact for support 
'depends_on_past': False,
'start_date': datetime(year=2023, month=1 , day=7),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}

Dag_Name = "CWP_VM_Eventos"
strName = "Panama - copia Eventos"

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWP VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

     from airflow.operators.dummy import DummyOperator
  
     jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)
     
     eventos = SSHOperator(
        task_id='eventos_prepaid',
        command= "/bin/bash /home/sasdemo/./copia_eventos.sh ",
        ssh_hook = sshHook,
        dag=dag
     )  

     jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

     jobStart >> eventos >> jobEnd 