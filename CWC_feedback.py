
from datetime import date, datetime, timedelta
from socket import TCP_NODELAY
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

default_args = {
'owner': 'Airflow',                 # name de autor dag contact for support 
'depends_on_past': False,
'start_date': datetime(year=2022, month=1, day=20),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10)# The time pause to back retries
}


Dag_Name   = "CWC_feedbacks"
strName    = "CWC Descarga archivos de feedback"

def defineDate(ti):
    ti.xcom_push(key='fechaPath', value=date.today().strftime('%Y-%m-%d'))
    ti.xcom_push(key='fechaFile', value=date.today().strftime('%Y%m%d'))


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     schedule_interval=None,
     description=f'{strName}',
     tags=['CWC VM']
     ) as dag:

    jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)
    defDate = PythonOperator(
        task_id='defDate',
        provide_context=True,
        python_callable=defineDate,
        op_kwargs={},
        dag=dag
    )   


    Callme_post_resp =  SSHOperator(
        task_id="Callme_post_responses",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-post-responses_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.dev/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 none',
        ssh_hook = sshHook,
        dag=dag
      )

    jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

    jobStart >> defDate >> Callme_post_resp >> jobEnd