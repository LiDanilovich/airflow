from datetime import date, datetime, timedelta
from socket import TCP_NODELAY
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils import trigger_rule

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

Dag_Name   = "CWC_feedbacks_Postpaid"
strName    = "CWC Descarga archivos de feedback"

def defineDate(ti):
    fecha = date.today() 
    ti.xcom_push(key='fechaPath', value=fecha.strftime('%Y-%m-%d'))
    ti.xcom_push(key='fechaFile', value=fecha.strftime('%Y%m%d'))


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

    #Callme_post_resp =  SSHOperator(
    #    task_id="Callme_post_responses",
    #    command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Postpaid-callme-post-responses_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 none',
    #command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-post-responses_20221030.csv   s3://export.cwc.dev/bigdata/campaign/cwc/feedback/dt=2022-10-30/ /compartido/librerias/j_landing/wrk/ 0 none',
    #   ssh_hook = sshHook,
    #   dag=dag
    #)

    #Callme_post_rec =  SSHOperator(
    #    task_id="Callme_post_records",
    #    command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Postpaid-callme-post-records_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 none',
    #   ssh_hook = sshHook,
    #   dag=dag
    # )  

    #Callme_prior_resp =  SSHOperator(
    #    task_id="Callme_prior_responses",
    #    command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Postpaid-callme-prior-responses_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 none',
    #   ssh_hook = sshHook,
    #   dag=dag
    # )

    #Callme_prior_rec =  SSHOperator(
    #    task_id="Callme_prior_records",
    #    command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Postpaid-callme-prior-records_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 none',
    #   ssh_hook = sshHook,
    #   dag=dag
    # )  

    Collections_resp =  SSHOperator(
        task_id="Collections_response",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Collections-Pos-response_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
    )

    #aSTGprior = SSHOperator(
    #    task_id=f"aSTGprior_{Dag_Name}",
    #    command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_CALL_PRIOR_RESP_POSTPAID.sas /lla/logs/sas/",
    #    ssh_hook = sshHook,
    #    dag=dag
    #  )

    #aSTGpost = SSHOperator(
    #    task_id=f"aSTGpost_{Dag_Name}",
    #    command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_CALL_POST_RESP_POSTPAID.sas /lla/logs/sas/",
    #   ssh_hook = sshHook,
    #   dag=dag
    # )

    lan_collections = SSHOperator(
        task_id=f"lan_collec_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_CALL_RESPONSES_POSTPAID.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

    stg_collections = SSHOperator(
        task_id=f"stg_collec_{Dag_Name}",
        command="/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_RESPONSES_POSTPAID.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
    )

    aRespHistory = SSHOperator(
        task_id=f"aRespHistory_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_FEEDBACK_POSTPAID.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
    
    jobEnd = DummyOperator(task_id=f'End_{Dag_Name}',  dag=dag)
    jobEnd.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

    jobStart >> defDate 
    #defDate >> Callme_post_resp 
    #defDate >> Callme_post_rec >> jobEnd
    #defDate >> Callme_prior_resp 
    #defDate >> Callme_prior_rec >> jobEnd
    defDate >> Collections_resp 

    #Callme_post_resp >> aSTGpost >> aRespHistory
    #Callme_prior_resp >> aSTGprior >> aRespHistory
    Collections_resp >> lan_collections >> stg_collections >> aRespHistory
    defDate >> jobEnd
    aRespHistory >> jobEnd
    
