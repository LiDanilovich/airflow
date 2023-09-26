
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


Dag_Name   = "CWC_feedbacks_Fixed"
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
    
    prepareTable = SSHOperator(
        task_id="prepareTable",
        command= '/bin/bash /lla/dags/bash_codes/truncate_st_feedback.sh fixed',
        ssh_hook = sshHook,
        dag=dag
    )

    Callme_post_resp =  SSHOperator(
        task_id="Callme_post_responses",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-post-responses_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        #command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-post-responses_20221030.csv   s3://export.cwc.dev/bigdata/campaign/cwc/feedback/dt=2022-10-30/ /compartido/librerias/j_landing/wrk/ 0 none',
        ssh_hook = sshHook,
        dag=dag
      )

    Callme_post_rec =  SSHOperator(
        task_id="Callme_post_records",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-post-records_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
      )  

    Callme_prior_resp =  SSHOperator(
        task_id="Callme_prior_responses",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-prior-responses_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
      )

    Callme_prior_rec =  SSHOperator(
        task_id="Callme_prior_records",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Callme-prior-records_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
      )  

    Collections_resp =  SSHOperator(
        task_id="Collections_response",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Collections-response_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://export.cwc.prod/bigdata/campaign/cwc/feedback/dt={{ task_instance.xcom_pull(task_ids="defDate",key="fechaPath") }}/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
      )

    Onboarding_resp =  SSHOperator(
        task_id="Onboarding_response",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  Onboarding-response_{{ task_instance.xcom_pull(task_ids="defDate",key="fechaFile") }}.csv   s3://source.cwc.prod/cwc/campaign/feedback/ /compartido/librerias/j_landing/wrk/ 0 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfeedback/',
        ssh_hook = sshHook,
        dag=dag
      )






    lan_prior = SSHOperator(
        task_id=f"lan_prior_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_CALL_PRIOR_RESPONSES_Air.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

    lan_pos = SSHOperator(
        task_id=f"lan_post_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_CALL_POST_RESPONSES_Air.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )


    lan_collec = SSHOperator(
        task_id=f"lan_collec_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_COLLECTION_RESPONSES_Air.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

    lan_onboarding = SSHOperator(
        task_id=f"lan_onboarding_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_ONBOARDING_RESPONSES_Air.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )




    actualizaSTG = SSHOperator(
        task_id=f"actualizaSTG_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_RESPONSES.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

    aRespHistory = SSHOperator(
        task_id=f"aRespHistory_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_FEEDBACK_A_RH.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      ) 

     
    jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)
    jobEnd.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

    jobStart >> defDate 
    defDate >> Callme_post_resp >>  prepareTable

    defDate >> Callme_post_rec >> jobEnd
    defDate >> Callme_prior_resp >> prepareTable
    defDate >> Callme_prior_rec >> jobEnd


    defDate >> Collections_resp >> Onboarding_resp >> prepareTable
    prepareTable >> lan_pos >> lan_prior >> lan_collec >> lan_onboarding >> actualizaSTG >> aRespHistory >> jobEnd
    
