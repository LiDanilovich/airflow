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

Dag_Name = "CWP_VM_Prepaid_Backups"
strName = "Panama - Backups Prepaid"

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
     

     bkp_dna = SSHOperator(
        task_id=f"bkp_dna_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/jb_BK_DNA_PREPAID_BASE_SNAP.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )    

     bkp_rtng = SSHOperator(
        task_id=f"rtng_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_BK_PROD_RTNG_PLN_PRE.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )    
     
     bkp_low = SSHOperator(
        task_id=f"low_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_RT_LOWBALANCE_BIS.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )    

     bkp_topup = SSHOperator(
        task_id=f"topup_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_RT_ADJUSTMENT_BIS.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )    



     bkp_adj = SSHOperator(
        task_id=f"adj_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_RT_TOPUP_BIS.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )    
     
     jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

     jobStart >> bkp_dna >> jobEnd 
     jobStart >> bkp_rtng >> jobEnd 
     jobStart >> bkp_low >> jobEnd 
     jobStart >> bkp_topup >> jobEnd 
     jobStart >> bkp_adj >> jobEnd 
