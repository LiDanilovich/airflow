


from datetime import datetime, timedelta
from airflow import DAG


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


Dag_Name = "CWP_VM_Prepaid_Resto"
strName = "Panama - VM Prepaid Calculadas"


with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWP VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
      from airflow.contrib.operators.ssh_operator import SSHOperator
      from airflow.providers.ssh.hooks.ssh import SSHHook
      from airflow.operators.trigger_dagrun import TriggerDagRunOperator

      sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

      noRech = SSHOperator(
        task_id=f"noRech_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_LA_CARGA_NORECH4W_vAir.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )      
      
      topUp = SSHOperator(
        task_id=f"topUp_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_LA_CARGA_TOPUP_vAir.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )  
      
      
      rtngPlanPre = SSHOperator(
        task_id=f"rtng_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_LA_PROD_RTNG_PLN_PRE.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )  


      trigger_VM_Eventos = TriggerDagRunOperator(
        task_id='trigger_VM_Eventos',
        trigger_dag_id = 'CWP_VM_Eventos',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )

      trigger_SRE_Offers = TriggerDagRunOperator(
        task_id='trigger_CWP_SRE_Recomendador',
        trigger_dag_id = 'CWP_SRE_Recomendador',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval=30 
      )

      stg2 = SSHOperator(
        task_id=f"stg2_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_PRE_SUSCRIBER_PREPAID_v1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )


      dm = SSHOperator(
        task_id=f"dm_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_SUBSCRIBER_PREPAID_v1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      dm_hst = SSHOperator(
        task_id=f"dm1_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_DM_SUBSCRIBER_PREP_STS_HST.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
      
      
      
      vm_prepaid = SSHOperator(
        task_id='vm_prepaid',
        command= "/bin/bash /home/sasdemo/./vmcwp_prepaid.sh ",
        ssh_hook = sshHook,
        dag=dag
      )  
      
    
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> noRech >> stg2
      jobStart >> topUp >> stg2
      jobStart >> rtngPlanPre >> stg2
      jobStart >> trigger_VM_Eventos >> stg2
      jobStart >> trigger_SRE_Offers >> jobEnd
      stg2 >> dm >> dm_hst  >> vm_prepaid >> jobEnd
     

