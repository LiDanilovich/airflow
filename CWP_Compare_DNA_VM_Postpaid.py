
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from SshStrataOperator import SshStrataOperator


"""
   comparamos fecha del dna disponibilizado con la fecha de la última corrida
   acciones a seguir:
   - si hay disponible un nuevo dna, lanza los jobs sas (staging) 
      y setea variable newDNA = 1
   - si ejecutan los jobs sas (staging) correctamente newDNA = 2    
   - si coinciden las fechas y ya tengo ultimo dna ?
   - si coinciden las fechas y no tengo último dna (no publicaron uno nuevo)
     newDNA = 0 
  
"""
default_args = {
'owner': 'Lili Danilovich',                 # name de autor dag contact for support 
'depends_on_past': False,
'start_date': datetime(year=2022, month=12, day=26),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}

Dag_Name = "CWP_Compare_DNA_VM_Postpaid"
sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

def compara(ti, **context):
    
    hook = OracleHook(oracle_conn_id="Oracle")
  
    a_csv1 = hook.get_first(sql="SELECT to_char(max(snap_key)) FROM LANDING.LA_DNA_POSTPAID_BASE_SNAP")
    if a_csv1[0] is None:
       v_csv1 = '0000-00-00'
    else:
       v_csv1 = a_csv1[0]
    
    
    a_csv2 = hook.get_first(sql="SELECT to_char(max(data_dt),'YYYYMMDD') FROM STAGING.VM_SUSCRIBER_CWP WHERE SUSC_MKT='POSTPAID' ")
    
    if a_csv2[0] is None:
       v_csv2 = '0000-00-00'
    else:
       v_csv2 = a_csv2[0]
    
    print('el valor en datalake es {0}'.format(v_csv1))
    print('el valor en vm_suscriber_cwp es {0} '.format(v_csv2))

    # valor esperado
    fecha = date.today()-timedelta(days=1) 
    print('la fecha esperada es {}'.format(fecha.strftime('%Y-%m-%d')))

    
    if v_csv1 > v_csv2 :
        print("actualizar")
        ti.xcom_push(key='newDNA', value='1')
        return 1
    elif v_csv2 == fecha.strftime('%Y-%m-%d') :
        print('ya estaba actualizado')
        ti.xcom_push(key='newDNA', value='2')
        return 2 
    else: 
        print("no news")    
        ti.xcom_push(key='newDNA', value='0')
        return 0
        


with DAG(f'{Dag_Name}',
         schedule_interval=None,
         start_date=datetime(2022, 8, 29),
         tags=['CWP VM']
         ) as dag:

    jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)
    
    lan = SshStrataOperator(
        task_id=f"lan_{Dag_Name}",
        job_name=f"lan_{Dag_Name}",
        exec_command = f"/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_LA_DNA_POSTPAID_BASE_SNAP_vAir.sas /lla/logs/sas/",
        retorna_valor_int = False,
        ssh_conn_id ='ssh_sasuser_compute',
        dag=dag
    )
 
    controlNovedades = PythonOperator(
        task_id='controlNovedades',
        provide_context=True,
        python_callable=compara,
        op_kwargs={          
                  },
        dag=dag
    )    
    
    sleep_task = BashOperator(
        task_id='sleep_task',
        bash_command='sleep 60',
        dag=dag,
    )


    jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

    jobStart >> lan >>  controlNovedades >> sleep_task >> jobEnd
 
