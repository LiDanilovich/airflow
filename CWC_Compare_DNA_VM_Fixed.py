import pandas as pd
import s3fs

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
'start_date': datetime(year=2022, month=8, day=27),
'email': ['sample@sample.us'],      # email list notificaction 
'email_on_failure': False,          # set true send notificaction 
'email_on_retry': False,            # set true send notificaction 
'retries': 0,                       # number retries 0 => 1 retries
'retry_delay': timedelta(minutes=10) # The time pause to back retries
}

Dag_Name = "CWC_Compare_DNA_VM_Fixed"
fecha_actual = str(date.today())
sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id

def compara( ti, **context):
    
    #  comparo fecha de J_LANDING.LA_DNA_FIX_BASE_SNAP y J_STAGING.ST_DNA_FIX_2

    hook = OracleHook(oracle_conn_id="Oracle")
    a_lan = hook.get_first(sql="SELECT to_char(max(dt),'YYYY-MM-DD') FROM J_LANDING.LA_DNA_FIX_BASE_SNAP")
    if a_lan[0] is None:
       v_lan = '0000-00-00'
    else:
       v_lan = a_lan[0]

    a_stg = hook.get_first(sql="SELECT to_char(max(dt),'YYYY-MM-DD') FROM J_STAGING.ST_DNA_FIX_2")
    if a_stg[0] is None:
       v_stg = '0000-00-00'
    else:
       v_stg = a_stg[0]

    
    print('el valor en landing es {0}'.format(v_lan))
    print('el valor en stg_2 es {0} '.format(v_stg))

    # valor esperado
    fecha = date.today()-timedelta(days=2) 
    print('la fecha esperada es {}'.format(fecha.strftime('%Y-%m-%d')))

    
    if v_lan > v_stg :
        print("actualizar")
        ti.xcom_push(key='newDNA', value='1')
        return 1
        # acá va 1
    elif v_stg == fecha.strftime('%Y-%m-%d') :
        print('ya estaba actualizado')
        ti.xcom_push(key='newDNA', value='2')
        return 2 
    else: 
        print("no news")    
        ti.xcom_push(key='newDNA', value='0')
        return 0
        # acá va 0
        


with DAG(f'{Dag_Name}',
         schedule_interval=None,
         start_date=datetime(2022, 8, 29),
         tags=['CWC VM']
         ) as dag:

    jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)
    # descarga DNA
    busca_dna_fix = XComEnabledAWSAthenaOperator(
        task_id='busca_dna_fix',
        query='select * from jamaica.fixedcwcprod',
        output_location='s3://snowflake-awsglue-etl/downloads/fixedcwc/',
        database='db-analytics-prod'
    )

    transform_dna_fix = S3FileTransformOperator(
        task_id='transform_dna_fix',
        source_s3_key='s3://snowflake-awsglue-etl/downloads/fixedcwc/{{ task_instance.xcom_pull(task_ids="busca_dna_fix") }}.csv',
        dest_s3_key=f's3://cwc-fixed/dna/fixedcwc_{fecha_actual}.csv',
        transform_script='/bin/cp',
        replace = True
    )

    # incorpora datos a oracle si son nuevos
    landing_dna_fix =  SSHOperator(
        task_id="landing_dna_fix",
        command= f'/bin/bash /lla/dags/bash_codes/move_dna.sh  fixedcwc_{fecha_actual}.csv  s3://cwc-fixed/dna/  /compartido/librerias/j_landing/wrk/ 1 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfixed/ ',
        ssh_hook = sshHook,
        dag=dag
    )
     

    dna = SSHOperator(
        task_id=f"dna_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_DNA_FIX_BASE_SNAP_Air.sas /lla/logs/sas/",
        ssh_hook = sshHook,
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
 
    jobStart >> busca_dna_fix >> transform_dna_fix >> landing_dna_fix >> dna >> controlNovedades 
    controlNovedades >> sleep_task >> jobEnd
 
    
 
