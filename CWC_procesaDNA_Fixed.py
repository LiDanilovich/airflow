import s3fs
import airflow 
import boto3
import pandas as pd
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta
from airflow import DAG
from urllib.parse import urlparse

#from airflow.contrib.operators.python_operator import PythonOperator

sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")
# a hook can also be defined directly in the code:
# sshHook = SSHHook(remote_host='server.com', username='admin', key_file='/opt/airflow/keys/ssh.key')

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


Dag_Name = "CWC_procesaDNA_Fixed"
strName = "Caribe - DNA Fixed"

class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id

def descarga_dna(myUrl,myKey,myPrefix,myPathLocal):
    # ejemplo para buscar en s3://fixed-cwc/dna/fixedcwc.csv'
    # myUrl = 's3://fixed-cwc/dna/"
    # myKey = 'dna/fixedcwc.csv'
    # myPrefix = 'dna'
    #s3_client = boto3.client('s3',region_name='us-east-1')
    #s3_client.download_file('cwc-fixed', 'dna/fixedcwc.csv', '/lla/dags/sas_codes/fixedcwc.csv')
    bucket = 'cwc-fixed'
    path = ''
    file_name = ''

    s3_session = boto3.Session()
    s3_resource = s3_session.resource('s3')

    #mnt_loc = '/compartido/librerias/j_landing/'
    s3_loc = urlparse(myUrl, allow_fragments=False)
    s3_files = []
    bucket = s3_resource.Bucket(s3_loc.netloc)
    for elem in bucket.objects.filter(Prefix=myPrefix):
        if s3_files.append(elem.key) == myKey:
           elem =  s3_files.append(elem.key)



    bucket = str(elem.bucket_name)
    path = str(elem.key)
    file_name = str(elem.key.rsplit('/', 1)[-1])
    print(bucket,path,file_name)
    print(myPathLocal+file_name)
    s3_resource.Bucket(bucket).download_file(path, myPathLocal + file_name)


def copyAirflowOBJ(source_bucket_name, source_bucket_key, dest_bucket_name, dest_bucket_key):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import logging
    import os
    logging.basicConfig(level=logging.INFO)

    hookS3 = S3Hook("aws_default") 
    
    logging.info('get_bucket: %s', hookS3.get_bucket(bucket_name=source_bucket_name))
    logging.info('check_for_bucket: %s', hookS3.check_for_bucket(bucket_name=source_bucket_name))

    hookS3.copy_object(
        source_bucket_key   = source_bucket_key,
        dest_bucket_key     = dest_bucket_key,
        source_bucket_name  = source_bucket_name,
        dest_bucket_name    = dest_bucket_name
    )

def obtenerFecha(csv1, ti, **context):
    a_csv1 = pd.read_csv(f"{csv1}", sep=',',usecols=[0])
    for i in range(0,1):
        v_csv1 = a_csv1.values
    v_csv1 = v_csv1[0]
    v_csv1 = v_csv1[0]
    ti.xcom_push(key='fechaDNA', value=v_csv1)

with DAG(f'{Dag_Name}',
     default_args=default_args,
     catchup=False, 
     tags=['CWC VM'],
     schedule_interval=None,
     description=f'{strName}'
     ) as dag:

      from airflow.operators.dummy import DummyOperator
  
      jobStart = DummyOperator(task_id=f'Start_{Dag_Name}', dag=dag)

      fecha_dna_fix = PythonOperator(
        task_id='fecha_dna_fix',
        provide_context=True,
        python_callable=obtenerFecha,
        op_kwargs={'csv1':'s3://cwc-fixed/dna/fechadna.csv'              
                  },
        dag=dag
      )    

      busca_dna_fix = XComEnabledAWSAthenaOperator(
        task_id='busca_dna_fix',
        query='select * from jamaica.fixedcwcprod',
        output_location='s3://snowflake-awsglue-etl/downloads/fixedcwc/',
        database='db-analytics-prod'
      )

      transform_dna_fix = S3FileTransformOperator(
        task_id='transform_dna_fix',
        source_s3_key='s3://snowflake-awsglue-etl/downloads/fixedcwc/{{ task_instance.xcom_pull(task_ids="busca_dna_fix") }}.csv',
        dest_s3_key='s3://cwc-fixed/dna/fixedcwc_{{ task_instance.xcom_pull(task_ids="fecha_dna_fix",key="fechaDNA") }}.csv',
        transform_script='/bin/cp',
        replace = True
      )
      
      landing_dna_fix =  SSHOperator(
        task_id="landing_dna_fix",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  fixedcwc_{{ task_instance.xcom_pull(task_ids="fecha_dna_fix",key="fechaDNA") }}.csv  s3://cwc-fixed/dna/  /compartido/librerias/j_landing/wrk/ 1 s3://cwp-backupbd/backups_jamaica/fuentes/cwcfixed/ ',
        ssh_hook = sshHook,
        dag=dag
      )
     
      dna_fix = SSHOperator(
        task_id='dna_proceso_fix',
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_DNA_FIX_BASE_SNAP.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      dnaContingencia = SSHOperator(
        task_id=f"dnaCont_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_DNA_FIX_CONTINGENCIA.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )
      
       
      stg = SSHOperator(
        task_id=f"stg_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      stg1 = SSHOperator(
        task_id=f"stg1_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_1.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      stg2 = SSHOperator(
        task_id=f"stg2_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_2.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      stg3 = SSHOperator(
        task_id=f"stg3_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_POSTPAID_A_FIX.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      
      churn = SSHOperator(
        task_id=f"churn_{Dag_Name}",
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_ST_DNA_FIX_CHRN.sas /lla/logs/sas/",
        ssh_hook = sshHook,
        dag=dag
      )

      #trigger_DNA_Postpaid = TriggerDagRunOperator(
      #  task_id='trigger_DNA_Postpaid',
      #  trigger_dag_id = 'CWC_Compare_DNA_VM_Postpaid',
      #  #execution_date = '{{ ds }}',
      #  reset_dag_run = True,
      #  wait_for_completion = True,
      #  poke_interval=30 
      #)
            
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> fecha_dna_fix >> busca_dna_fix >> transform_dna_fix >> landing_dna_fix
      landing_dna_fix >> dna_fix >> dnaContingencia >> stg >> stg1 >> stg2 >> stg3  >> jobEnd
      dna_fix >> churn >> jobEnd
     

