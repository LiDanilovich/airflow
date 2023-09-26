import s3fs
import airflow 
import boto3
import pandas as pd 
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from airflow import DAG
from urllib.parse import urlparse


sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")


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


Dag_Name = "CWC_descargaDNA_Postpaid"
strName = "Caribe - DNA Postpaid"

class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id

def descarga_dna():
    # ejemplo para buscar en s3://fixed-cwc/dna/fixedcwc.csv'
    # myUrl = 's3://fixed-cwc/dna/"
    # myKey = 'dna/fixedcwc.csv'
    # myPrefix = 'dna'
    #s3_client = boto3.client('s3',region_name='us-east-1')
    #s3_client.download_file('cwc-fixed', 'dna/fixedcwc.csv', '/lla/dags/sas_codes/fixedcwc.csv')
    bucket = 'cwc-postpaid'
    path = ''
    file_name = ''

    s3_session = boto3.Session()
    s3_resource = s3_session.resource('s3')

    #mnt_loc = '/compartido/librerias/j_landing/'
    s3_loc = urlparse('s3://cwc-postpaid/dna/', allow_fragments=False)
    s3_files = []
    bucket = s3_resource.Bucket(s3_loc.netloc)
    for elem in bucket.objects.filter(Prefix='dna'):
        if s3_files.append(elem.key) == 'dna/postpaidcwc.csv':
           elem =  s3_files.append(elem.key)



    bucket = str(elem.bucket_name)
    path = str(elem.key)
    file_name = str(elem.key.rsplit('/', 1)[-1])
    print(bucket,path,file_name)
    #s3_resource.Bucket(bucket).download_file(path, '/compartido/librerias/j_landing/' + file_name)


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

      fecha_dna = PythonOperator(
        task_id = 'fecha_dna',
        provide_context = True, 
        python_callable=obtenerFecha,
        op_kwargs={'csv1':'s3://cwc-postpaid/dna/fechadna.csv'              
                  },
        dag=dag

      )      

      busca_dna = XComEnabledAWSAthenaOperator(
        task_id='busca_dna',
        query='select * from jamaica.postpaidcwc',
        output_location='s3://snowflake-awsglue-etl/downloads/postpaidcwc/',
        database='db-analytics-prod'
      )

      transform_dna = S3FileTransformOperator(
        task_id='transform_dna',
        source_s3_key='s3://snowflake-awsglue-etl/downloads/postpaidcwc/{{ task_instance.xcom_pull(task_ids="busca_dna") }}.csv',
        dest_s3_key='s3://cwc-postpaid/dna/postpaidcwc_{{ task_instance.xcom_pull(task_ids="fecha_dna",key="fechaDNA") }}.csv',
        transform_script='/bin/cp',
        replace = True
      )
      
    
      landing_dna =  SSHOperator(
        task_id="landing_dna_fix",
        command= '/bin/bash /lla/dags/bash_codes/move_dna.sh  postpaidcwc_{{ task_instance.xcom_pull(task_ids="fecha_dna",key="fechaDNA") }}.csv  s3://cwc-postpaid/dna/  /compartido/librerias/j_landing/wrk/ 1 s3://cwp-backupbd/backups_jamaica/fuentes/cwcpostpaid/ ',
        ssh_hook = sshHook,
        dag=dag
      )


      dna_proceso = SSHOperator(
        task_id='dna_proceso',
        command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_J_LA_DNA_POSTPAID_BASE_SNAP.sas",
        ssh_hook = sshHook,
        dag=dag
      )

     
      jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

      jobStart >> fecha_dna >> busca_dna >> transform_dna >> landing_dna 
      landing_dna >> dna_proceso >> jobEnd


