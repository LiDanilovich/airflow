import pandas as pd
import s3fs

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator

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

Dag_Name = "Compare_DNA_VM"

class XComEnabledAWSAthenaOperator(AWSAthenaOperator):
    def execute(self, context):
        super(XComEnabledAWSAthenaOperator, self).execute(context)
        # just so that this gets `xcom_push`(ed)
        return self.query_execution_id

def compara(csv1, ti, **context):

    a_csv1 = pd.read_csv(f"{csv1}", sep=',',usecols=[0])
    for i in range(0,1):
        v_csv1 = a_csv1.values
    v_csv1 = v_csv1[0]
    v_csv1 = v_csv1[0]
    ti.xcom_push(key='fechaDNA', value=v_csv1)

    # 
    hook = OracleHook(oracle_conn_id="Oracle")
    a_csv2 = hook.get_first(sql="SELECT to_char(max(dt),'YYYY-MM-DD') FROM J_STAGING.ST_DNA_FIX_2")
    
    v_csv2 = a_csv2[0]
    
    print('el valor en el csv {0} es {1}'.format(csv1,v_csv1))
    print('el valor en stg_2 es {0} '.format(v_csv2))
    
    if v_csv1 > v_csv2 :
        print("actualizar")
        ti.xcom_push(key='newDNA', value='1')
        return 1
        # acá va 1
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
    
    run_queryAthena = XComEnabledAWSAthenaOperator(
        task_id='run_queryAthena',
        query='select max(dt) as fechadna from jamaica.fixedcwcprod',
        output_location='s3://snowflake-awsglue-etl/downloads/fixedcwc/maxdt/',
        database='db-analytics-prod'
    )
    
    move_resultsAthena = S3FileTransformOperator(
        task_id='move_resultsAthena',
        source_s3_key='s3://snowflake-awsglue-etl/downloads/fixedcwc/maxdt/{{ task_instance.xcom_pull(task_ids="run_queryAthena") }}.csv',
        dest_s3_key='s3://cwc-fixed/dna/fechadna.csv',
        transform_script='/bin/cp',
        replace=True
    )
 
    controlNovedades = PythonOperator(
        task_id='controlNovedades',
        provide_context=True,
        python_callable=compara,
        op_kwargs={'csv1':'s3://cwc-fixed/dna/fechadna.csv'              
                  },
        dag=dag
    )    
    
    jobEnd = DummyOperator(task_id=f'End_{Dag_Name}', dag=dag)

    jobStart >> run_queryAthena >> move_resultsAthena >>  controlNovedades >> jobEnd
 
