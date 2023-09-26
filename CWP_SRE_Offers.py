from airflow import DAG
from airflow.utils import trigger_rule
from datetime import datetime, timedelta, date

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "liliana.danilovich@strata-analytics.us",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("CWP_SRE_Offers_Sensor", 
         start_date=datetime(2023, 2 ,2), 
         schedule_interval="0 15 * * *", 
         default_args=default_args, 
         tags=['CWP VM','SENSORS'],
         catchup=False) as dag:

        from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
        from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator

        import boto3
        from airflow.providers.ssh.hooks.ssh import SSHHook
        from airflow.contrib.operators.ssh_operator import SSHOperator
        from airflow.operators.dummy import DummyOperator

        sshHook = SSHHook(ssh_conn_id="ssh_sasuser_compute")

        jobStart = DummyOperator(task_id='Start', dag=dag)

        s3_bucketname = 'sre-cwp-prepaid-dev-serving'
        s3_loc = 'batch_inference/sre_cwp_pre_offers.csv'
        s3_bkp = 'batch_inference/backup/'
        fecha = date.today()

        s3_sensor = S3KeySensor(
           task_id='s3_check_if_file_present',
           poke_interval=300,
           timeout=5460,
           soft_fail=False,
           retries=0,
           bucket_key=s3_loc,
           bucket_name=s3_bucketname,
           aws_conn_id='aws_default',
           mode='reschedule',
           dag=dag
        )


        landing_sre =  SSHOperator(
            task_id="landing_sre",
            command= '/bin/bash /lla/dags/bash_codes/downFromS3.sh sre_cwp_pre_offers.csv  s3://sre-cwp-prepaid-dev-serving/batch_inference/ /compartido/librerias/landing/wrk/ ',
            ssh_hook = sshHook,
            dag=dag
        )


        stg_sre = SSHOperator(
            task_id="stg_sre",
            command= "/bin/bash /lla/dags/bash_codes/ejecuta_sas.sh /saswork /sasconfig/Lev1/SASApp/SASEnvironment/SASCode/Jobs/JB_ST_CWP_PRE_OFFERS.sas /lla/logs/sas/",
            ssh_hook = sshHook,
            dag=dag
        )

        s3_bkp = S3CopyObjectOperator(
           task_id="bkp_object",
           source_bucket_name=s3_bucketname,
           dest_bucket_name=s3_bucketname,
           source_bucket_key=s3_loc,
           dest_bucket_key=s3_bkp+'sre_cwp_pre_offers_'+fecha.strftime('%Y%m%d')+'.csv',
           dag=dag
        )


        s3_delete = S3DeleteObjectsOperator(
            task_id="delete_object",
            bucket=s3_bucketname,
            keys=s3_loc,
            dag=dag
        )

        
        jobEnd = DummyOperator(task_id='End', dag=dag)
        jobEnd.trigger_rule = trigger_rule.TriggerRule.ALL_DONE
        jobStart >> s3_sensor >> landing_sre >> stg_sre >> s3_bkp >> s3_delete >> jobEnd