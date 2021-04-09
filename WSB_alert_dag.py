# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 14:02:41 2020
@author: tcai
"""

"""
S3 Sensor Connection Test
"""
import airflow
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from datetime import datetime, timedelta
from airflow.utils.email import send_email_smtp

def task_success_callback(context):
    outer_task_success_callback(context, email= ['tcai@migcap.com', 'yjeon@migcap.com'])


def outer_task_success_callback(context, email):
    subject = "[Airflow] DAG {0} - Task {1}: Success".format(
	context['task_instance_key_str'].split('__')[0],
	context['task_instance_key_str'].split('__')[1]
    )
    html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Succeeded on: {2}
    """.format(
	context['task_instance_key_str'].split('__')[0],
	context['task_instance_key_str'].split('__')[1],
	datetime.now()
    )
    send_email_smtp(email, subject, html_content)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 28),
     'end_date' : None,
    'email': ['tcai@migcap.com', 'yjeon@migcap.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_success_callback': task_success_callback,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


WSB_alert_dag = DAG('WSB_alert_dag', default_args=default_args, schedule_interval= '*/5 * * * *')


reddit_WSB_sh = BashOperator(
    task_id='WSB_alert',
    bash_command="python3 /home/ec2-user/GET_Webhose/wstbet_email_alert.py ",
    queue="pipeline2",
    dag=WSB_alert_dag)

