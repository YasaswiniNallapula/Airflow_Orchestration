from pathlib import Path
import yaml
import pendulum
#from airflow import DAG
from airflow.decorators import dag
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'Yasaswini',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

'''dag = DAG(
    dag_id='Assignment_6_dag',
    default_args=default_args,
    description='This dag generates tasks dynamically',
    start_date=datetime(2023,2,8,12)
)'''

DAG_ID = "Assignment_6_dag"

DAG_DIR = Path(__file__).parent
CONFIG_DIR = "configs"

SOURCES_FILE_NAME = "sources.yaml"
SOURCE_CONFIG_FILE_PATH = DAG_DIR / CONFIG_DIR / SOURCES_FILE_NAME
SOURCES = "sources"


@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description='This dag generates tasks dynamically',
    start_date=pendulum.now(tz="Asia/Singapore"),
    #schedule_interval=None,
)
def create_dag():
    Start_Task = DummyOperator(task_id="Start_Task")
    Slack_Start_Notification = DummyOperator(task_id="Slack_Start_Notification")
    ETL_Start_Task = DummyOperator(task_id="ETL_Start_Task")
    ETL_End_Task = DummyOperator(task_id="ETL_End_Task")
    Data_Validation = DummyOperator(task_id="Data_Validation")
    Publish_Start_Task = DummyOperator(task_id="Publish_Start_Task")
    Publish_End_Task = DummyOperator(task_id='Publish_End_Task')
    Email_Success_Notification = DummyOperator(task_id='Email_Success_Notification')
    Slack_Success_Notification = DummyOperator(task_id='Slack_Success_Notification')
    End_Task = DummyOperator(task_id='End_Task')

    Start_Task >> Slack_Start_Notification
    Slack_Start_Notification >> ETL_Start_Task

    source_config_file_path = Path(SOURCE_CONFIG_FILE_PATH)

    sources = []

    if source_config_file_path.exists():
        with open(source_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        sources = sources_config.get(SOURCES, [])

    for source in sources:
        load_to_s3_ = source+"_to_s3_raw"
        cleansed_in_s3_ = source+"_s3_raw_to_s3_cleansed"
        publish_to_s3_ = source+"_s3_cleansed_to_s3_publish"
        load_to_s3 = DummyOperator(task_id=load_to_s3_)
        cleansed_in_s3 = DummyOperator(task_id=cleansed_in_s3_)
        publish_to_s3 = DummyOperator(task_id=publish_to_s3_)

        ETL_Start_Task >> load_to_s3
        load_to_s3.set_downstream(cleansed_in_s3)
        cleansed_in_s3.set_downstream(publish_to_s3)

        '''if source in ['REGIONS', 'LOCATIONS', 'JOBS']:'''
        group_id = source+"_Write_To_PMP"
        with TaskGroup(group_id=group_id) as task_group:
            to_dynamodb_ = source + "_s3_publish_to_dynamodb"
            to_opensearch_ = source + "_s3_publish_to_opensearch"
            to_dynamodb = DummyOperator(task_id=to_dynamodb_)
            to_opensearch = DummyOperator(task_id=to_opensearch_)

            #publish_to_s3 >> task_group
            to_dynamodb
            to_opensearch
        publish_to_s3 >> task_group


        task_group >> ETL_End_Task
    ETL_End_Task >> Data_Validation
    Data_Validation >> Publish_Start_Task

    with TaskGroup(group_id="Publish_To_NSP") as task_group:
        for source in sources:
            task_id = source+"_s3_publish_to_NSP"
            to_nsp = DummyOperator(task_id=task_id)

            # Publish_Start_Task >> task_group
            to_nsp
    Publish_Start_Task >> task_group

    task_group >> Publish_End_Task

    Publish_End_Task >> [Email_Success_Notification, Slack_Success_Notification]
    [Email_Success_Notification, Slack_Success_Notification] >> End_Task


'''else:
    s3_to_pmp_ = source + "_Write_To_PMP"
    s3_to_pmp = DummyOperator(task_id=s3_to_pmp_)

    publish_to_s3.set_downstream(s3_to_pmp)'''


globals()[DAG_ID] = create_dag()

'''task1 = DummyOperator(task_id='start_task', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag)
task5 = DummyOperator(task_id='task5', dag=dag)
task6 = DummyOperator(task_id='end_task', dag=dag)

task1.set_downstream(task2)
task2 >> [task3,task4]
task4 >> task5
[task4,task5] >> task6'''