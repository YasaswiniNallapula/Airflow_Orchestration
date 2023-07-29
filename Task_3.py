from airflow import DAG
from datetime import datetime,timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator

default_args = {
    'owner': 'Yasaswini',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='Assignment_7_dag',
    default_args=default_args,
    description='This dag generates airflow branching',
    start_date=datetime(2023,2,8,12)
)

def Get_Env(ti):
    #my_var = Variable.set("my_key", "my_value")
    env_var = Variable.get("environment")
    ti.xcom_push(key='env_var', value=env_var)
    if env_var.lower() == 'dev':
        return 'Validate_DEV_Branch'
    elif env_var.lower() == 'qa':
        return 'Validate_QA_Branch'
    else:
        return 'Validate_PROD_Branch'

def Validate_DEV_Branch(ti):
    env_var = ti.xcom_pull(task_ids='Get_Env', key='env_var')
    print('environment from given variable is: ', env_var)
    if env_var.lower() == 'dev':
        return True
    else:
        return False

def Validate_QA_Branch(ti):
    env_var = ti.xcom_pull(task_ids='Get_Env', key='env_var')
    print('environment from given variable is: ', env_var)
    if env_var.lower() == 'qa':
        return True
    else:
        return False

def Validate_PROD_Branch(ti):
    env_var = ti.xcom_pull(task_ids='Get_Env', key='env_var')
    print('environment from given variable is: ', env_var)
    if env_var.lower() == 'prod':
        return True
    else:
        return False


Start_Task = DummyOperator(
    task_id='Start_Task',
    dag=dag
)
Get_Env = BranchPythonOperator(
    dag=dag,
    task_id='Get_Env',
    python_callable=Get_Env
)

Validate_DEV_Branch_Task = ShortCircuitOperator(
    task_id='Validate_DEV_Branch',
    dag=dag,
    provide_context=True,
    python_callable=Validate_DEV_Branch
)
Validate_QA_Branch_Task = ShortCircuitOperator(
    task_id='Validate_QA_Branch',
    dag=dag,
    provide_context=True,
    python_callable=Validate_QA_Branch
)
Validate_PROD_Branch_Task = ShortCircuitOperator(
    task_id='Validate_PROD_Branch',
    dag=dag,
    provide_context=True,
    python_callable=Validate_PROD_Branch
)

Deploy_and_Release_DEV = DummyOperator(
    task_id='Deploy_and_Release_DEV',
    dag=dag
)
Deploy_and_Release_QA = DummyOperator(
    task_id='Deploy_and_Release_QA',
    dag=dag
)
Deploy_and_Release_PROD = DummyOperator(
    task_id='Deploy_and_Release_PROD',
    dag=dag
)

Join_Task = DummyOperator(
    task_id='Join_Task',
    trigger_rule='one_success',
    dag=dag
)

End_Task = DummyOperator(
    task_id='End_Task',
    dag=dag
)

Start_Task.set_downstream(Get_Env)
Get_Env >> [Validate_DEV_Branch_Task, Validate_QA_Branch_Task, Validate_PROD_Branch_Task]
Validate_DEV_Branch_Task >> Deploy_and_Release_DEV
Validate_QA_Branch_Task >> Deploy_and_Release_QA
Validate_PROD_Branch_Task >> Deploy_and_Release_PROD
[Deploy_and_Release_DEV, Deploy_and_Release_QA, Deploy_and_Release_PROD] >> Join_Task
Join_Task.set_downstream(End_Task)
