from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator, PythonOperator
import json
from pathlib import Path
import yaml

default_args = {
    'owner': 'Yasaswini',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

DAG_ID = "Assignment_8_yashu_dag"
DAG_DIR = Path(__file__).parent
file_name = "countries.json"
out_file = DAG_DIR / 'yash.yaml'

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    description='This dag uses xcoms',
    start_date=datetime(2023,2,8,12)
    #schedule_interval=None,
)
def create_dag():
    Start_Task = DummyOperator(task_id="Start_Task")
    End_Task = DummyOperator(task_id='End_Task')


    def process_data(ti):
        try:
            file = DAG_DIR / file_name
            continent_poptn = {}
            continent_list = []
            with open(file, 'r') as file_obj:
                json_data = json.load(file_obj)
            data = json_data['countries']['country']
            for row in data:
                continentname = row["continentName"]
                continent_poptn[continentname] = continent_poptn.get(continentname, 0) + int(row["population"])
            for key,value in continent_poptn.items():
                ti.xcom_push(key=key, value=value)
                continent_list.append(key)
            #continents ={i for i in continent_poptn.keys()}
            #print(continents)

            with open(out_file, 'w') as f:
                yaml.dump(continent_list, f)
            return True
        except Exception as e:
            print(e.args)
            return False

    def population_count(continent_name,ti):
        try:
            pop = ti.xcom_pull(task_ids='Process_data',key=continent_name)
            print(f"population of {continent_name} is: {pop}")
            return True
        except Exception as e:
            print(e.args)
            return False

    Process_Data_Task = ShortCircuitOperator(
        task_id='Process_data',
        provide_context=True,
        python_callable=process_data,
        #do_xcom_push=True
    )


    out_config_file_path = Path(out_file)

    if out_config_file_path.exists():
        with open(out_config_file_path, "r") as config_file:
            continents = yaml.safe_load(config_file)
            #continents = continent_config.get('continents', [])

    for continent in continents:
        pop_of_continent = "Population_Of_"+continent
        pop_of_continent_task = ShortCircuitOperator(
            task_id=pop_of_continent.replace(" ", "_"),
            provide_context=True,
            python_callable=population_count,
            op_kwargs={'continent_name':continent}
        )

        Process_Data_Task >> pop_of_continent_task
        pop_of_continent_task >> End_Task

    Start_Task >> Process_Data_Task



globals()[DAG_ID] = create_dag()

