# Airflow_Orchestration

## Task 1
● Aiflow docker image is downloaded.  
● Setup volumes to local folder in docker compose:  
  volumes:  
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags  
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs  
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins  
● Changed it to::  
  volumes:  
    - User_pathXXXX/dags:/opt/airflow/dags  
    - User_pathXXXX/logs:/opt/airflow/logs  
    - User_pathXXXX/plugins:/opt/airflow/plugins  

● Then in windows command prompt by using below docker commands airflow container starts running  
docker-compose up airflow-init   
docker-compose up -d  




## Task 2
●	Using only DummyOperator, implemented the dag structure
●	In that DAG, table names are COUNTRIES, DEPARTMENTS, EMPLOYEES, JOBS, LOCATIONS, REGIONS. All these table names must be defined in a config yaml file and dag code should iterate over those tables and create the tasks dynamically
● TaskGroup concept is also introduced in this concept

## Task 3
●	a variable “environment” is created in Admin->Variables in the Airflow 
●	This variable can have any of these values: dev | qa | prod
●	Create a dag with the below steps
  ○	Start_Task - DummyOpertor
  ○	Get_Env - BranchPythonOperator. Get above created “environment” variable value
  ○	Based on the value, a specific branch of the dag must be executed
  ○	Create ShortCircuitOperator with task name “Validate_<DEV|QA|PROD>_Branch” in each branch and this task will verify the “environment” variable is as per the branch and prints the “environment” variable
  ○	Create DummyOperator “Deploy_and_Release_<DEV|QA|PROD>” in each branch
  ○	Join task -  set a TriggerRule for this task to run.
  ○	End_Task - DummyOpertor. 

## Task 4
●	Create a dag
 ○	Start and end task
 ○	Process_Data ShortCircuitOperator task
   ■	Read data from file “countries.json”
   ■	Aggregate the population numbers to continent level. In simple terms, it is sum of population of all countries within each continent
   ■	Example:{“europe”: 12000, “asia”: 14000, “america”: 1200}
   ■	At the end, it should be a dictionary with keys as continent names and values as sum of population of all countries within that continent
   ■	For this xcom push of the above created dictionary is done
 ○	Based on the number of continents, create parallel shortcircuit tasks (one for each continent)
   ■	Example task name - Population_Of_Europe
 ○	Within this individual continent task, do a xcom pull and get the population count of its respective continent
 ○	Print the population count

