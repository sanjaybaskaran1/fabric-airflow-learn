from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

#================dag decorator====================
@dag(dag_id='first_dag') #name of the dag
def first_dag():

    @task.python
    def first_task():
        print("this is the first task")

    @task.python
    def second_task():
        print("this is the second task")

    #defining the task dependencies
    first=first_task()
    second=second_task() 

    first >> second


first_dag()