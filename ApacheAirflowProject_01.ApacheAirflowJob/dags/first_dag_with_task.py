from datetime import datetime
from airflow.decorators import dag, task

#================dag decorator====================
@dag(
    dag_id="first_dag",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
)
def first_dag():

    @task.python
    def first_task():
        print("this is the first tasking")

    @task.python
    def second_task():
        print("this is the second task")

    #defining the task dependencies
    first=first_task()
    second=second_task() 

    first >> second


first_dag()