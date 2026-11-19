from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    dag_id="third_dag_xcom_kwargs",
    description="XCom basics: one task returns a dict, the next consumes it using kwags xcom",
    start_date=datetime(2026, 11, 19),
    schedule=None,
    catchup=False,
    tags=["learning", "xcom"],
)
def second_dag_xcom():

    @task.python
    def first_task(**kwargs):
        """Pushes its return value to XCom under the key 'return_value'."""
        print("first task: calling insert_gl_interface api, returning the response")

        ti = kwargs['ti'] # ti[task instance] is inbuild  variable inside the kwargs in airflow
        api_response = {
            "Total_rows_inserted": 500,
            "Batch_id": "500xyz",
        }
        
        ti.xcom_push(key='return_result', value=api_response)

    @task.python
    def second_task(**kwargs):
        """Receives first_task's return value - no xcom_pull needed."""
        print("second task: reading the batch id from the previous task")

        ti = kwargs['ti'] # ti[task instance] is inbuild  variable inside the kwargs in airflow
        
        data = ti.xcom_pull(task_ids='first_task', key='return_result')
        batch_id = data["Batch_id"]
        rows = data["Total_rows_inserted"]

        print(f"The batch id fetched from the first task is: {batch_id}")
        print(f"Rows reported inserted: {rows}")

    first  = first_task()
    second = second_task()

    first >> second


second_dag_xcom()
