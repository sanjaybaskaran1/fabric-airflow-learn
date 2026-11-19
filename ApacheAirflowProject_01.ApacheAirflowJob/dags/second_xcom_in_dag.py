from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="second_dag_xcom",
    description="XCom basics: one task returns a dict, the next consumes it",
    start_date=datetime(2026, 11, 19),
    schedule=None,
    catchup=False,
    tags=["learning", "xcom"],
)
def second_dag_xcom():

    @task.python
    def first_task():
        """Pushes its return value to XCom under the key 'return_value'."""
        print("first task: calling insert_gl_interface api, returning the response")

        api_response = {
            "Total_rows_inserted": 500,
            "Batch_id": "500xyz",
        }
        return api_response

    @task.python
    def second_task(data: dict):
        """Receives first_task's return value - no xcom_pull needed."""
        print("second task: reading the batch id from the previous task")

        batch_id = data["Batch_id"]
        rows = data["Total_rows_inserted"]

        print(f"The batch id fetched from the first task is: {batch_id}")
        print(f"Rows reported inserted: {rows}")

    first = first_task()
    second_task(first)


second_dag_xcom()