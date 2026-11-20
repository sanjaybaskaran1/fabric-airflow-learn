from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.email import send_email

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['baskaransanjay8@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='conditional_branching_if_else_in_dag',
    default_args=default_args,
    description='Conditional branching with an email on successful run',
    start_date=datetime(2026, 11, 20),
    schedule=None,
    catchup=False,
)
def conditional_branching_dag():

    @task.python
    def data_extract_task():
        print("Extracting Data.....")
        return {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "s3_extracted_data": [7, 8, 9],
            "status_flag": "succeeded",
        }

    @task.python
    def task_needs_to_executed_for_success(data: dict):
        print("Executing the success path for data_extract_task")
        print(f"The data retrieved is: {data}")
        status = data["status_flag"]
        return data

    @task.python
    def task_needs_to_executed_for_failure():
        print("data_extract_task did not succeed")
        print("No data found")

    # Creating the Decider Node
    @task.branch
    def decider_task(data: dict):
        if data["status_flag"] == "succeeded":
            return "task_needs_to_executed_for_success"
        else:
            return "task_needs_to_executed_for_failure"

    # Default trigger rule (all_success) - only runs if its upstream succeeded
    @task
    def notify(data: dict):
       if data["status_flag"] == "succeeded":
         send_email(
            to=["baskaransanjay8@gmail.com"],
            subject="Airflow dag SUCCESS",
            html_content=(
                "<h3>Load complete</h3>"
                "<p>task_needs_to_executed_for_success finished without errors.</p>"
                f"<p>Rows received: {len(data['api_extracted_data'])} API, "
                f"{len(data['db_extracted_data'])} DB, "
                f"{len(data['s3_extracted_data'])} S3</p>"
            ),
        )

    # Defining task dependencies
    extracted = data_extract_task()
    decider = decider_task(extracted)
    success_path = task_needs_to_executed_for_success(extracted)
    failure_path = task_needs_to_executed_for_failure()
    notification = notify(success_path)

    decider >> [success_path, failure_path]
    success_path >> notification


# Instantiating the DAG
conditional_branching_dag()