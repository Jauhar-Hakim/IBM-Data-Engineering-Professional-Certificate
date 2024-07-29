# Import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# Define DAG arguments
default_args = {
    'owner':'Baka Kikim',
    'start_date':days_ago(0),
    'email':['bakakakikim@gmail.com'],# Import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# Define DAG arguments
default_args = {
    'owner':'Baka Kikim',
    'start_date':days_ago(0),
    'email':['bakakakikim@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes = 5)
}

# Define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='ETL and Data Pipeline with Apache AirFlow',
    schedule_interval=timedelta(days=1),
)

# Extract Data Task
extract_data = BashOperator(
    task_id = 'extract_data',
    bash_command = """cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt
                > /home/project/airflow/dags/capstone/extracted_data.txt""",
    dag = dag,
)

# Transform Data Task
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = """grep 198.46.149.143 /home/project/airflow/dags/capstone/extracted_data.txt
                > /home/project/airflow/dags/capstone/transformed_data.txt""",
    dag = dag,
)

# Load Data Task
load_data = BashOperator(
    task_id = 'load_data',
    bash_command = """tar -czvf /home/project/airflow/dags/capstone/weblog.tar
    /home/project/airflow/dags/capstone/transformed_data.txt"""
)

# Define the task pipeline
extract_data >> transform_data >>load_data

 