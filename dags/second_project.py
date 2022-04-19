from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime

def create_table():
    return "creating table"

def select_database():
    return "airflow selected"

def insert_records():
    return "records inserting"

def fetch_records(**context):
  request = "SELECT * FROM airflow_practice"
  mysql_hook = MySqlHook(mysql_conn_id = 'practice', schema = 'airflow')
  connection = mysql_hook.get_conn()
  cursor = connection.cursor()
  cursor.execute(request)
  sources = cursor.fetchall()
  context["ti"].xcom_push(key = "data",value = sources)
  print(sources)

with DAG(
        dag_id="second_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "admin",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2022, 4, 12),
        },
        catchup=False) as f:
    select_database= MySqlOperator(
        task_id = "select_Database",
        mysql_conn_id = "practice",
        sql = "USE airflow")

    create_table= MySqlOperator(
        task_id = "create_table",
        mysql_conn_id = "practice",
        sql = "CREATE TABLE IF NOT EXISTS airflow_practice( ID int auto_increment primary key, DETAILS varchar(255))")

    insert_records = MySqlOperator(
        task_id = "insert_records",
        mysql_conn_id = "practice",
        sql = "INSERT INTO airflow_practice (DETAILS)VALUES('last  INSERTION')"
        )
    fetch_records = PythonOperator(
    task_id = 'fetch_records',
    python_callable = fetch_records,
    provide_context = True
)
    
    select_database >> create_table >> insert_records >> fetch_records

    
        
