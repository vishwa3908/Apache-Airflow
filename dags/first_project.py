from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import requests
from random import randint


def multiplication_table(**context):
    table = []
    num = context.get("number")
    for i in range(1,8):
        table.append(num*i)
    context["ti"].xcom_push(key = "table",value = max(table))
    return table

def check_number_divisible_by_2(**context):
    data = context.get("ti").xcom_pull(key = "table")
    if data %2 == 0:
        context["ti"].xcom_push(key="divisible",value = data)
        context["ti"].xcom_push(key = "table",value = data)
        return "divisible"
    else:
        context["ti"].xcom_push(key="not_divisible",value = data)
        context["ti"].xcom_push(key = "table",value = data)
        return "not_divisible"

def divisible():
    print("in divisible")

def not_divisible():
    print("in not divisible")


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "admin",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date":datetime(2022, 4, 12)
        },
        catchup=False) as f:

    multiplication_table = PythonOperator(
        task_id = "multiplication_table",
        python_callable = multiplication_table,
        provide_context = True,
        op_kwargs = {"number":randint(7,18)}
    )
    check_number_divisible_by_2 = BranchPythonOperator(
        task_id = "check_number_divisible_by_2",
        python_callable = check_number_divisible_by_2,
        provide_context = True
    )
    divisible = PythonOperator(
        task_id = "divisible",
        python_callable = divisible
    )
    not_divisible = PythonOperator(
        task_id = "not_divisible",
        python_callable = not_divisible
    )

multiplication_table >> check_number_divisible_by_2 >> [divisible,not_divisible]