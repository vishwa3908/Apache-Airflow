from platform import python_branch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import timedelta,datetime
from random import randint
import os


from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator







def a_list_of_numbers(**context):
    num_list = []
    for i in range(6):
        num_list.append(randint(1,100))
    print(num_list)
    context["ti"].xcom_push(key = "num_list",value = num_list)

def odd_number(**context):
    num_list = context.get("ti").xcom_pull(key = "num_list")
    odd_list = []
    for i in num_list:
        if i%2 != 0:
            odd_list.append(i)
    print(odd_list)
    odd_total = sum(odd_list)
    print(odd_total)
    context["ti"].xcom_push(key = "num_list",value = num_list)
    context["ti"].xcom_push(key = "odd_list",value = odd_list)
    context["ti"].xcom_push(key = "odd_total",value = odd_total)

def even_number(**context):
    num_list = context.get("ti").xcom_pull(key = "num_list")
    even_list = []
    for i in num_list:
        if i%2 == 0:
            even_list.append(i)
    print(even_list)
    even_total = sum(even_list)
    print(even_total)
    context["ti"].xcom_push(key = "num_list",value = num_list)
    context["ti"].xcom_push(key = "even_list",value = even_list)
    context["ti"].xcom_push(key = "even_total",value = even_total)

    
def check_sum(**context):
    odd_sum = context.get("ti").xcom_pull(key ="odd_total")
    even_sum = context.get("ti").xcom_pull(key ="even_total")
    if odd_sum > even_sum:
        return "odd_sum_forward"
    elif odd_sum == even_sum:
        return "equal_forward"
    else:
        return "even_sum_forward"

def odd_sum_forward():
    print("odd sum is great")

def even_sum_forward():
    print("even sum is great")

def equal_forward():
    print("equal sum")

def welcome_message():
    print("welcome home")


with DAG(
    dag_id = "third_dag",
    schedule_interval = "@daily",
    default_args = {
        "owner":"admin",
        "retries":1,
        "retry_delay":timedelta(minutes=2),
        "start_date":datetime(2022,4,12),
    },catchup = False
)as dag:
    a_list_of_numbers = PythonOperator(
        task_id = "a_list_of_numbers",
        python_callable = a_list_of_numbers,
        provide_context = True
    )

    odd_number = PythonOperator(
        task_id = "odd_number",
        python_callable = odd_number,
        provide_context = True
    )

    even_number = PythonOperator(
        task_id = "even_number",
        python_callable = even_number,
        provide_context = True
    )
    check_sum = BranchPythonOperator(
        task_id = "check_sum",
        python_callable = check_sum,
        provide_context = True,
        do_xcom_push = False
    )

    odd_sum_forward = PythonOperator(
        task_id = "odd_sum_forward",
        python_callable = odd_sum_forward
    )

    even_sum_forward = PythonOperator(
        task_id = "even_sum_forward",
        python_callable = even_sum_forward
    )
    equal_forward = PythonOperator(
        task_id = "equal_forward",
        python_callable = equal_forward
    )
    welcome_message = PythonOperator(
        task_id = "welcome_message",
        python_callable = welcome_message,
        trigger_rule = "one_success"
    )

    # k = KubernetesPodOperator(
    # namespace = "default",
    # name="hello-dry-run",
    # image="python",
    # cmds=["python", "-c"],
    # arguments=["print('hello k8s')"],
    # labels={"foo": "bar"},
    # task_id="dry_run_demo",
    # in_cluster = False,
    # do_xcom_push=True,
    # get_logs = True,
    # config_file = os.path.expanduser('~')+"/.kube/config"
)

    a_list_of_numbers >> [odd_number,even_number] >> check_sum >> [odd_sum_forward,even_sum_forward,equal_forward] >> welcome_message 