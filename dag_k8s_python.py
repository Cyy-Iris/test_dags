from airflow.decorators import task, dag
from airflow import DAG
from kubernetes import config
from datetime import datetime
import pendulum

@dag(
    dag_id="dag_k8s_python",
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 29, tz="UTC"),
    catchup=False,
    tags=["k8s_python"],
)
def test_python():
    @task.kubernetes(
        image="193995948507.dkr.ecr.eu-central-1.amazonaws.com/auto-modeling-python:3.8-slim-buster",
        name="k8s_test",
        #namespace="airflow",
        namespace="productmodellingtool-prod-v2-axa-rev",
        in_cluster=True,
        config_file=None,
        image_pull_secrets="ecr-token"
    )
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    @task.kubernetes(image="193995948507.dkr.ecr.eu-central-1.amazonaws.com/auto-modeling-python:3.8-slim-buster", namespace="productmodellingtool-prod-v2-axa-rev",in_cluster=True,image_pull_secrets="ecr-token")
    def print_pattern():
        n = 5
        for i in range(0, n):
            # inner loop to handle number of columns
            # values changing acc. to outer loop
            for j in range(0, i + 1):
                # printing stars
                print("* ", end="")

            # ending line after each row
            print("\r")

    execute_in_k8s_pod_instance = execute_in_k8s_pod()
    print_pattern_instance = print_pattern()

    execute_in_k8s_pod_instance >> print_pattern_instance
dag_run=test_python()
