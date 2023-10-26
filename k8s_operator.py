from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

default_args = {
    'owner': 'yiyu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_k8s",
    start_date=datetime(2023, 10, 24),
    schedule_interval='0 3 * * Tue-Fri'
) as dag:
    task1 = KubernetesPodOperator(
    name="k8s_Operator_test",
    image="debian",
    cmds=["pip list"],
    task_id="k8s_Operator_demo",
    do_xcom_push=True,
)
    task1
