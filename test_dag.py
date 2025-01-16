from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

# Définit les paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Déclare le DAG
with DAG(
    'example_kubernetes_operator',
    default_args=default_args,
    description='Example DAG using KubernetesPodOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['example', 'kubernetes'],
) as dag:

    # Tâche 1: Exécuter un Pod Kubernetes
    task1 = KubernetesPodOperator(
        namespace='default',  # Namespace Kubernetes où le Pod sera lancé
        name='hello-world-pod',  # Nom du Pod
        task_id='hello_world',  # ID unique de la tâche Airflow
        image='alpine:3.16',  # Image Docker utilisée par le Pod
        cmds=['echo', 'Hello, Kubernetes!'],  # Commandes exécutées dans le Pod
        is_delete_operator_pod=True,  # Supprimer le Pod après exécution
        in_cluster=True,  # Airflow tourne dans le même cluster Kubernetes
    )

    # Tâche 2: Calculer un résultat avec Python dans un Pod
    task2 = KubernetesPodOperator(
        namespace='default',
        name='python-calculate-pod',
        task_id='python_calculate',
        image='python:3.9',  # Image Python officielle
        cmds=['python', '-c'],
        arguments=[
            'print("The result of 42 * 2 is:", 42 * 2)'
        ],
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # Définir l'ordre d'exécution
    task1 >> task2
