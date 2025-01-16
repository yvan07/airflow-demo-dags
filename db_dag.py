from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

# Définir les paramètres par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Créer le DAG
with DAG(
    'test_db_connection_error',
    default_args=default_args,
    description='Test DAG to connect to a non-existent database',
    schedule_interval=None,  # Pas de planification automatique
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'kubernetes', 'error-handling'],
) as dag:

    # KubernetesPodOperator pour tenter de se connecter à une DB inexistante
    connect_to_db = KubernetesPodOperator(
        namespace='default',  # Namespace où le Pod sera lancé
        name='connect-to-fake-db',  # Nom du Pod
        task_id='connect_to_fake_db',  # ID unique pour la tâche
        image='python:3.9',  # Image Docker contenant Python
        cmds=['python', '-c'],  # Commande exécutée dans le conteneur
        arguments=[
            """
import psycopg2
try:
    # Tenter de se connecter à une base de données fictive
    connection = psycopg2.connect(
        dbname='nonexistent_db',
        user='fake_user',
        password='fake_password',
        host='fake_host',
        port=5432
    )
except Exception as e:
    print('Failed to connect to the database:')
    print(e)
    raise e
"""
        ],
        is_delete_operator_pod=True,  # Supprimer le Pod après exécution
        in_cluster=True,  # Airflow est exécuté dans le cluster Kubernetes
    )

    connect_to_db
