from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
import json

# Fonction pour récupérer les données de l'API et les envoyer à Kafka
def fetch_data_and_send_to_kafka():
    # Récupérer les données de l'API
    api_url = "http://logistics-api:8000/api/logistics"
    response = requests.get(api_url)
    response.raise_for_status()  # Lève une exception si la requête échoue
    data = response.json()

    # Initialiser le producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Envoyer chaque ligne de données au topic 'logistics'
    for record in data:
        producer.send("logistics", record)
        print(f"Sent record to Kafka: {record}")

    # Vider le buffer du producteur
    producer.flush()
    producer.close()

# Définir les arguments par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    "csv_api_to_kafka_dag",
    default_args=default_args,
    description="DAG to fetch data from API and send to Kafka",
    schedule_interval=timedelta(hours=1),  # Exécuter toutes les heures
    start_date=datetime(2025, 4, 17),
    catchup=False,
) as dag:

    # Tâche pour récupérer les données et les envoyer à Kafka
    fetch_data_task = PythonOperator(
        task_id="fetch_data_from_api_and_send_to_kafka",
        python_callable=fetch_data_and_send_to_kafka,
    )

    fetch_data_task