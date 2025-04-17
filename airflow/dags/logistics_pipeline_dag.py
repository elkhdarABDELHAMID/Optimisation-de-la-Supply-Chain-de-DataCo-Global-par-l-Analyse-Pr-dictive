from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer
import json
import snowflake.connector
import os

# Fonction pour collecter les données via l'API
def fetch_logistics_data():
    api_url = "http://api:8000/api/logistics"  # Remplace par l'URL de ton API
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        print(f"Données collectées : {data}")
        return data
    else:
        raise Exception(f"Échec de la collecte des données : {response.status_code}")

# Fonction pour envoyer les données à Kafka
def send_to_kafka():
    data = fetch_logistics_data()
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    for record in data:
        producer.send("logistics_topic", record)
        print(f"Données envoyées à Kafka : {record}")
    producer.flush()
    producer.close()

# Fonction pour consommer depuis Kafka et stocker dans Snowflake
def consume_and_store_to_snowflake():
    from kafka import KafkaConsumer

    # Configuration Kafka
    consumer = KafkaConsumer(
        "logistics_topic",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="logistics_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    # Configuration Snowflake
    conn = snowflake.connector.connect(
        account="UQDHYDO-VI51041",
        user="ELKHDAR",
        password="980719Ha@-Ha@@",
        database="logistics_db",
        schema="public",
        warehouse="COMPUTE_WH",
        timeout=120
    )
    cursor = conn.cursor()

    # Créer la table si elle n'existe pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logistics_data (
            id STRING,
            shipment_id STRING,
            status STRING,
            timestamp STRING
        )
    """)

    # Consommer et stocker
    for message in consumer:
        data = message.value
        try:
            cursor.execute(
                """
                INSERT INTO logistics_data (id, shipment_id, status, timestamp)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    data.get("id", ""),
                    data.get("shipment_id", ""),
                    data.get("status", ""),
                    data.get("timestamp", "")
                )
            )
            conn.commit()
            print("Données insérées dans Snowflake.")
        except Exception as e:
            print(f"Erreur lors de l'insertion : {e}")
            conn.rollback()
        break  # Limite à un message pour cette exécution du DAG

    cursor.close()
    conn.close()
    consumer.close()

# Définir les paramètres du DAG
default_args = {
    "owner": "dataco_global",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Créer le DAG
with DAG(
    "logistics_pipeline_dag",
    default_args=default_args,
    description="Pipeline pour collecter, envoyer à Kafka et stocker dans Snowflake",
    schedule_interval=timedelta(hours=1),  # Exécuter toutes les heures
    start_date=datetime(2025, 4, 17),
    catchup=False,
) as dag:

    # Tâche 1 : Collecter les données et envoyer à Kafka
    fetch_and_send_task = PythonOperator(
        task_id="fetch_and_send_to_kafka",
        python_callable=send_to_kafka,
    )

    # Tâche 2 : Consommer depuis Kafka et stocker dans Snowflake
    consume_and_store_task = PythonOperator(
        task_id="consume_and_store_to_snowflake",
        python_callable=consume_and_store_to_snowflake,
    )

    # Définir l'ordre des tâches
    fetch_and_send_task >> consume_and_store_task