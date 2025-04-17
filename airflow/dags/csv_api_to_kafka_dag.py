from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer, KafkaConsumer
import json
import snowflake.connector
import os

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

# Fonction pour consommer depuis Kafka et stocker dans Snowflake (basée sur consumer.py)
def consume_from_kafka_and_store_to_snowflake():
    # Configuration Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logistics")

    # Configuration Snowflake
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "UQDHYDO-VI51041")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "ELKHDAR")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "980719Ha@-Ha@@")
    SNOWFLAKE_DATABASE = "logistics_db"
    SNOWFLAKE_SCHEMA = "public"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

    # Initialiser le consommateur Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="logistics_group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    # Initialiser la connexion Snowflake
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        timeout=120
    )

    # Créer une table dans Snowflake si elle n'existe pas
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS logistics_data (
            id STRING,
            shipment_id STRING,
            status STRING,
            timestamp STRING
        )
    """)

    print("Consommation des messages depuis Kafka...")

    # Consommer un message depuis Kafka
    for message in consumer:
        data = message.value
        print(f"Message reçu depuis Kafka : {data}")

        # Insérer les données dans Snowflake
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
            print("Données insérées dans Snowflake avec succès.")
        except Exception as e:
            print(f"Erreur lors de l'insertion dans Snowflake : {e}")
            conn.rollback()
        break  # Limite à un message par exécution pour éviter une boucle infinie

    # Fermer les connexions
    cursor.close()
    conn.close()
    consumer.close()

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
    description="DAG to fetch data from API, send to Kafka, and store in Snowflake",
    schedule_interval=timedelta(hours=1),  # Exécuter toutes les heures
    start_date=datetime(2025, 4, 17),
    catchup=False,
) as dag:

    # Tâche pour récupérer les données et les envoyer à Kafka
    fetch_data_task = PythonOperator(
        task_id="fetch_data_from_api_and_send_to_kafka",
        python_callable=fetch_data_and_send_to_kafka,
    )

    # Tâche pour consommer depuis Kafka et stocker dans Snowflake
    consume_and_store_task = PythonOperator(
        task_id="consume_from_kafka_and_store_to_snowflake",
        python_callable=consume_from_kafka_and_store_to_snowflake,
    )

    # Définir l'ordre des tâches
    fetch_data_task >> consume_and_store_task