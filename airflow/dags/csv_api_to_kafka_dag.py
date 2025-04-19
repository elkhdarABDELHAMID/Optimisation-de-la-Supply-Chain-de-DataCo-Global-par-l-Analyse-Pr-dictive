from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import snowflake.connector
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def recuperer_donnees_et_envoyer_a_kafka():
    logger.info("Début de la tâche recuperer_donnees_et_envoyer_a_kafka")
    
    bootstrap_servers = "kafka:9092"
    topic_name = "logistic"
    
    logger.info(f"Vérification et création du topic {topic_name}...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='airflow_admin'
        )
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            logger.info(f"Le topic {topic_name} n'existe pas, création en cours...")
            topic_list = [NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic {topic_name} créé avec succès.")
        else:
            logger.info(f"Le topic {topic_name} existe déjà.")
        admin_client.close()
    except Exception as e:
        logger.error(f"Erreur lors de la création du topic {topic_name} : {e}")
        raise

    api_url = "http://logistics-api:8000/api/logistics"
    max_retries = 3
    retry_delay = 5  
    data = None

    for attempt in range(max_retries):
        logger.info(f"Tentative {attempt + 1}/{max_retries} de connexion à l'API : {api_url}")
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Données récupérées de l'API : {data[:3] if isinstance(data, list) else data}")
            break
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des données (tentative {attempt + 1}) : {e}")
            if attempt < max_retries - 1:
                logger.info(f"Attente de {retry_delay} secondes avant la prochaine tentative...")
                time.sleep(retry_delay)
            else:
                logger.error("Échec après toutes les tentatives.")
                raise

    if data is None:
        logger.error("Aucune donnée récupérée de l'API.")
        return

    if isinstance(data, dict) and 'data' in data:
        data = data['data']  
    if not isinstance(data, list):
        logger.info(f"Les données ne sont pas une liste : {data}")
        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, str):
            data = [{"Order Id": "default_id", "Order City": data, "Customer City": "unknown", "order date (DateOrders)": datetime.now().isoformat()}]
        else:
            logger.info("Données non valides, on saute l'envoi")
            return

    for record in data:
        if not isinstance(record, dict):
            logger.info(f"Un élément n'est pas un dictionnaire : {record}")
            continue

    logger.info("Initialisation du producteur Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info("Producteur Kafka initialisé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation du producteur Kafka : {e}")
        raise

    for record in data:
        serialized_record = json.dumps(record)
        logger.info(f"Données sérialisées avant envoi à Kafka : {serialized_record}")
        producer.send(topic_name, record)
        logger.info(f"Enregistrement envoyé à Kafka : {record}")

    try:
        producer.flush()
    except Exception as e:
        logger.error(f"Erreur lors du flush du producer : {e}")
        raise
    finally:
        try:
            producer.close()
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture du producer : {e}")
    
    logger.info("Données envoyées à Kafka avec succès.")

def consommer_de_kafka_et_stocker_dans_snowflake():
    logger.info("Début de la tâche consommer_de_kafka_et_stocker_dans_snowflake")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logistic")
    logger.info(f"Connexion à Kafka sur {KAFKA_BOOTSTRAP_SERVERS}, topic {KAFKA_TOPIC}")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="logistics_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000
        )
        logger.info("Connexion à Kafka réussie.")
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Kafka : {e}")
        raise

    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "UQDHYDO-VI51041")
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "ELKHDAR")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "980719Ha@-Ha@@")
    SNOWFLAKE_DATABASE = "LOGISTICS_DB"
    SNOWFLAKE_SCHEMA = "PUBLIC"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    logger.info("Connexion à Snowflake...")

    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
            timeout=120
        )
        logger.info("Connexion à Snowflake réussie.")
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Snowflake : {e}")
        raise

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS LOGISTICS_DB.PUBLIC.LOGISTICS_DATA (
            id STRING,
            order_city STRING,
            customer_city STRING,
            timestamp STRING
        )
    """)
    logger.info("Tableau LOGISTICS_DATA créé ou déjà existant.")

    logger.info("Consommation des messages depuis Kafka...")
    try:
        max_messages = 100
        message_count = 0
        for message in consumer:
            if message_count >= max_messages:
                break
            data = message.value
            logger.info(f"Données brutes reçues depuis Kafka : {data}")
            try:
                insert_data = (
                    str(data.get("Order Id", "default_id")),
                    str(data.get("Order City", "unknown")),
                    str(data.get("Customer City", "unknown")),
                    str(data.get("order date (DateOrders)", datetime.now().isoformat()))
                )
                logger.info(f"Données à insérer dans Snowflake : {insert_data}")
                cursor.execute(
                    """
                    INSERT INTO LOGISTICS_DB.PUBLIC.LOGISTICS_DATA (id, order_city, customer_city, timestamp)
                    VALUES (%s, %s, %s, %s)
                    """,
                    insert_data
                )
                conn.commit()
                logger.info("Données insérées dans Snowflake avec succès.")
            except Exception as e:
                logger.error(f"Erreur lors de l'insertion dans Snowflake : {e}")
                conn.rollback()
            message_count += 1
    except Exception as e:
        logger.error(f"Erreur lors de la consommation : {e}")
        raise
    finally:
        try:
            cursor.close()
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture du cursor : {e}")
        try:
            conn.close()
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture de la connexion Snowflake : {e}")
        try:
            consumer.close()
        except Exception as e:
            logger.error(f"Erreur lors de la fermeture du consumer : {e}")
    
    logger.info("Tâche terminée.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "csv_api_to_kafka_dag",
    default_args=default_args,
    description="DAG pour récupérer des données d'une API, les envoyer à Kafka, et les stocker dans Snowflake",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 4, 17),
    catchup=False,
) as dag:
    tache_recuperation_donnees = PythonOperator(
        task_id="fetch_data_from_api_and_send_to_kafka",
        python_callable=recuperer_donnees_et_envoyer_a_kafka,
    )
    tache_consommation_et_stockage = PythonOperator(
        task_id="consume_from_kafka_and_store_to_snowflake",
        python_callable=consommer_de_kafka_et_stocker_dans_snowflake,
    )
    tache_recuperation_donnees >> tache_consommation_et_stockage
