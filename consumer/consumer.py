import os
from kafka import KafkaConsumer
import snowflake.connector
import json

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logistics_topic")

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

# Boucle pour consommer les messages Kafka
for message in consumer:
    data = message.value
    print(f"Message reçu : {data}")

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

# Fermer les connexions (ne sera jamais atteint car la boucle est infinie)
cursor.close()
conn.close()
consumer.close()