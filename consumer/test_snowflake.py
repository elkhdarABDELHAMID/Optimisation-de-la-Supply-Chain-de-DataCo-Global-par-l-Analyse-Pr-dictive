import snowflake.connector

conn = snowflake.connector.connect(
    account="UQDHYDO-VI51041",
    user="ELKHDAR",
    password="980719Ha@-Ha@@",
    database="logistics_db",
    schema="public",
    warehouse="COMPUTE_WH"
)

cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
result = cursor.fetchone()
print(f"Snowflake version: {result[0]}")

cursor.close()
conn.close()