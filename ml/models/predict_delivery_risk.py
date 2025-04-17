import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd
import snowflake.connector

mlflow.set_experiment("delivery_risk_prediction")

def load_data():
    conn = snowflake.connector.connect(
        user="your_user",
        password="your_password",
        account="your_account",
        warehouse="your_warehouse",
        database="LOGISTICS_DB",
        schema="marts"
    )
    query = "SELECT quantity, expected_duration, is_late FROM delivery_risk"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

with mlflow.start_run():
    df = load_data()
    X = df[['quantity', 'expected_duration']]
    y = df['is_late']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    score = model.score(X_test, y_test)
    mlflow.log_metric("accuracy", score)
    mlflow.sklearn.log_model(model, "random_forest_model")
    
    print(f"Model trained with accuracy: {score}")