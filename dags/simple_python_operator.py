from datetime import datetime, timedelta

# Operators; we need this to operate!
# from airflow.operators.python_operator import PythonOperator 

# The DAG object; we'll need this to instantiate a DAG
# from airflow.sdk import DAG
from airflow.sdk import dag, task   # doing things the pythonic way

from typing import Tuple
import numpy as np
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=["example"],
)

def example_python_flow():

    @task()
    def load_data() -> Tuple[np.ndarray]:
        data = load_iris(as_frame=True)
        X = data["data"]
        y = data["target"]
        return tuple(X, y)

    @task()
    def split_data(data: Tuple) -> Tuple[np.ndarray]:
        X, y = data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42
        )
        return tuple(X_train, X_test, y_train, y_test)

    @task
    def preprocess_data(splitted_data) -> Tuple[np.ndarray]:
        X_train, X_test, y_train, y_test = splitted_data
        # random forest does not require normalization
        # do nothing
        return tuple(X_train, X_test, y_train, y_test)

    @task()
    def classify(preprocessed_data):
        X_train, X_test, y_train, y_test = preprocessed_data
        # create logistic regression 
        model = RandomForestClassifier(
            n_estimators=200
        )
        # fit model
        model.fit(X_train, y_train)

        # evaluate fitted model
        score = model.score(X_test, y_test)
        print(f"Score is: {score}")
        # task completed
    
    data = load_data()
    splitted_data = split_data(data)
    preprocessed_data = preprocess_data(splitted_data)
    classify(preprocessed_data)

example_python_flow()