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
        return X, y

    @task()
    def split_data(X:np.ndarray, y:np.ndarray) -> Tuple[np.ndarray]:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.3, random_state=42
        )
        return X_train, X_test, y_train, y_test

    @task
    def preprocess_data(
        X_train:np.ndarray,
        X_test:np.ndarray,
        y_train:np.ndarray, 
        y_test:np.ndarray
    ) -> Tuple[np.ndarray]:
        # random forest does not require normalization
        # do nothing
        return X_train, X_test, y_train, y_test

    @task()
    def classify(
        X_train:np.ndarray,
        X_test:np.ndarray,
        y_train:np.ndarray, 
        y_test:np.ndarray
    ) -> float:
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
    X_train, X_test, y_train, y_test = split_data(data)
    X_train, X_test, y_train, y_test = preprocess_data(
        X_train, X_test, y_train, y_test
    )
    classify(X_train, X_test, y_train, y_test)

example_python_flow()