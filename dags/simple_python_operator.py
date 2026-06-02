from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator 

# The DAG object; we'll need this to instantiate a DAG
from airflow.sdk import DAG
# from airflow.sdk import dag, task   # doing things the pythonic way

from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

def classify():
    data = load_iris(as_frame=True)

    X = data["data"]
    y = data["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    # create logistic regression 
    model = RandomForestClassifier(
        n_estimators=200
    )
    # fit model
    model.fit(X_train, y_train)

    # evaluate fitted model
    model.score(X_test, y_test)

    # task completed

with DAG(
    "python tutorial",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="A python task example DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 6, 5),
    catchup=False,
    tags=["example"],
) as dag:

    # t1 is an example of task
    t1 = PythonOperator(
        task_id='classify',
        python_callable=classify
    )
    
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this

    t1