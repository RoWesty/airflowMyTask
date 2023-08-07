from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from datetime import datetime
from time import localtime, strftime
import decimal
import requests
import psycopg2

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

url_conf = {
    "table_name": "rates",
    "rate_base": "BTC",
    "rate_target": "RUB",
    "url_base": "https://api.exchangerate.host/latest"
}

def request(**kwargs):
    hist_date = "latest"
    url = url_conf['url_base'] + hist_date
    ingest_datetime = strftime("%Y-%m-%d %H: %M: %S", localtime())
    response = requests.get(url, params={'base': url_conf['rate_base']})
    data = response.json()
    rate_date = data['date']
    value = str(decimal.Decimal(data['rates']['RUB']))[:20]
    ti = kwargs['ti']
    ti.xcom_push(key='results', value={"rate_date": rate_date, "value": value})


def dbconnect(**kwargs):
    task_instance = kwargs['task_instance']
    result = task_instance.xcom_pull(key='results', task_ids='requests')
    counter = Variable.get('counter')
    Variable.set("counter", int(counter) + 1)
    count = Variable.get('counter')
    db_connection = psycopg2.connect(dbname='WorkDB',
                                     user='admin',
                                     password='password',
                                     host='host.docker.internal',
                                     port='5432')
    cur = db_connection.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS execute_rate(id int PRIMARY KEY, RUB decimal, dates date)")
    cur.execute("INSERT INTO execute_rate (id, RUB, dates) VALUES (%s, %s, %s)", (count, result["value"], result["rate_date"]))
    db_connection.commit()
    cur.close()
    db_connection.close()

with DAG(dag_id = "Exchange-rate",
            schedule_interval = "*/10 * * * *",
            default_args = default_args,
            tags = ["1T","NewTask","3.3","DE"],
            catchup = False) as DAG:
    bash_task_hello = BashOperator(task_id="hellotask",
                                   bash_command="echo 'Good morning my diggers!'")
    request_echange_rate = PythonOperator(task_id="requests",
                                          python_callable=request)
    db_insert_requst = PythonOperator(task_id="insertrequst",
                                      python_callable=dbconnect)
bash_task_hello >> request_echange_rate >> db_insert_requst