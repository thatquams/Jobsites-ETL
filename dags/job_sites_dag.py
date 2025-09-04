from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# from pendulum import timezone, datetime, timedelta
from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
from include.extension import connectToJobSite, MAX_PAGES, CURRENT_PAGE
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.integrate import integrateRecords
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 31),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=["jobs"])
def job_sites_dag():
    
    wait_for_jobberman = HttpSensor(
        task_id='wait_for_jobberman',
        http_conn_id='jobberman_url',
        endpoint='jobs',
        poke_interval=5,
        timeout=20,
        mode='poke'
    )
    wait_for_myjobmag = HttpSensor(
        task_id='wait_for_myjobmag',
        http_conn_id='myjobmag_url',
        endpoint='jobs',
        poke_interval=5,
        timeout=20,
        mode='poke'
    )
    
    
    @task
    def extract_jobberman():
        from include.jobberman import jobberMan

        jobberman_data = jobberMan(Variable.get("jobberman_base_url"), CURRENT_PAGE)
        return jobberman_data
 
    @task
    def extract_myJobMag():
        from include.myjobmag import myJobMag

        myjobmag_data = myJobMag(Variable.get("myjobmag_base_url"), CURRENT_PAGE)
        return myjobmag_data
    
    @task
    def integrate_results(jobberman_data, myjobmag_data):
        return integrateRecords(jobberman_data, myjobmag_data)
    
    @task
    def load_to_postgres(data):
        from include.load_to_postgres import loadToPostgres
        return loadToPostgres(data)
    
    
    connect_to_pg_db = PostgresOperator(
            task_id="connect_to_pg_db",
            postgres_conn_id="aws_postgres_conn",
            sql="""
                    CREATE TABLE IF NOT EXISTS job_listings (id SERIAL PRIMARY KEY, title TEXT NOT NULL,
                    company TEXT NOT NULL,posted_at TEXT NOT NULL, location TEXT NOT NULL, href TEXT NOT NULL, source TEXT NOT NULL);
                """,
        )
    
    # Call tasks only once
    jobberman_task = extract_jobberman()
    myjobmag_task = extract_myJobMag()
    integrate_results = integrate_results(jobberman_task, myjobmag_task)
    
    chain([wait_for_jobberman, wait_for_myjobmag], [jobberman_task, myjobmag_task],\
          integrate_results, connect_to_pg_db, load_to_postgres(integrate_results))
    # connect_to_pg_db
job_sites_dag = job_sites_dag()