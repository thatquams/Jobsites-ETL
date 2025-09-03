from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# from pendulum import timezone, datetime, timedelta
from airflow.decorators import task, dag
from airflow.models.baseoperator import chain
from include.extension import connectToJobSite, MAX_PAGES, CURRENT_PAGE
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor

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
    def integrateRecords(jobberman_data, myjobmag_data):
        import pandas as pd 

        combined_df = pd.concat([pd.DataFrame(jobberman_data), pd.DataFrame(myjobmag_data)], ignore_index=True)
        return combined_df.to_dict(orient="records")
    
    jobberMan_task = extract_jobberman()
    myJobMag_task = extract_myJobMag()
    

    chain([wait_for_jobberman, wait_for_myjobmag], [jobberMan_task, myJobMag_task], integrateRecords(jobberMan_task, myJobMag_task))

job_sites_dag_instance = job_sites_dag()