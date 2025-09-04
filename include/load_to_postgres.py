from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import pandas as pd
from include.integrate import integrateRecords

TABLE_NAME = "job_listings"


# def loadToPostgres(data, table_name, postgres_conn_id):
def loadToPostgres(data):

    convertDataToDf = pd.DataFrame(data)

    # try:
    # Initialize PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id='aws_postgres_conn')
    

    # Get a connection and cursor
    try:
        _connection = postgres_hook.get_conn()
        _cursor = _connection.cursor()
        try:
            
            select_queery = f"SELECT href FROM {TABLE_NAME};"
            for index, row in convertDataToDf.iterrows():
                insert_query = f"""
                    INSERT INTO {TABLE_NAME} (title, company, posted_at, location, href, source) WHERE href NOT IN ({select_queery})
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                values = (row['title'], row['company'], row['posted_at'], row['location'], row['href'], row['source'])
                _cursor.execute(insert_query, values)
                
            _connection.commit()
            print(f"Successfully inserted {len(convertDataToDf)} records into {TABLE_NAME}.")
            
        
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
        
        finally:
            _cursor.close if _cursor else None
            _connection.close if _connection else None

    except Exception as e:
        print(f"Error obtaining connection: {e}")
        return
    