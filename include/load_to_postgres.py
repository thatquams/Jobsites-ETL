from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
import pandas as pd
from include.integrate import integrateRecords

TABLE_NAME = "job_listings"


# def loadToPostgres(data, table_name, postgres_conn_id):
def loadToPostgres(data):
    
    """
        Load integrated job listing records into a PostgreSQL table on AWS RDS.

        This function converts the input data into a pandas DataFrame, then connects
        to PostgreSQL using Airflow's PostgresHook. For each job record, it attempts 
        to insert the data into the target table while checking that the `href` field 
        is not already present (to prevent duplicate entries).

        Args:
            data (list[dict]): A list of job listing dictionaries. Each record is expected 
                to have the keys: 'title', 'company', 'posted_at', 'location', 'href', 'source'.

        Returns:
            None
            Prints the number of successfully inserted records or an error message if something fails.

        Notes:
            - Connection uses Airflow connection ID `aws_postgres_conn`.
            - Table name is defined in the global variable `TABLE_NAME`.
            - Uses `psycopg2` under the hood via PostgresHook.
    """

    convertDataToDf = pd.DataFrame(data)

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
                    INSERT INTO {TABLE_NAME} (href, title, company, posted_at, location, source) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (href) DO NOTHING;

                """
                values = (row['href'], row['title'], row['company'], row['posted_at'], row['location'], row['source'])
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
    