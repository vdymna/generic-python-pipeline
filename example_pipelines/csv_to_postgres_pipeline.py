import io
from urllib import request
import csv
import psycopg2

from pipeline.pipeline import Pipeline


DATA_FILE_URL = 'https://dq-content.s3.amazonaws.com/251/storm_data.csv'

DB_HOST = 'localhost'
DB_NAME = '' # set database name
DB_USER = '' # set database user name
DB_PASSWORD = '' # set database user password


pipeline = Pipeline()


@pipeline.task()
def create_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME, 
        user=DB_USER, 
        password=DB_PASSWORD
    )


@pipeline.task(depends_on=create_db_connection)
def create_db_tables(db_conn):
    """Create database tables for staging and final data."""
    cursor = db_conn.cursor()

    cursor.execute("""
        DROP TABLE IF EXISTS storm_data_staging;
        
        CREATE TABLE storm_data_staging
        (
            row_id INT,
            year SMALLINT,
            month SMALLINT,
            day SMALLINT,
            ad_time TIME WITH TIME ZONE,
            btid INT,
            name VARCHAR(100),
            lat DECIMAL(3, 1),
            long DECIMAL(4, 1),
            wind_kts INT,
            pressure INT,
            cat VARCHAR(10),
            basin VARCHAR(20),
            shape_leng DECIMAL(8, 6)
        )
    """)
    db_conn.commit()
    print("'storm_data_staging' TABLE WAS CREATED")

    cursor.execute("""
        CREATE TABLE storm_data
        (
            row_id INT NOT NULL PRIMARY KEY,
            recorded_timestamp TIMESTAMP WITH TIME ZONE,
            btid INT,
            name VARCHAR(100),
            lat DECIMAL(3, 1),
            long DECIMAL(4, 1),
            wind_kts INT,
            pressure INT,
            cat VARCHAR(10),
            basin VARCHAR(20),
            shape_leng DECIMAL(8, 6)
        )
    """) 
    db_conn.commit()
    print("'storm_data' TABLE WAS CREATED")
    
    return db_conn


@pipeline.task(depends_on=create_db_tables)
def load_data_into_staging(db_conn):
    """Load data file into staging table."""
    response_data = request.urlopen(DATA_FILE_URL)
    data_file = io.TextIOWrapper(response_data)
    
    cursor = db_conn.cursor()

    cursor.copy_expert('COPY storm_data_staging FROM STDIN WITH CSV HEADER', data_file)   
    db_conn.commit()

    table_name = 'storm_data_staging'
    print("LOADED {} ROWS INTO '{}' TABLE".format(get_table_row_count(db_conn, table_name), table_name))

    return db_conn


@pipeline.task(depends_on=load_data_into_staging)
def load_final_table(db_conn):
    """Load data into final table from staging."""
    cursor = db_conn.cursor()

    cursor.execute("""
        INSERT INTO storm_data 
        (
            row_id,
            recorded_timestamp,
            btid,
            name,
            lat,
            long,
            wind_kts,
            pressure,
            cat,
            basin,
            shape_leng
        )       
        SELECT 
            row_id,
            TO_TIMESTAMP(year||'-'||month||'-'||day||' '||ad_time, 'YYYY-MM-DD HH24:MI:SS'),
            btid,
            name,
            lat,
            long,
            wind_kts,
            pressure,
            cat,
            basin,
            shape_leng
        FROM storm_data_staging
    """)
    db_conn.commit()

    table_name = 'storm_data'
    print("LOADED {} ROWS INTO '{}' TABLE".format(get_table_row_count(db_conn, table_name), table_name))

    return db_conn


@pipeline.task(depends_on=load_final_table)
def create_db_users_and_groups(db_conn):
    """Created needed database groups and users with appropriate permissions."""
    cursor = db_conn.cursor()

    cursor.execute("""
        CREATE GROUP data_analyst NOLOGIN;
        REVOKE ALL ON storm_data_staging, storm_data FROM data_analyst;
        GRANT SELECT, INSERT, UPDATE ON storm_data TO data_analyst;
    """)

    cursor.execute("""
        CREATE USER vsmith WITH PASSWORD 'temppass123' NOSUPERUSER IN GROUP data_analyst;
    """)   
    
    db_conn.commit()

    print("CREATED DB GROUP AND USERS")
    
    return db_conn


@pipeline.task(depends_on=create_db_users_and_groups)
def close_db_connection(db_conn):
    """After the work is done, close the database connection."""
    db_conn.close()


def get_table_row_count(db_conn, table_name):
    """Get basic table row count."""
    cursor = db_conn.cursor()

    cursor.execute("SELECT COUNT(1) FROM {}".format(table_name))

    return cursor.fetchone()[0]


if __name__ == '__main__':
    pipeline.run()