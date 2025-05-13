import psycopg2

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "dw_flights"
DB_USER = "postgres"
DB_PASSWORD = "Password1234**"


def load_data(csv_file_name, table_name, conn):
    """
    Load data into PostgreSQL database.
    This function connects to the PostgreSQL database and loads data.
    """
    # Placeholder for loading data logic   

    load_table('dataset/nyc_airlines.csv', 'airlines', conn, ('carrier', 'name'))
    load_table('dataset/nyc_airports.csv', 'airports', conn, ('faa', 'name', 'lat', 'lon','alt','tz','dst','tzone')) 
    #load_table('dataset/nyc_flights.csv', 'flights', conn, ('carrier', 'name')) ,year,month,day,dep_time,sched_dep_time,dep_delay,arr_time,sched_arr_time,arr_delay,carrier,flight,tailnum,origin,dest,air_time,distance,hour,minute,time_hour
    load_table('dataset/nyc_planes.csv', 'planes', conn, ('tailnum','year','type','manufacturer','model','engines','seats','speed','engine'))
    load_table('dataset/nyc_weather.csv', 'weather', conn, ('origin','year','month','day','hour','temp','dewp','humid','wind_dir','wind_speed','wind_gust','precip','pressure','visib','time_hour'))

def load_table(csv_file_name, table_name, conn, columns_to_read):
    """
    Load all tables into PostgreSQL database.
    This function connects to the PostgreSQL database and loads data.
    """
    try:

        cur = conn.cursor()

        with open(csv_file_name, 'r') as file:

            next(file)  # Skip the header row

            cur.copy_from(file, table_name, sep=',', columns=columns_to_read)

            conn.commit()
    
    except Exception as e:
        print(f"An error occurred while loading data: {e}")
    finally:
        cur.close()
        print("Data loading completed.")




if __name__ == "__main__":
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connection to the database was successful.")

        load_data('dataset/nyc_airlines.csv', 'airlines', conn)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")