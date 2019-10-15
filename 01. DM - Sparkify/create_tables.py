import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Create the spartifydb database.
    If it already exists, drop and create the database.
    Args: 
        None
    Output:
        cursor(psycopg2.cursor): The psycopg2 cursor
        connection(psycopg2.connection): The sparkifydb connection
    """
    
    # Connect to default database
    try: 
        # Connect to the local instance of PostgreSQL (localhost)
        # Reach out to the database (postgres) and use the correct privilages (user and password) to connect to the database
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=140139")
        # Set automactic commit so that each action is commited without having to call conn.commit() after each command.
        conn.set_session(autocommit=True)
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
    # Use the connection to get a cursor that will be used to execute queries
    try:
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    # Check if sparkify DB already exists - delete it if it does
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e: 
        print("Error: Could not get to delete sparkify database")
        print(e)
    
    # Create sparkify database with UTF8 encoding
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e: 
        print("Error: Could not get to create sparkify database")
        print(e)
        
    # Close connection to default database
    try:
        conn.close()    
    except psycopg2.Error as e: 
        print("Error: Could not close connection to DB postgres")
        print(e)
        
    # Connect to sparkify database
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=140139")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Read DROP TABLE queries from `sql_queries.drop_table_queries` and execute them.
    Args: 
        cursor(psycopg2.cursor): The psycopg2 cursor
        connection(psycopg2.connection): The sparkifydb connection
    Output:
        None
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Read CREATE TABLE queries from `sql_queries.create_table_queries` and execute them.
    Args: 
        cursor(psycopg2.cursor): The psycopg2 cursor
        connection(psycopg2.connection): The sparkifydb connection
    Output:
        None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to Postgresql, create new DB (sparkifydb),
    Drop any existing tables.
    Create new tables. 
    Close DB connection.

    Args: 
        None
    Output:
        None
    """
    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()