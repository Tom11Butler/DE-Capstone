import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies data from S3 into the staging tables
    
    Functionality:
    Loads in the relevant queries and runs them
    See sql_queries.py for implementation details
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data from staging tables into schema tables
    
    Functionality:
    Loads in the relevant queries and runs them
    See sql_queries.py for implementation details
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Function that is ran when the file is called directly
    
    Functionality:
    Connects to the Amazon Redshift cluster
    Calls the loading and inserting functions
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()