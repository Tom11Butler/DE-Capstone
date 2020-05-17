import configparser
import psycopg2
from sql_queries import drop_table_queries, create_table_queries

def drop_tables(cur, conn):
    '''
    Drops the tables from Amazon Redshift
    
    Inputs:
    - cur: cursor
    - conn: connection
    
    Outputs:
    - NA
    
    The function takes the cursor and connection arguments and
    loops through the queries for dropping the tables in AWS
    '''
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    '''
    Creates the tables from Amazon Redshift
    
    Inputs:
    - cur: cursor
    - conn: connection
    
    Outputs:
    - NA
    
    The function takes the cursor and connection arguments and
    loops through the queries for dropping the tables in AWS
    '''
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    '''
    Runs when the file is called directly
    
    Connects to the Amazon Redshift cluster using the config file
    Drops tables in there as by drop_tables_queries
    (Re)creates the tables as by create_tables_queries
    
    '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()
    

if __name__=="__main__":
    main()