import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
#the following import is for debugging
from sql_queries import truncate_final_table_queries, drop_final_table_queries, create_final_table_queries


def load_staging_tables(cur, conn):
    """ iterates through the list of copy commands to load files from S3 to staging tables """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ iterates through the list of (insert select) commands to fill the final tables """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# The three following functions are for the sake of debugging
def truncate_tables(cur, conn):
    """ iterates through a list of truncate commands to clear final tables """
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()
        
def drop_final_tables(cur, conn):
    """ iterates through a list of drop commands to drop final tables """
    for query in drop_final_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_final_tables(cur, conn):
    """ iterates through a list of create commands to create all final tables """
    for query in create_final_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    #fast debugging calls
    #copy_tables(cur, conn)
    #truncate_tables(cur,conn)
    #print("truncate ended")
    #drop_final_tables(cur,conn)
    #print("drop ended")
    #create_final_tables(cur,conn)
    #print("create creation ended")
    #insert_tables(cur, conn)
    #print("insert ended")

    conn.close()


if __name__ == "__main__":
    main()