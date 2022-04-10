import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import get_HOST

def load_staging_tables(cur, conn):
    """
    load data to staging table
    param:
    * conn
    * cur
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("sucess")


def insert_tables(cur, conn):
    """
    insert table to data model
    param
    * cur
    * conn
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - config info aws
    - connect cluster
    - load data staging
    - insert table to dw
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(get_HOST(config),*config['CLUSTER'].values()))
    cur = conn.cursor()
    # load file to staging table 
    load_staging_tables(cur, conn)
    # insert to data model
    insert_tables(cur, conn)
    print("Insert successful !")
    conn.close()


if __name__ == "__main__":
    main()