import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import boto3

def drop_tables(cur, conn):
    """
    drop table in redshift cluster db
    param:
    * cur: cursor
    * conn: connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create table in redshift cluster db
    param:
    * cur: cursor
    * conn: connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
def get_HOST(config):
    """
    get host endpoint redshift cluster
    parameter: 
    * config:(get key and secret)
    """
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER = config.get("INFO","DWH_CLUSTER_IDENTIFIER")
    redshift = boto3.client('redshift',
                     region_name='us-east-1',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET)
    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    # return address endpoint
    return response['Clusters'][0]['Endpoint']['Address']
def main():
    """
    - get config file 
    - connect server
    - drop table 
    - create table
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(get_HOST(config),*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Create tables successful !")
    conn.close()


if __name__ == "__main__":
    main()