import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ 
        Delete tables from database.
        
        Parameters:
            cur = SQL cursor
            conn = Connection to database
            
        Returns:
            NONE
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
        Create tables in database.
        
        Parameters:
            cur = SQL cursor
            conn = Connection to database
            
        Returns:
            NONE        
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Connects to AWS Redshift, deletes existing table, creates new ones and populate them.
        
        Variables and Objects:
            host = AWS Redshift cluster address
            dbname = Database name
            user = IAM user
            password = password for user
            port = port to connect to DataBase            
            cur = SQL cursor
            conn = Connection to DataBase           
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()