"""
This script deletes all data from the tables in the database.
Using it when testing the data insertion.
"""

import pymysql
import json

def delete_all_data(conn):
    tables = ["filechecksum", "queue", "history", "transactions", "file", "fileset", "game", "engine", "log"]
    cursor = conn.cursor()
    
    for table in tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            print(f"Table '{table}' data deleted successfully")
        except pymysql.Error as err:
            print(f"Error deleting data from table '{table}': {err}")

if __name__ == "__main__":
    with open(__file__ + '/../mysql_config.json') as f:
        mysql_cred = json.load(f)

    servername = mysql_cred["servername"]
    username = mysql_cred["username"]
    password = mysql_cred["password"]
    dbname = mysql_cred["dbname"]

    # Create connection
    conn = pymysql.connect(
        host=servername,
        user=username,
        password=password,
        db=dbname,  # Specify the database to use
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

    # Check connection
    if conn is None:
        print("Error connecting to MySQL")
        exit(1)

    # Delete all data from tables
    delete_all_data(conn)

    # Close connection
    conn.close()