import pymysql
import json

def db_connect():
    with open('mysql_config.json') as f:
        mysql_cred = json.load(f)
    
    conn = pymysql.connect(
        host=mysql_cred["servername"],
        user=mysql_cred["username"],
        password=mysql_cred["password"],
        db=mysql_cred["dbname"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )
    
    return conn

def insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn, ip):
    query = """
        INSERT INTO fileset (source, detection, `key`, megakey, `transaction`, log, ip)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (src, int(detection), key, megakey, transaction_id, log_text, ip))
            conn.commit()
            cursor.execute("SET @fileset_last = LAST_INSERT_ID()")
        return True
    except pymysql.MySQLError as e:
        print(f"Insert fileset failed: {e}")
        return False

def insert_file(file, detection, src, conn):
    query = """
        INSERT INTO file (name, size, detection, source, fileset)
        VALUES (%s, %s, %s, %s, @fileset_last)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (file['name'], file['size'], int(detection), src))
            conn.commit()
            cursor.execute("SET @file_last = LAST_INSERT_ID()")
        return True
    except pymysql.MySQLError as e:
        print(f"Insert file failed: {e}")
        return False

def insert_filechecksum(file, key, conn):
    query = """
        INSERT INTO filechecksum (file, checksum, checktype)
        VALUES (@file_last, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (file[key], key))
            conn.commit()
        return True
    except pymysql.MySQLError as e:
        print(f"Insert file checksum failed: {e}")
        return False

def find_matching_game(fileset):
    # TODO: Implement logic to find matching game for a fileset
    pass

def merge_filesets(fileset1, fileset2):
    # TODO: Implement logic to merge two filesets
    pass

def create_log(category, user, text, conn):
    query = """
        INSERT INTO log (category, user, text)
        VALUES (%s, %s, %s)
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (category, user, text))
            conn.commit()
            cursor.execute("SET @log_last = LAST_INSERT_ID()")
        return True
    except pymysql.MySQLError as e:
        print(f"Insert log failed: {e}")
        return False

def get_current_user():
    # Implement logic to get current user
    pass