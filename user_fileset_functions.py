import hashlib
import time
from db_functions import db_connect, insert_fileset, insert_file, insert_filechecksum, find_matching_game, merge_filesets, create_log, get_current_user

def user_calc_key(user_fileset):
    key_string = ""
    for file in user_fileset:
        for key, value in file.items():
            if key != 'checksums':
                key_string += ':' + str(value)
                continue
            for checksum_pair in value:
                key_string += ':' + checksum_pair['checksum']
    key_string = key_string.strip(':')
    return hashlib.md5(key_string.encode()).hexdigest()

def file_json_to_array(file_json_object):
    res = {}
    for key, value in file_json_object.items():
        if key != 'checksums':
            res[key] = value
            continue
        for checksum_pair in value:
            res[checksum_pair['type']] = checksum_pair['checksum']
    return res

def user_insert_queue(user_fileset, conn):
    query = "INSERT INTO queue (time, notes, fileset, ticketid, userid, commit) VALUES (%s, NULL, @fileset_last, NULL, NULL, NULL)"
    with conn.cursor() as cursor:
        cursor.execute(query, (int(time.time()),))
    conn.commit()

def user_insert_fileset(user_fileset, ip, conn):
    src = 'user'
    detection = False
    key = ''
    megakey = user_calc_key(user_fileset)
    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(`transaction`) FROM transactions")
        transaction_id = cursor.fetchone()['MAX(`transaction`)'] + 1
        log_text = "from user submitted files"
        cursor.execute("SET @fileset_time_last = %s", (int(time.time()),))
        if insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn, ip):
            for file in user_fileset:
                file = file_json_to_array(file)
                insert_file(file, detection, src, conn)
                for key, value in file.items():
                    if key not in ["name", "size"]:
                        insert_filechecksum(file, key, conn)
        cursor.execute("SELECT @fileset_last")
        fileset_id = cursor.fetchone()['@fileset_last']
    conn.commit()
    return fileset_id

def match_and_merge_user_filesets(id, conn):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT fileset.id, filechecksum.checksum, src, status
            FROM fileset
            JOIN file ON file.fileset = fileset.id
            JOIN filechecksum ON file.id = filechecksum.file
            WHERE status = 'user' AND fileset.id = %s
        """, (id,))
        unmatched_files = cursor.fetchall()

    unmatched_filesets = []
    cur_fileset = None
    temp = []
    for file in unmatched_files:
        if cur_fileset is None or cur_fileset != file['id']:
            if temp:
                unmatched_filesets.append(temp)
            cur_fileset = file['id']
            temp = []
        temp.append(file)
    if temp:
        unmatched_filesets.append(temp)

    for fileset in unmatched_filesets:
        matching_games = find_matching_game(fileset)
        if len(matching_games) != 1:
            continue
        matched_game = matching_games[0]
        status = 'fullmatch'
        matched_game = {k: ("NULL" if v is None else v) for k, v in matched_game.items()}
        category_text = f"Matched from {fileset[0]['src']}"
        log_text = f"Matched game {matched_game['engineid']}: {matched_game['gameid']}-{matched_game['platform']}-{matched_game['language']} variant {matched_game['key']}. State {status}. Fileset:{fileset[0]['id']}."
        query = """
            UPDATE fileset
            SET game = %s, status = %s, `key` = %s
            WHERE id = %s
        """
        history_last = merge_filesets(matched_game["fileset"], fileset[0]['id'])
        with conn.cursor() as cursor:
            cursor.execute(query, (matched_game["id"], status, matched_game["key"], fileset[0]['id']))
            user = 'cli:' + get_current_user()
            create_log("Fileset merge", user, f"Merged Fileset:{matched_game['fileset']} and Fileset:{fileset[0]['id']}")
            log_last = create_log(category_text, user, log_text)
            cursor.execute("UPDATE history SET log = %s WHERE id = %s", (log_last, history_last))
        conn.commit()

def insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn, ip):
    
    pass

def insert_file(file, detection, src, conn):
    
    pass

def insert_filechecksum(file, key, conn):
    
    pass

def find_matching_game(fileset):
    
    pass

def merge_filesets(fileset1, fileset2):
    
    pass

def create_log(category, user, text):
    
    pass

def get_current_user():
    
    pass