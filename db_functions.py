import pymysql
import json
from collections import Counter
import getpass
import time
import hashlib
import os
import argparse
from pymysql.converters import escape_string
from collections import defaultdict

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

def get_checksum_props(checkcode, checksum):
    checksize = 0
    checktype = checkcode

    if '-' in checkcode:
        exploded_checkcode = checkcode.split('-')
        last = exploded_checkcode.pop()
        if last == '1M' or last.isdigit():
            checksize = last

        checktype = '-'.join(exploded_checkcode)

    # Detection entries have checktypes as part of the checksum prefix
    if ':' in checksum:
        prefix = checksum.split(':')[0]
        checktype += "-" + prefix

        checksum = checksum.split(':')[1]

    return checksize, checktype, checksum

def insert_game(engine_name, engineid, title, gameid, extra, platform, lang, conn):
    # Set @engine_last if engine already present in table
    exists = False
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id FROM engine WHERE engineid = '{engineid}'")
        res = cursor.fetchone()
        if res is not None:
            exists = True
            cursor.execute(f"SET @engine_last = '{res['id']}'")

    # Insert into table if not present
    if not exists:
        with conn.cursor() as cursor:
            cursor.execute(f"INSERT INTO engine (name, engineid) VALUES ('{escape_string(engine_name)}', '{engineid}')")
            cursor.execute("SET @engine_last = LAST_INSERT_ID()")

    # Insert into game
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO game (name, engine, gameid, extra, platform, language) VALUES ('{escape_string(title)}', @engine_last, '{gameid}', '{escape_string(extra)}', '{platform}', '{lang}')")
        cursor.execute("SET @game_last = LAST_INSERT_ID()")

def insert_fileset(src, detection, key, megakey, transaction, log_text, conn, ip=''):
    status = "detection" if detection else src
    game = "NULL"
    key = "NULL" if key == "" else f"'{key}'"
    megakey = "NULL" if megakey == "" else f"'{megakey}'"

    if detection:
        status = "detection"
        game = "@game_last"

    # Check if key/megakey already exists, if so, skip insertion (no quotes on purpose)
    with conn.cursor() as cursor:
        if detection:
            cursor.execute(f"SELECT id FROM fileset WHERE `key` = {key}")
        else:
            cursor.execute(f"SELECT id FROM fileset WHERE megakey = {megakey}")

        existing_entry = cursor.fetchone()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", help="override user")
    global args
    args = parser.parse_args()
    if existing_entry is not None:
        existing_entry = existing_entry['id']
        with conn.cursor() as cursor:
            cursor.execute(f"SET @fileset_last = {existing_entry}")

        category_text = f"Uploaded from {src}"
        log_text = f"Duplicate of Fileset:{existing_entry}, {log_text}"
        if src == 'user':
            log_text = f"Duplicate of Fileset:{existing_entry}, from user IP {ip}, {log_text}"

        user = args.user if args.user else f'cli:{getpass.getuser()}'
        create_log(escape_string(category_text), user, escape_string(log_text), conn)

        if not detection:
            return False

        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE fileset SET `timestamp` = FROM_UNIXTIME(@fileset_time_last) WHERE id = {existing_entry}")
            cursor.execute(f"UPDATE fileset SET status = 'detection' WHERE id = {existing_entry} AND status = 'obsolete'")
            cursor.execute("DELETE FROM game WHERE id = @game_last")
        return False

    # $game and $key should not be parsed as a mysql string, hence no quotes
    query = f"INSERT INTO fileset (game, status, src, `key`, megakey, `timestamp`) VALUES ({game}, '{status}', '{src}', {key}, {megakey}, FROM_UNIXTIME(@fileset_time_last))"
    with conn.cursor() as cursor:
        cursor.execute(query)
        cursor.execute("SET @fileset_last = LAST_INSERT_ID()")

    category_text = f"Uploaded from {src}"
    with conn.cursor() as cursor:
        cursor.execute("SELECT @fileset_last")
        fileset_last = cursor.fetchone()['@fileset_last']

    log_text = f"Created Fileset:{fileset_last}, {log_text}"
    if src == 'user':
        log_text = f"Created Fileset:{fileset_last}, from user IP {ip}, {log_text}"

    user = args.user if args.user else f'cli:{getpass.getuser()}'
    create_log(escape_string(category_text), user, escape_string(log_text), conn)
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO transactions (`transaction`, fileset) VALUES ({transaction}, {fileset_last})")

    return True

def insert_file(file, detection, src, conn):
    # Find full md5, or else use first checksum value
    checksum = ""
    checksize = 5000
    if "md5" in file:
        checksum = file["md5"]
    else:
        for key, value in file.items():
            if "md5" in key:
                checksize, checktype, checksum = get_checksum_props(key, value)
                break

    query = f"INSERT INTO file (name, size, checksum, fileset, detection) VALUES ('{escape_string(file['name'])}', '{file['size']}', '{checksum}', @fileset_last, {detection})"
    with conn.cursor() as cursor:
        cursor.execute(query)

    if detection:
        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE fileset SET detection_size = {checksize} WHERE id = @fileset_last AND detection_size IS NULL")
    with conn.cursor() as cursor:
        cursor.execute("SET @file_last = LAST_INSERT_ID()")

def insert_filechecksum(file, checktype, conn):
    if checktype not in file:
        return

    checksum = file[checktype]
    checksize, checktype, checksum = get_checksum_props(checktype, checksum)

    query = f"INSERT INTO filechecksum (file, checksize, checktype, checksum) VALUES (@file_last, '{checksize}', '{checktype}', '{checksum}')"
    with conn.cursor() as cursor:
        cursor.execute(query)

def delete_filesets(conn):
    query = "DELETE FROM fileset WHERE `delete` = TRUE"
    with conn.cursor() as cursor:
        cursor.execute(query)


def create_log(category, user, text, conn):
    query = f"INSERT INTO log (`timestamp`, category, user, `text`) VALUES (FROM_UNIXTIME({int(time.time())}), '{escape_string(category)}', '{escape_string(user)}', '{escape_string(text)}')"
    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Creating log failed: {e}")
            log_last = None
        else:
            cursor.execute("SELECT LAST_INSERT_ID()")
            log_last = cursor.fetchone()['LAST_INSERT_ID()']
    return log_last

def calc_key(fileset):
    key_string = ""

    for key, value in fileset.items():
        if key in ['engineid', 'gameid', 'rom']:
            continue
        key_string += ':' + str(value)

    files = fileset['rom']
    for file in files:
        for key, value in file.items():
            key_string += ':' + str(value)

    key_string = key_string.strip(':')
    return hashlib.md5(key_string.encode()).hexdigest()

def calc_megakey(files):
    key_string = ""

    for file in files:
        for key, value in file.items():
            key_string += ':' + str(value)

    key_string = key_string.strip(':')
    return hashlib.md5(key_string.encode()).hexdigest()

def db_insert(data_arr):
    header = data_arr[0]
    game_data = data_arr[1]
    resources = data_arr[2]
    filepath = data_arr[3]

    try:
        conn = db_connect()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return

    try:
        author = header["author"]
        version = header["version"]
    except KeyError as e:
        print(f"Missing key in header: {e}")
        return

    src = "dat" if author not in ["scan", "scummvm"] else author

    detection = (src == "scummvm")
    status = "detection" if detection else src

    try:
        conn.cursor().execute(f"SET @fileset_time_last = {int(time.time())}")
    except Exception as e:
        print(f"Failed to execute query: {e}")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(`transaction`) FROM transactions")
            transaction_id = cursor.fetchone()['MAX(`transaction`)'] + 1
    except Exception as e:
        print(f"Failed to execute query: {e}")
    finally:
        conn.close()

    category_text = f"Uploaded from {src}"
    log_text = f"Started loading DAT file, size {os.path.getsize(filepath)}, author '{author}', version {version}. State '{status}'. Transaction: {transaction_id}"

    
    user = args.user if args.user else f'cli:{getpass.getuser()}'
    create_log(escape_string(category_text), user, escape_string(log_text), conn)

    for fileset in game_data:
        if detection:
            engine_name = fileset["engine"]
            engineid = fileset["sourcefile"]
            gameid = fileset["name"]
            title = fileset["title"]
            extra = fileset["extra"]
            platform = fileset["platform"]
            lang = fileset["language"]

            insert_game(engine_name, engineid, title, gameid, extra, platform, lang, conn)
        elif src == "dat":
            if 'romof' in fileset and fileset['romof'] in resources:
                fileset["rom"] = fileset["rom"] + resources[fileset["romof"]]["rom"]

        key = calc_key(fileset) if detection else ""
        megakey = calc_megakey(fileset['rom']) if not detection else ""
        log_text = f"size {os.path.getsize(filepath)}, author '{author}', version {version}. State '{status}'."

        if insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn):
            for file in fileset["rom"]:
                insert_file(file, detection, src, conn)
                for key, value in file.items():
                    if key not in ["name", "size"]:
                        insert_filechecksum(file, key, conn)

    if detection:
        conn.cursor().execute("UPDATE fileset SET status = 'obsolete' WHERE `timestamp` != FROM_UNIXTIME(@fileset_time_last) AND status = 'detection'")
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(fileset) from transactions WHERE `transaction` = {transaction_id}")
        fileset_insertion_count = cur.fetchone()['COUNT(fileset)']
        category_text = f"Uploaded from {src}"
        log_text = f"Completed loading DAT file, filename '{filepath}', size {os.path.getsize(filepath)}, author '{author}', version {version}. State '{status}'. Number of filesets: {fileset_insertion_count}. Transaction: {transaction_id}"
    except Exception as e:
        print("Inserting failed:", e)
    else:
        user = args.user if args.user else f'cli:{getpass.getuser()}'
        create_log(escape_string(category_text), user, escape_string(log_text), conn)

def compare_filesets(id1, id2, conn):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT name, size, checksum FROM file WHERE fileset = '{id1}'")
        fileset1 = cursor.fetchall()
        cursor.execute(f"SELECT name, size, checksum FROM file WHERE fileset = '{id2}'")
        fileset2 = cursor.fetchall()

    # Sort filesets on checksum
    fileset1.sort(key=lambda x: x[2])
    fileset2.sort(key=lambda x: x[2])

    if len(fileset1) != len(fileset2):
        return False

    for i in range(len(fileset1)):
        # If checksums do not match
        if fileset1[i][2] != fileset2[i][2]:
            return False

    return True

def status_to_match(status):
    order = ["detection", "dat", "scan", "partialmatch", "fullmatch", "user"]
    return order[:order.index(status)]

def get_fileset_ids_with_matching_files(files, conn):
    if not files:
        return []
    
    checksums = [file['checksum'] for file in files if 'checksum' in file]
    if not checksums:
        return []

    placeholders = ', '.join(['%s'] * len(checksums))

    query = f"""
    SELECT DISTINCT file.fileset
    FROM file
    WHERE file.checksum IN ({placeholders})
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query, checksums)
        result = cursor.fetchall()

    fileset_ids = [row['fileset'] for row in result]

    return fileset_ids

def find_matching_game(game_files):
    matching_games = defaultdict(list)

    conn = db_connect()

    fileset_ids = get_fileset_ids_with_matching_files(game_files, conn)

    while len(fileset_ids) > 100:
        game_files.pop()
        fileset_ids = get_fileset_ids_with_matching_files(game_files, conn)

    if not fileset_ids:
        return matching_games
    
    placeholders = ', '.join(['%s'] * len(fileset_ids))

    query = f"""
    SELECT fileset.id AS fileset, engineid, game.id AS game_id, gameid, platform, language, `key`, src
    FROM fileset
    JOIN file ON file.fileset = fileset.id
    JOIN game ON game.id = fileset.game
    JOIN engine ON engine.id = game.engine
    WHERE fileset.id IN ({placeholders})
    """

    with conn.cursor() as cursor:
        cursor.execute(query, fileset_ids)
        records = cursor.fetchall()
    
    for record in records:
        fileset_id = record['fileset']
        matching_games[fileset_id].append(record)

    return matching_games

def merge_filesets(detection_id, dat_id):
    conn = db_connect()

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT(filechecksum.checksum), checksize, checktype FROM filechecksum JOIN file on file.id = filechecksum.file WHERE fileset = '{detection_id}'")
            detection_files = cursor.fetchall()

            for file in detection_files:
                checksum = file[0]
                checksize = file[1]
                checktype = file[2]

                cursor.execute(f"DELETE FROM file WHERE checksum = '{checksum}' AND fileset = {detection_id} LIMIT 1")
                cursor.execute(f"UPDATE file JOIN filechecksum ON filechecksum.file = file.id SET detection = TRUE, checksize = {checksize}, checktype = '{checktype}' WHERE fileset = '{dat_id}' AND filechecksum.checksum = '{checksum}'")

            cursor.execute(f"INSERT INTO history (`timestamp`, fileset, oldfileset) VALUES (FROM_UNIXTIME({int(time.time())}), {dat_id}, {detection_id})")
            cursor.execute("SELECT LAST_INSERT_ID()")
            history_last = cursor.fetchone()['LAST_INSERT_ID()']

            cursor.execute(f"UPDATE history SET fileset = {dat_id} WHERE fileset = {detection_id}")
            cursor.execute(f"DELETE FROM fileset WHERE id = {detection_id}")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error merging filesets: {e}")
    finally:
        conn.close()

    return history_last


def populate_matching_games():
    conn = db_connect()

    unmatched_filesets = []
    unmatched_files = []

    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT fileset.id, filechecksum.checksum, src, status
            FROM fileset
            JOIN file ON file.fileset = fileset.id
            JOIN filechecksum ON file.id = filechecksum.file
            WHERE fileset.game IS NULL AND status != 'user'
        """)
        unmatched_files = cursor.fetchall()

    i = 0
    while i < len(unmatched_files):
        cur_fileset = unmatched_files[i][0]
        temp = []
        while i < len(unmatched_files) and cur_fileset == unmatched_files[i][0]:
            temp.append(unmatched_files[i])
            i += 1
        unmatched_filesets.append(temp)

    for fileset in unmatched_filesets:
        matching_games = find_matching_game(fileset)

        if len(matching_games) != 1:  
            continue

        matched_game = matching_games[list(matching_games.keys())[0]][0]

        status = fileset[0][2]
        if fileset[0][2] == "dat":
            status = "partialmatch"
        elif fileset[0][2] == "scan":
            status = "fullmatch"

        matched_game = {k: 'NULL' if v is None else v for k, v in matched_game.items()}

        category_text = f"Matched from {fileset[0][2]}"
        log_text = f"Matched game {matched_game['engineid']}:\n{matched_game['gameid']}-{matched_game['platform']}-{matched_game['language']}\nvariant {matched_game['key']}. State {status}. Fileset:{fileset[0][0]}."

        query = f"UPDATE fileset SET game = {matched_game['game_id']}, status = '{status}', `key` = '{matched_game['key']}' WHERE id = {fileset[0][0]}"

        history_last = merge_filesets(matched_game["fileset"], fileset[0][0])

        if cursor.execute(query):
            user = args.user if args.user else f'cli:{getpass.getuser()}'

            create_log("Fileset merge", user, escape_string(f"Merged Fileset:{matched_game['fileset']} and Fileset:{fileset[0][0]}"), conn)

            log_last = create_log(escape_string(f"{category_text}"), user, escape_string(f"{log_text}"), conn)

            cursor.execute(f"UPDATE history SET log = {log_last} WHERE id = {history_last}")

        try:
            conn.commit()
        except:
            print("Updating matched games failed")