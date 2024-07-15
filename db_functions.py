import pymysql
import json
from collections import Counter
import getpass
import time
import hashlib
import os
from pymysql.converters import escape_string
from collections import defaultdict
import re

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

def insert_fileset(src, detection, key, megakey, transaction, log_text, conn, ip='', username=None, skiplog=None):
    status = "detection" if detection else src
    game = "NULL"
    key = "NULL" if key == "" else f"'{key}'"
    megakey = "NULL" if megakey == "" else f"'{megakey}'"

    if detection:
        status = "detection"
        game = "@game_last"

    # Check if key/megakey already exists, if so, skip insertion (no quotes on purpose)
    if detection:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id FROM fileset WHERE megakey = {megakey}")

            existing_entry = cursor.fetchone()
    else:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id FROM fileset WHERE `key` = {key}")

            existing_entry = cursor.fetchone()

    if existing_entry is not None:
        existing_entry = existing_entry['id']
        with conn.cursor() as cursor:
            cursor.execute(f"SET @fileset_last = {existing_entry}")
            cursor.execute(f"DELETE FROM file WHERE fileset = {existing_entry}")
            cursor.execute(f"UPDATE fileset SET `timestamp` = FROM_UNIXTIME(@fileset_time_last) WHERE id = {existing_entry}")
            cursor.execute(f"UPDATE fileset SET status = 'detection' WHERE id = {existing_entry} AND status = 'obsolete'")

        category_text = f"Updated Fileset:{existing_entry}"
        log_text = f"Updated Fileset:{existing_entry}, {log_text}"
        user = f'cli:{getpass.getuser()}' if username is None else username
        if not skiplog:
            log_last = create_log(escape_string(category_text), user, escape_string(log_text), conn)
            update_history(existing_entry, existing_entry, conn, log_last)
            
        return True

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
        log_text = f"Created Fileset:{fileset_last}, from user: IP {ip}, {log_text}"

    user = f'cli:{getpass.getuser()}' if username is None else username
    if not skiplog:
        log_last = create_log(escape_string(category_text), user, escape_string(log_text), conn)
        update_history(fileset_last, fileset_last, conn, log_last)
    else:
        update_history(fileset_last, fileset_last, conn)
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

    if not detection:
        checktype = "None"
    query = f"INSERT INTO file (name, size, checksum, fileset, detection, detection_type) VALUES ('{escape_string(file['name'])}', '{file['size']}', '{checksum}', @fileset_last, {detection}, '{checktype}-{checksize}')"
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

def update_history(source_id, target_id, conn, log_last=None):
    query = f"INSERT INTO history (`timestamp`, fileset, oldfileset, log) VALUES (NOW(), {target_id}, {source_id}, {log_last if log_last is not None else 0})"
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

def get_all_related_filesets(fileset_id, conn, visited=None):
    if visited is None:
        visited = set()

    if fileset_id in visited:
        return []
    
    visited.add(fileset_id)

    related_filesets = [fileset_id]
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT fileset, oldfileset FROM history WHERE fileset = {fileset_id} OR oldfileset = {fileset_id}")
        history_records = cursor.fetchall()

    for record in history_records:
        if record['fileset'] not in visited:
            related_filesets.extend(get_all_related_filesets(record['fileset'], conn, visited))
        if record['oldfileset'] not in visited:
            related_filesets.extend(get_all_related_filesets(record['oldfileset'], conn, visited))

    return related_filesets

def convert_log_text_to_links(log_text):
    log_text = re.sub(r'Fileset:(\d+)', r'<a href="/fileset?id=\1">Fileset:\1</a>', log_text)
    log_text = re.sub(r'user:(\w+)', r'<a href="/log?search=user:\1">user:\1</a>', log_text)
    log_text = re.sub(r'Transaction:(\d+)', r'<a href="/transaction?id=\1">Transaction:\1</a>', log_text)
    return log_text

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

def calc_megakey(fileset):
    key_string = f":{fileset['platform']}:{fileset['language']}"
    for file in fileset['rom']:
        for key, value in file.items():
            key_string += ':' + str(value)

    key_string = key_string.strip(':')
    return hashlib.md5(key_string.encode()).hexdigest()

def db_insert(data_arr, username=None, skiplog=False):
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

    conn.cursor().execute(f"SET @fileset_time_last = {int(time.time())}")

    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(`transaction`) FROM transactions")
        temp = cursor.fetchone()['MAX(`transaction`)']
        if temp == None:
            temp = 0
        transaction_id = temp + 1

    category_text = f"Uploaded from {src}"
    log_text = f"Started loading DAT file, size {os.path.getsize(filepath)}, author {author}, version {version}. State {status}. Transaction: {transaction_id}"

    user = f'cli:{getpass.getuser()}' if username is None else username
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

        key = calc_key(fileset) if not detection else ""
        megakey = calc_megakey(fileset) if detection else ""
        log_text = f"size {os.path.getsize(filepath)}, author {author}, version {version}. State {status}."

        if insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn, username=username, skiplog=skiplog):
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
        log_text = f"Completed loading DAT file, filename {filepath}, size {os.path.getsize(filepath)}, author {author}, version {version}. State {status}. Number of filesets: {fileset_insertion_count}. Transaction: {transaction_id}"
    except Exception as e:
        print("Inserting failed:", e)
    else:
        user = f'cli:{getpass.getuser()}' if username is None else username
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

def find_matching_game(game_files):
    matching_games = []  # All matching games
    matching_filesets = []  # All filesets containing one file from game_files
    matches_count = 0  # Number of files with a matching detection entry

    conn = db_connect()

    for file in game_files:
        checksum = file[1]

        query = f"SELECT file.fileset as file_fileset FROM filechecksum JOIN file ON filechecksum.file = file.id WHERE filechecksum.checksum = '{checksum}' AND file.detection = TRUE"
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()

        # If file is not part of detection entries, skip it
        if len(records) == 0:
            continue

        matches_count += 1
        for record in records:
            matching_filesets.append(record[0])

    # Check if there is a fileset_id that is present in all results
    for key, value in Counter(matching_filesets).items():
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(file.id) FROM file JOIN fileset ON file.fileset = fileset.id WHERE fileset.id = '{key}'")
            count_files_in_fileset = cursor.fetchone()['COUNT(file.id)']

        # We use < instead of != since one file may have more than one entry in the fileset
        # We see this in Drascula English version, where one entry is duplicated
        if value < matches_count or value < count_files_in_fileset:
            continue

        with conn.cursor() as cursor:
            cursor.execute(f"SELECT engineid, game.id, gameid, platform, language, `key`, src, fileset.id as fileset FROM game JOIN fileset ON fileset.game = game.id JOIN engine ON engine.id = game.engine WHERE fileset.id = '{key}'")
            records = cursor.fetchall()

        matching_games.append(records[0])

    if len(matching_games) != 1:
        return matching_games

    # Check the current fileset priority with that of the match
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id FROM fileset, ({query}) AS res WHERE id = file_fileset AND status IN ({', '.join(['%s']*len(game_files[3]))})", status_to_match(game_files[3]))
        records = cursor.fetchall()

    # If priority order is correct
    if len(records) != 0:
        return matching_games

    if compare_filesets(matching_games[0]['fileset'], game_files[0][0], conn):
        with conn.cursor() as cursor:
            cursor.execute(f"UPDATE fileset SET `delete` = TRUE WHERE id = {game_files[0][0]}")
        return []

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

    # Getting unmatched filesets
    unmatched_filesets = []

    with conn.cursor() as cursor:
        cursor.execute("SELECT fileset.id, filechecksum.checksum, src, status FROM fileset JOIN file ON file.fileset = fileset.id JOIN filechecksum ON file.id = filechecksum.file WHERE fileset.game IS NULL AND status != 'user'")
        unmatched_files = cursor.fetchall()

    # Splitting them into different filesets
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

        if len(matching_games) != 1: # If there is no match/non-unique match
            continue

        matched_game = matching_games[0]

        # Update status depending on $matched_game["src"] (dat -> partialmatch, scan -> fullmatch)
        status = fileset[0][2]
        if fileset[0][2] == "dat":
            status = "partialmatch"
        elif fileset[0][2] == "scan":
            status = "fullmatch"

        # Convert NULL values to string with value NULL for printing
        matched_game = {k: 'NULL' if v is None else v for k, v in matched_game.items()}

        category_text = f"Matched from {fileset[0][2]}"
        log_text = f"Matched game {matched_game['engineid']}:\n{matched_game['gameid']}-{matched_game['platform']}-{matched_game['language']}\nvariant {matched_game['key']}. State {status}. Fileset:{fileset[0][0]}."

        # Updating the fileset.game value to be $matched_game["id"]
        query = f"UPDATE fileset SET game = {matched_game['id']}, status = '{status}', `key` = '{matched_game['key']}' WHERE id = {fileset[0][0]}"

        history_last = merge_filesets(matched_game["fileset"], fileset[0][0])

        if cursor.execute(query):
            user = f'cli:{getpass.getuser()}'

            create_log("Fileset merge", user, escape_string(f"Merged Fileset:{matched_game['fileset']} and Fileset:{fileset[0][0]}"), conn)

            # Matching log
            log_last = create_log(escape_string(conn, category_text), user, escape_string(conn, log_text))

            # Add log id to the history table
            cursor.execute(f"UPDATE history SET log = {log_last} WHERE id = {history_last}")

        try:
            conn.commit()
        except:
            print("Updating matched games failed")
            
def match_fileset(data_arr, username=None):
    header, game_data, resources, filepath = data_arr

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
    source_status = "detection" if detection else src

    conn.cursor().execute(f"SET @fileset_time_last = {int(time.time())}")

    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(`transaction`) FROM transactions")
        transaction_id = cursor.fetchone()['MAX(`transaction`)'] + 1

    category_text = f"Uploaded from {src}"
    log_text = f"Started loading DAT file, size {os.path.getsize(filepath)}, author {author}, version {version}. State {source_status}. Transaction: {transaction_id}"

    user = f'cli:{getpass.getuser()}' if username is None else username
    create_log(escape_string(category_text), user, escape_string(log_text), conn)

    for fileset in game_data:
        process_fileset(fileset, resources, detection, src, conn, transaction_id, filepath, author, version, source_status, user)
    finalize_fileset_insertion(conn, transaction_id, src, filepath, author, version, source_status, user)

def process_fileset(fileset, resources, detection, src, conn, transaction_id, filepath, author, version, source_status, user):
    if detection:
        insert_game_data(fileset, conn)
    elif src == "dat" and 'romof' in fileset and fileset['romof'] in resources:
        fileset["rom"] += resources[fileset["romof"]]["rom"]

    key = calc_key(fileset) if not detection else ""
    megakey = calc_megakey(fileset) if detection else ""
    log_text = f"size {os.path.getsize(filepath)}, author {author}, version {version}. State {source_status}."
    if src != "dat":
        matched_map = find_matching_filesets(fileset, conn)
    else:
        matched_map = matching_set(fileset, conn)

    if matched_map:
        handle_matched_filesets(matched_map, fileset, conn, detection, src, key, megakey, transaction_id, log_text, user)
    else:
        insert_new_fileset(fileset, conn, detection, src, key, megakey, transaction_id, log_text, user)

def insert_game_data(fileset, conn):
    engine_name = fileset["engine"]
    engineid = fileset["sourcefile"]
    gameid = fileset["name"]
    title = fileset["title"]
    extra = fileset["extra"]
    platform = fileset["platform"]
    lang = fileset["language"]
    insert_game(engine_name, engineid, title, gameid, extra, platform, lang, conn)

def find_matching_filesets(fileset, conn):
    matched_map = defaultdict(int)
    with conn.cursor() as cursor:
        for file in fileset["rom"]:
            matched_set = set()
            for key, value in file.items():
                if key not in ["name", "size"]:
                    checksum = file[key]
                    checktype = key
                    checksize, checktype, checksum = get_checksum_props(checktype, checksum)
                    query = f"""SELECT DISTINCT fs.id AS fileset_id
                                FROM fileset fs
                                JOIN file f ON fs.id = f.fileset
                                JOIN filechecksum fc ON f.id = fc.file
                                WHERE fc.checksum = '{checksum}' AND fc.checktype = '{checktype}'
                                AND fs.status IN ('detection', 'dat', 'scan', 'partial', 'full', 'obsolete')"""
                    cursor.execute(query)
                    records = cursor.fetchall()
                    if records:
                        for record in records:
                            matched_set.add(record['fileset_id'])

            for id in matched_set:
                matched_map[id] += 1
                        
    print(matched_map)
    return matched_map

def matching_set(fileset, conn):
    matched_map = defaultdict(int)
    with conn.cursor() as cursor:
        for file in fileset["rom"]:
            if "md5" in file:
                checksum = file["md5"]
                size = file["size"]
                query = f"""
                    SELECT DISTINCT fs.id AS fileset_id
                    FROM fileset fs
                    JOIN file f ON fs.id = f.fileset
                    JOIN filechecksum fc ON f.id = fc.file
                    WHERE fc.checksum = '{checksum}' AND fc.checktype = 'md5'
                    AND f.size > {size}
                    AND fs.status = 'detection'
                """
                cursor.execute(query)
                records = cursor.fetchall()
                if records:
                    for record in records:
                        matched_map[record['fileset_id']] += 1
                    break
    return matched_map

def handle_matched_filesets(matched_map, fileset, conn, detection, src, key, megakey, transaction_id, log_text, user):
    matched_list = sorted(matched_map.items(), key=lambda x: x[1], reverse=True)
    is_full_matched = False
    with conn.cursor() as cursor:
        for matched_fileset_id, matched_count in matched_list:
            if is_full_matched:
                break
            cursor.execute(f"SELECT status FROM fileset WHERE id = {matched_fileset_id}")
            status = cursor.fetchone()['status']
            cursor.execute(f"SELECT COUNT(file.id) FROM file WHERE fileset = {matched_fileset_id}")
            count = cursor.fetchone()['COUNT(file.id)']

            if status in ['detection', 'obsolete'] and count == matched_count:
                is_full_matched = True
                update_fileset_status(cursor, matched_fileset_id, 'full' if src != "dat" else "partial")
                insert_files(fileset, matched_fileset_id, conn, detection)
                log_matched_fileset(src, matched_fileset_id, 'full' if src != "dat" else "partial", user, conn)
            elif status == 'full' and len(fileset['rom']) == count:
                is_full_matched == True
                log_matched_fileset(src, matched_fileset_id, 'full', user, conn)
                return
            elif status == 'partial' and count == matched_count:
                update_fileset_status(cursor, matched_fileset_id, 'full')
                insert_files(fileset, matched_fileset_id, conn, detection)
                log_matched_fileset(src, matched_fileset_id, 'full', user, conn)
            elif status == 'scan' and count == matched_count:
                log_matched_fileset(src, matched_fileset_id, 'full', user, conn)
                return
            elif src == 'dat':
                log_matched_fileset(src, matched_fileset_id, 'partial matched', user, conn)
            else:
                insert_new_fileset(fileset, conn, detection, src, key, megakey, transaction_id, log_text, user)

def update_fileset_status(cursor, fileset_id, status):
    cursor.execute(f"""
        UPDATE fileset SET 
            status = '{status}', 
            `timestamp` = FROM_UNIXTIME({int(time.time())})
        WHERE id = {fileset_id}
    """)

def insert_files(fileset, fileset_id, conn, detection):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM file WHERE fileset = {fileset_id}")
        target_files = cursor.fetchall()
        target_files_dict = {}
        for target_file in target_files:
            cursor.execute(f"SELECT * FROM filechecksum WHERE file = {target_file['id']}")
            target_checksums = cursor.fetchall()
            for checksum in target_checksums:
                target_files_dict[checksum['checksum']] = target_file
                target_files_dict[target_file['id']] = f"{checksum['checktype']}-{checksum['checksize']}"
        for file in fileset['rom']:
            file_exists = False
            cursor.execute(f"INSERT INTO file (name, size, checksum, fileset, detection) VALUES ('{escape_string(file['name'])}', '{file['size']}', '{file['md5']}', {fileset_id}, {0})")
            cursor.execute("SET @file_last = LAST_INSERT_ID()")
            cursor.execute("SELECT @file_last AS file_id")
            file_id = cursor.fetchone()['file_id']
            target_id = None
            for key, value in file.items():
                if key not in ["name", "size"]:
                    insert_filechecksum(file, key, conn)
                    if value in target_files_dict and not file_exists:
                        file_exists = True
                        target_id = target_files_dict[value]['id']
                        cursor.execute(f"DELETE FROM file WHERE id = {target_files_dict[value]['id']}")
            
            if file_exists:
                cursor.execute(f"UPDATE file SET detection = 1 WHERE id = {file_id}")
                cursor.execute(f"UPDATE file SET detection_type = '{target_files_dict[target_id]}' WHERE id = {file_id}")
            else:
                cursor.execute(f"UPDATE file SET detection_type = 'None' WHERE id = {file_id}")

def insert_new_fileset(fileset, conn, detection, src, key, megakey, transaction_id, log_text, user):
    if insert_fileset(src, detection, key, megakey, transaction_id, log_text, conn, username=user):
        for file in fileset["rom"]:
            insert_file(file, detection, src, conn)
            for key, value in file.items():
                if key not in ["name", "size"]:
                    insert_filechecksum(file, key, conn)

def log_matched_fileset(src, fileset_id, state, user, conn):
    category_text = f"Matched from {src}"
    log_text = f"Matched Fileset:{fileset_id}. State {state}."
    log_last = create_log(escape_string(category_text), user, escape_string(log_text), conn)
    update_history(fileset_id, fileset_id, conn, log_last)

def finalize_fileset_insertion(conn, transaction_id, src, filepath, author, version, source_status, user):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(fileset) from transactions WHERE `transaction` = {transaction_id}")
        fileset_insertion_count = cursor.fetchone()['COUNT(fileset)']
        category_text = f"Uploaded from {src}"
        log_text = f"Completed loading DAT file, filename {filepath}, size {os.path.getsize(filepath)}, author {author}, version {version}. State {source_status}. Number of filesets: {fileset_insertion_count}. Transaction: {transaction_id}"
        create_log(escape_string(category_text), user, escape_string(log_text), conn)
    conn.close()

def find_user_match_filesets(fileset, conn):
    matched_map = defaultdict(int)
    with conn.cursor() as cursor:
        for file in fileset["files"]:
            matched_set = set()
            for checksum_info in file["checksums"]:
                checksum = checksum_info["checksum"]
                checktype = checksum_info["type"]
                checksize, checktype, checksum = get_checksum_props(checktype, checksum)
                query = f"""SELECT DISTINCT fs.id AS fileset_id
                                FROM fileset fs
                                JOIN file f ON fs.id = f.fileset
                                JOIN filechecksum fc ON f.id = fc.file
                                WHERE fc.checksum = '{checksum}' AND fc.checktype = '{checktype}'
                                AND fs.status IN ('detection', 'dat', 'scan', 'partial', 'full', 'obsolete')"""
                cursor.execute(query)
                records = cursor.fetchall()
                if records:
                    for record in records:
                        matched_set.add(record['fileset_id'])
            for id in matched_set:
                matched_map[id] += 1
                        
    print(matched_map)
    return matched_map

def user_integrity_check(data):
    print(data)
    src = "user"
    source_status = src
    try:
        conn = db_connect()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return
    
    conn.cursor().execute(f"SET @fileset_time_last = {int(time.time())}")

    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(`transaction`) FROM transactions")
        transaction_id = cursor.fetchone()['MAX(`transaction`)'] + 1

    category_text = f"Uploaded from {src}"
    log_text = f"Started loading file, State {source_status}. Transaction: {transaction_id}"

    user = f'cli:{getpass.getuser()}'

    create_log(escape_string(category_text), user, escape_string(log_text), conn)