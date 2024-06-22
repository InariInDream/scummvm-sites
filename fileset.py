from flask import Flask, request, render_template, redirect, url_for, render_template_string, jsonify
import pymysql.cursors
import json
import re
import os
from user_fileset_functions import user_calc_key, file_json_to_array, user_insert_queue, user_insert_fileset, match_and_merge_user_filesets
from pagination import create_page
import difflib
from pymysql.converters import escape_string

app = Flask(__name__)

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

@app.route('/')
def index():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" type="text/css" href="{{ url_for('static', filename='style.css') }}">
    </head>
    <body>
    <h1>Fileset Database</h1>
    <h2>Fileset Actions</h2>
    <ul>
        <li><a href="{{ url_for('fileset') }}">Fileset</a></li>
        <li><a href="{{ url_for('user_games_list') }}">User Games List</a></li>
        <li><a href="{{ url_for('games_list') }}">Games List</a></li>
    </ul>
    <h2>Logs</h2>
    <ul>
        <li><a href="{{ url_for('logs') }}">Logs</a></li>
    </ul>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/fileset', methods=['GET', 'POST'])
def fileset():
    id = request.args.get('id', default = 1, type = int)
    widetable = request.args.get('widetable', default = 'false', type = str)
    # Load MySQL credentials from a JSON file
    with open('mysql_config.json') as f:
        mysql_cred = json.load(f)

    # Create a connection to the MySQL server
    connection = pymysql.connect(host=mysql_cred["servername"],
                                 user=mysql_cred["username"],
                                 password=mysql_cred["password"],
                                 db=mysql_cred["dbname"],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Get the minimum id from the fileset table
            cursor.execute("SELECT MIN(id) FROM fileset")
            min_id = cursor.fetchone()['MIN(id)']

            # Get the id from the GET parameters, or use the minimum id if it's not provided
            id = request.args.get('id', default=min_id, type=int)

            # Get the maximum id from the fileset table
            cursor.execute("SELECT MAX(id) FROM fileset")
            max_id = cursor.fetchone()['MAX(id)']

            # Ensure the id is between the minimum and maximum id
            id = max(min_id, min(id, max_id))

            # Check if the id exists in the fileset table
            cursor.execute(f"SELECT id FROM fileset WHERE id = {id}")
            if cursor.rowcount == 0:
                # If the id doesn't exist, get a new id from the history table
                cursor.execute(f"SELECT fileset FROM history WHERE oldfileset = {id}")
                id = cursor.fetchone()['fileset']

            # Get the history for the current id
            cursor.execute(f"SELECT `timestamp`, oldfileset, log FROM history WHERE fileset = {id} ORDER BY `timestamp`")
            history = cursor.fetchall()

            # Display fileset details
            html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <link rel="stylesheet" type="text/css" href="{{{{ url_for('static', filename='style.css') }}}}">
        </head>
        <body>
        <h2><u>Fileset: {id}</u></h2>
        <h3>Fileset details</h3>
        <table>
        """
            html += f"<td><button onclick=\"location.href='/fileset/{id}/merge'\">Merge</button></td>"

            cursor.execute(f"SELECT * FROM fileset WHERE id = {id}")
            result = cursor.fetchone()
            print(result)
            html += "<h3>Fileset details</h3>"
            html += "<table>\n"
            if result['game']:
                cursor.execute(f"SELECT game.name as 'game name', engineid, gameid, extra, platform, language FROM fileset JOIN game ON game.id = fileset.game JOIN engine ON engine.id = game.engine WHERE fileset.id = {id}")
                result = {**result, **cursor.fetchone()}
            else:
                result.pop('key', None)
                result.pop('status', None)
                result.pop('delete', None)

            for column in result.keys():
                if column != 'id' and column != 'game':
                    html += f"<th>{column}</th>\n"

            html += "<tr>\n"
            for column, value in result.items():
                if column != 'id' and column != 'game':
                    html += f"<td>{value}</td>"
            html += "</tr>\n"
            html += "</table>\n"

            # Files in the fileset
            html += "<h3>Files in the fileset</h3>"
            html += "<form>"
            for k, v in request.args.items():
                if k != 'widetable':
                    html += f"<input type='hidden' name='{k}' value='{v}'>"
            if widetable == 'true':
                html += "<input class='hidden' type='text' name='widetable' value='true' />"
                html += "<input type='submit' value='Hide extra checksums' />"
            else:
                html += "<input class='hidden' type='text' name='widetable' value='false' />"
                html += "<input type='submit' value='Expand Table' />"
            html += "</form>"

            # Table
            html += "<table>\n"

            cursor.execute(f"SELECT file.id, name, size, checksum, detection FROM file WHERE fileset = {id}")
            result = cursor.fetchall()

            if widetable == 'true':
                file_ids = [file['id'] for file in result]
                cursor.execute(f"SELECT file, checksum, checksize, checktype FROM filechecksum WHERE file IN ({','.join(map(str, file_ids))})")
                checksums = cursor.fetchall()

                checksum_dict = {}
                for checksum in checksums:
                    if checksum['checksize'] != 0:
                        key = f"{checksum['checktype']}-{checksum['checksize']}"
                        if checksum['file'] not in checksum_dict:
                            checksum_dict[checksum['file']] = {}
                        checksum_dict[checksum['file']][key] = checksum['checksum']

                for index, file in enumerate(result):
                    if file['id'] in checksum_dict:
                        result[index].update(checksum_dict[file['id']])

            counter = 1
            for row in result:
                if counter == 1:
                    html += "<th/>\n" # Numbering column
                    for key in row.keys():
                        if key != 'id':
                            html += f"<th>{key}</th>\n"
                html += "<tr>\n"
                html += f"<td>{counter}.</td>\n"
                for key, value in row.items():
                    if key != 'id':
                        html += f"<td>{value}</td>\n"
                html += "</tr>\n"
                counter += 1
            html += "</table>\n"

            # Generate the HTML for the developer actions
            html += "<h3>Developer Actions</h3>"
            html += f"<button id='delete-button' type='button' onclick='delete_id({id})'>Mark Fileset for Deletion</button>"
            html += f"<button id='match-button' type='button' onclick='match_id({id})'>Match and Merge Fileset</button>"

            if 'delete' in request.form:
                cursor.execute(f"UPDATE fileset SET `delete` = TRUE WHERE id = {request.form['delete']}")
                connection.commit()
                html += "<p id='delete-confirm'>Fileset marked for deletion</p>"

            if 'match' in request.form:
                match_and_merge_user_filesets(request.form['match'])
                return redirect(url_for('fileset', id=request.form['match']))

            # Generate the HTML for the fileset history
            cursor.execute(f"SELECT `timestamp`, category, `text`, id FROM log WHERE `text` REGEXP 'Fileset:{id}' ORDER BY `timestamp` DESC, id DESC")
            logs = cursor.fetchall()

            html += "<h3>Fileset history</h3>"
            html += "<table>\n"
            html += "<th>Timestamp</th>\n"
            html += "<th>Category</th>\n"
            html += "<th>Description</th>\n"
            html += "<th>Log ID</th>\n"
            cursor.execute("SELECT * FROM history")
            history = cursor.fetchall()

            oldfilesets = [history_row['oldfileset'] for history_row in history]
            cursor.execute(f"""SELECT `timestamp`, category, `text`, id FROM log WHERE `text` LIKE 'Fileset:%' AND `category` NOT LIKE 'merge%' AND `text` REGEXP 'Fileset:({"|".join(map(str, oldfilesets))})' ORDER BY `timestamp` DESC, id DESC""")
            logs = cursor.fetchall()

            for log in logs:
                html += "<tr>\n"
                html += f"<td>{log['timestamp']}</td>\n"
                html += f"<td>{log['category']}</td>\n"
                html += f"<td>{log['text']}</td>\n"
                html += f"<td><a href='logs?id={log['id']}'>{log['id']}</a></td>\n"
                html += "</tr>\n"
            html += "</table>\n"
            return render_template_string(html)
    finally:
        connection.close()
        
@app.route('/fileset/<int:id>/merge', methods=['GET', 'POST'])
def merge_fileset(id):
    if request.method == 'POST':
        search_query = request.form['search']
        
        with open('mysql_config.json') as f:
            mysql_cred = json.load(f)

        connection = pymysql.connect(
            host=mysql_cred["servername"],
            user=mysql_cred["username"],
            password=mysql_cred["password"],
            db=mysql_cred["dbname"],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                query = f"""
                SELECT 
                    fs.*, 
                    g.name AS game_name, 
                    g.engine AS game_engine, 
                    g.platform AS game_platform,
                    g.language AS game_language
                FROM 
                    fileset fs
                LEFT JOIN 
                    game g ON fs.game = g.id
                LEFT JOIN 
                    file f ON fs.id = f.fileset
                WHERE g.name LIKE '%{search_query}%' OR g.platform LIKE '%{search_query}%' OR g.language LIKE '%{search_query}%'
                """
                cursor.execute(query)
                results = cursor.fetchall()

                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <link rel="stylesheet" type="text/css" href="{{{{ url_for('static', filename='style.css') }}}}">
                </head>
                <body>
                <h2>Search Results for '{search_query}'</h2>
                <form method="POST">
                    <input type="text" name="search" placeholder="Search fileset">
                    <input type="submit" value="Search">
                </form>
                <table>
                <tr><th>ID</th><th>Game Name</th><th>Platform</th><th>Language</th><th>Action</th></tr>
                """
                for result in results:
                    html += f"""
                    <tr>
                        <td>{result['id']}</td>
                        <td>{result['game_name']}</td>
                        <td>{result['game_platform']}</td>
                        <td>{result['game_language']}</td>
                        <td><a href="/fileset/{id}/merge/confirm?target_id={result['id']}">Select</a></td>
                    </tr>
                    """
                html += "</table>\n"
                html += "</body>\n</html>"

                return render_template_string(html)

        finally:
            connection.close()

    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" type="text/css" href="{{ url_for('static', filename='style.css') }}">
    </head>
    <body>
    <h2>Search Fileset to Merge</h2>
    <form method="POST">
        <input type="text" name="search" placeholder="Search fileset">
        <input type="submit" value="Search">
    </form>
    </body>
    </html>
    '''
    
@app.route('/fileset/<int:id>/merge/confirm', methods=['GET', 'POST'])
def confirm_merge(id):
    target_id = request.args.get('target_id', type=int)

    with open('mysql_config.json') as f:
        mysql_cred = json.load(f)

    connection = pymysql.connect(
        host=mysql_cred["servername"],
        user=mysql_cred["username"],
        password=mysql_cred["password"],
        db=mysql_cred["dbname"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    fs.*, 
                    g.name AS game_name, 
                    g.engine AS game_engine, 
                    g.platform AS game_platform,
                    g.language AS game_language
                    (SELECT COUNT(*) FROM file WHERE fileset = fs.id) AS file_count
                FROM 
                    fileset fs
                LEFT JOIN 
                    game g ON fs.game = g.id
                WHERE 
                    fs.id = {id}
            """)
            source_fileset = cursor.fetchone()
            print(source_fileset)
            cursor.execute(f"""
                SELECT 
                    fs.*, 
                    g.name AS game_name, 
                    g.engine AS game_engine, 
                    g.platform AS game_platform,
                    g.language AS game_language
                    (SELECT COUNT(*) FROM file WHERE fileset = fs.id) AS file_count
                FROM 
                    fileset fs
                LEFT JOIN 
                    game g ON fs.game = g.id
                WHERE 
                    fs.id = {target_id}
            """)
            target_fileset = cursor.fetchone()

            def highlight_differences(source, target):
                diff = difflib.ndiff(source, target)
                source_highlighted = ""
                target_highlighted = ""
                for d in diff:
                    if d.startswith('-'):
                        source_highlighted += f"<span style='color: green;'>{d[2:]}</span>"
                    elif d.startswith('+'):
                        target_highlighted += f"<span style='color: red;'>{d[2:]}</span>"
                    elif d.startswith(' '):
                        source_highlighted += d[2:]
                        target_highlighted += d[2:]
                return source_highlighted, target_highlighted

            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <link rel="stylesheet" type="text/css" href="{{ url_for('static', filename='style.css') }}">
            </head>
            <body>
            <h2>Confirm Merge</h2>
            <table border="1">
            <tr><th>Field</th><th>Source Fileset</th><th>Target Fileset</th></tr>
            """
            for column in source_fileset.keys():
                source_value = str(source_fileset[column])
                target_value = str(target_fileset[column])
                if column == 'id':
                    html += f"<tr><td>{column}</td><td><a href='/fileset?id={source_value}'>{source_value}</a></td><td><a href='/fileset?id={source_value}'>{target_value}</a></td></tr>"
                    continue
                if source_value != target_value:
                    source_highlighted, target_highlighted = highlight_differences(source_value, target_value)
                    html += f"<tr><td>{column}</td><td>{source_highlighted}</td><td>{target_highlighted}</td></tr>"
                else:
                    html += f"<tr><td>{column}</td><td>{source_value}</td><td>{target_value}</td></tr>"

            html += """
            </table>
            <form method="POST" action="{{ url_for('execute_merge', id=id) }}">
                <input type="hidden" name="source_id" value="{{ source_fileset['id'] }}">
                <input type="hidden" name="target_id" value="{{ target_fileset['id'] }}">
                <input type="submit" value="Confirm Merge">
            </form>
            <form action="{{ url_for('fileset', id=id) }}">
                <input type="submit" value="Cancel">
            </form>
            </body>
            </html>
            """
            return render_template_string(html, source_fileset=source_fileset, target_fileset=target_fileset, id=id)

    finally:
        connection.close()

@app.route('/fileset/<int:id>/merge/execute', methods=['POST'])
def execute_merge(id):
    source_id = request.form['source_id']
    target_id = request.form['target_id']

    with open('mysql_config.json') as f:
        mysql_cred = json.load(f)

    connection = pymysql.connect(
        host=mysql_cred["servername"],
        user=mysql_cred["username"],
        password=mysql_cred["password"],
        db=mysql_cred["dbname"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM fileset WHERE id = {source_id}")
            source_fileset = cursor.fetchone()

            cursor.execute(f"""
            UPDATE fileset SET
                game = '{source_fileset['game']}',
                status = '{source_fileset['status']}',
                `key` = '{source_fileset['key']}',
                megakey = '{source_fileset['megakey']}',
                `timestamp` = '{source_fileset['timestamp']}'
            WHERE id = {target_id}
            """)
                
            cursor.execute(f"DELETE FROM file WHERE fileset = {target_id}")

            cursor.execute(f"SELECT * FROM file WHERE fileset = {source_id}")
            source_files = cursor.fetchall()

            for file in source_files:
                cursor.execute(f"""
                INSERT INTO file (name, size, checksum, fileset, detection)
                VALUES ('{escape_string(file['name'])}', '{file['size']}', '{file['checksum']}', {target_id}, {file['detection']})
                """)

                cursor.execute("SELECT LAST_INSERT_ID() as file_id")
                new_file_id = cursor.fetchone()['file_id']
                
                cursor.execute(f"SELECT * FROM filechecksum WHERE file = {file['id']}")
                file_checksums = cursor.fetchall()

                for checksum in file_checksums:
                    cursor.execute(f"""
                    INSERT INTO filechecksum (file, checksize, checktype, checksum)
                    VALUES ({new_file_id}, '{checksum['checksize']}', '{checksum['checktype']}', '{checksum['checksum']}')
                    """)

            cursor.execute(f"""
            INSERT INTO history (`timestamp`, fileset, oldfileset, log)
            VALUES (NOW(), {target_id}, {source_id}, 1)
            """)


            connection.commit()

            return redirect(url_for('fileset', id=target_id))

    finally:
        connection.close()

@app.route('/validate', methods=['POST'])
def validate():

    error_codes = {
        "unknown": -1,
        "success": 0,
        "empty": 2,
        "no_metadata": 3,
    }

    json_object = request.get_json()

    ip = request.remote_addr
    ip = '.'.join(ip.split('.')[:3]) + '.X'

    game_metadata = {k: v for k, v in json_object.items() if k != 'files'}

    json_response = {
        'error': error_codes['success'],
        'files': []
    }

    if not game_metadata:
        if not json_object.get('files'):
            json_response['error'] = error_codes['empty']
            del json_response['files']
            json_response['status'] = 'empty_fileset'
            return jsonify(json_response)

        json_response['error'] = error_codes['no_metadata']
        del json_response['files']
        json_response['status'] = 'no_metadata'

        fileset_id = user_insert_fileset(json_object['files'], ip, conn)
        json_response['fileset'] = fileset_id
        # TODO: handle database operations

        return jsonify(json_response)
    
@app.route('/user_games_list')
def user_games_list():
    filename = "user_games_list"
    records_table = "fileset"
    select_query = """
    SELECT engineid, gameid, extra, platform, language, game.name,
    status, fileset.id as fileset
    FROM fileset
    LEFT JOIN game ON game.id = fileset.game
    LEFT JOIN engine ON engine.id = game.engine
    WHERE status = 'user'
    """
    order = "ORDER BY gameid"
    filters = {
        "engineid": "engine",
        "gameid": "game",
        "extra": "game",
        "platform": "game",
        "language": "game",
        "name": "game",
        "status": "fileset"
    }
    mapping = {
        'engine.id': 'game.engine',
        'game.id': 'fileset.game',
    }
    return render_template_string(create_page(filename, 200, records_table, select_query, order, filters, mapping))


@app.route('/games_list')
def games_list():
    filename = "games_list"
    records_table = "game"
    select_query = """
    SELECT engineid, gameid, extra, platform, language, game.name,
    status, fileset.id as fileset
    FROM game
    JOIN engine ON engine.id = game.engine
    JOIN fileset ON game.id = fileset.game
    """
    order = "ORDER BY gameid"
    filters = {
        "engineid": "engine",
        "gameid": "game",
        "extra": "game",
        "platform": "game",
        "language": "game",
        "name": "game",
        'status': 'fileset'
    }
    mapping = {
        'engine.id': 'game.engine',
        'game.id': 'fileset.game',
    }
    return render_template_string(create_page(filename, 25, records_table, select_query, order, filters, mapping))

@app.route('/logs')
def logs():
    filename = "logs"
    records_table = "log"
    select_query = "SELECT id, `timestamp`, category, user, `text` FROM log"
    order = "ORDER BY `timestamp` DESC, id DESC"
    filters = {
        'id': 'log',
        'timestamp': 'log',
        'category': 'log',
        'user': 'log',
        'text': 'log'
    }
    return render_template_string(create_page(filename, 25, records_table, select_query, order, filters))


if __name__ == '__main__':
    app.run()