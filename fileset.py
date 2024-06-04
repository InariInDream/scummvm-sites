from flask import Flask, request, render_template, redirect, url_for, render_template_string, jsonify
import pymysql.cursors
import json
import re
import os
from user_fileset_functions import user_calc_key, file_json_to_array, user_insert_queue, user_insert_fileset, match_and_merge_user_filesets

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
            html = f"<h2><u>Fileset: {id}</u></h2>"

            cursor.execute(f"SELECT * FROM fileset WHERE id = {id}")
            result = cursor.fetchone()

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
                html += "<input class='hidden' type='text' name='widetable' value='false' />"
                html += "<input type='submit' value='Hide extra checksums' />"
            else:
                html += "<input class='hidden' type='text' name='widetable' value='true' />"
                html += "<input type='submit' value='Expand Table' />"
            html += "</form>"

            # Table
            html += "<table>\n"

            cursor.execute(f"SELECT file.id, name, size, checksum, detection FROM file WHERE fileset = {id}")
            result = cursor.fetchall()

            if widetable == 'true':
                for index, file in enumerate(result):
                    cursor.execute(f"SELECT checksum, checksize, checktype FROM filechecksum WHERE file = {file['id']}")
                    while True:
                        spec_checksum = cursor.fetchone()
                        if spec_checksum is None:
                            break
                        if spec_checksum['checksize'] == 0:
                            continue
                        result[index][f"{spec_checksum['checktype']}-{spec_checksum['checksize']}"] = spec_checksum['checksum']

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
            cursor.execute(f"SELECT * FROM history")
            history = cursor.fetchall()
            for history_row in history:
                cursor.execute(f"SELECT `timestamp`, category, `text`, id FROM log WHERE `text` LIKE 'Fileset:{history_row['oldfileset']}%' AND `category` NOT LIKE 'merge%' ORDER BY `timestamp` DESC, id DESC")
                logs = cursor.fetchall()
                for log in logs:
                    html += "<tr>\n"
                    html += f"<td>{log['timestamp']}</td>\n"
                    html += f"<td>{log['category']}</td>\n"
                    html += f"<td>{log['text']}</td>\n"
                    html += f"<td><a href='logs.php?id={log['id']}'>{log['id']}</a></td>\n"
                    html += "</tr>\n"
            html += "</table>\n"
            return render_template_string(html)
    finally:
        connection.close()

@app.route('validate', methods=['POST'])
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
    

if __name__ == '__main__':
    app.run()