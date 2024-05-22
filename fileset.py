from flask import Flask, request, render_template, redirect, url_for, render_template_string
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
    id = request.args.get('id')
    with conn.cursor() as cursor:
        cursor.execute("SELECT MIN(id) AS min_id FROM fileset")
        min_id = cursor.fetchone()['min_id']
        
        if not id:
            id = min_id
        else:
            cursor.execute("SELECT MAX(id) AS max_id FROM fileset")
            max_id = cursor.fetchone()['max_id']
            id = max(min_id, min(int(id), max_id))
            cursor.execute("SELECT id FROM fileset WHERE id = %s", (id,))
            if cursor.rowcount == 0:
                cursor.execute("SELECT fileset FROM history WHERE oldfileset = %s", (id,))
                id = cursor.fetchone()['fileset']

        cursor.execute("SELECT * FROM fileset WHERE id = %s", (id,))
        result = cursor.fetchone()

        if result['game']:
            cursor.execute("""
                SELECT game.name AS 'game name', engineid, gameid, extra, platform, language
                FROM fileset
                JOIN game ON game.id = fileset.game
                JOIN engine ON engine.id = game.engine
                WHERE fileset.id = %s
            """, (id,))
            result.update(cursor.fetchone())
        else:
            result.pop('key', None)
            result.pop('status', None)
            result.pop('delete', None)

        fileset_details = result

        cursor.execute("SELECT file.id, name, size, checksum, detection FROM file WHERE fileset = %s", (id,))
        files = cursor.fetchall()

        if request.args.get('widetable') == 'true':
            for file in files:
                cursor.execute("SELECT checksum, checksize, checktype FROM filechecksum WHERE file = %s", (file['id'],))
                checksums = cursor.fetchall()
                for checksum in checksums:
                    if checksum['checksize'] != 0:
                        file[f"{checksum['checktype']}-{checksum['checksize']}"] = checksum['checksum']

        cursor.execute("""
            SELECT `timestamp`, oldfileset, log
            FROM history
            WHERE fileset = %s
            ORDER BY `timestamp`
        """, (id,))
        history = cursor.fetchall()

        cursor.execute("""
            SELECT `timestamp`, category, `text`, id
            FROM log
            WHERE `text` REGEXP %s
            ORDER BY `timestamp` DESC, id DESC
        """, (f'Fileset:{id}',))
        logs = cursor.fetchall()

        for history_row in history:
            cursor.execute("""
                SELECT `timestamp`, category, `text`, id
                FROM log
                WHERE `text` REGEXP %s
                AND `category` NOT REGEXP 'merge'
                ORDER BY `timestamp` DESC, id DESC
            """, (f'Fileset:{history_row["oldfileset"]}',))
            logs.extend(cursor.fetchall())

    if request.method == 'POST':
        if 'delete' in request.form:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE fileset SET `delete` = TRUE WHERE id = %s", (request.form['delete'],))
                conn.commit()
        if 'match' in request.form:
            match_and_merge_user_filesets(request.form['match'])
            return redirect(url_for('fileset', id=request.form['match']))

    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" href="{{ stylesheet }}">
        <script type="text/javascript" src="{{ jquery_file }}"></script>
        <script type="text/javascript" src="{{ js_file }}"></script>
    </head>
    <body>
        <h2><u>Fileset: {{ id }}</u></h2>
        <h3>Fileset details</h3>
        <table>
            {% for key, value in fileset_details.items() %}
                {% if key not in ['id', 'game'] %}
                    <tr><th>{{ key }}</th><td>{{ value }}</td></tr>
                {% endif %}
            {% endfor %}
        </table>
        <h3>Files in the fileset</h3>
        <form method="get">
            {% for key, value in request.args.items() %}
                {% if key != 'widetable' %}
                    <input type="hidden" name="{{ key }}" value="{{ value }}">
                {% endif %}
            {% endfor %}
            {% if request.args.get('widetable') == 'true' %}
                <input type="hidden" name="widetable" value="false">
                <input type="submit" value="Hide extra checksums">
            {% else %}
                <input type="hidden" name="widetable" value="true">
                <input type="submit" value="Expand Table">
            {% endif %}
        </form>
        <table>
            {% if files %}
                <tr>
                    <th>#</th>
                    {% for key in files[0].keys() %}
                        {% if key != 'id' %}
                            <th>{{ key }}</th>
                        {% endif %}
                    {% endfor %}
                </tr>
                {% for i, file in enumerate(files, 1) %}
                    <tr>
                        <td>{{ i }}</td>
                        {% for key, value in file.items() %}
                            {% if key != 'id' %}
                                <td>{{ value }}</td>
                            {% endif %}
                        {% endfor %}
                    </tr>
                {% endfor %}
            {% endif %}
        </table>
        <h3>Developer Actions</h3>
        <form method="post">
            <button type="submit" name="delete" value="{{ id }}">Mark Fileset for Deletion</button>
            <button type="submit" name="match" value="{{ id }}">Match and Merge Fileset</button>
        </form>
        <h3>Fileset history</h3>
        <table>
            <tr>
                <th>Timestamp</th>
                <th>Category</th>
                <th>Description</th>
                <th>Log ID</th>
            </tr>
            {% for log in logs %}
                <tr>
                    <td>{{ log.timestamp }}</td>
                    <td>{{ log.category }}</td>
                    <td>{{ log.text }}</td>
                    <td><a href="logs.php?id={{ log.id }}">{{ log.id }}</a></td>
                </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """, id=id, fileset_details=fileset_details, files=files, logs=logs, stylesheet='style.css', jquery_file='https://code.jquery.com/jquery-3.7.0.min.js', js_file='js_functions.js')


def get_join_columns(table1, table2, mapping):
    for primary, foreign in mapping.items():
        primary = primary.split('.')
        foreign = foreign.split('.')
        if (primary[0] == table1 and foreign[0] == table2) or (primary[0] == table2 and foreign[0] == table1):
            return f"{primary[0]}.{primary[1]} = {foreign[0]}.{foreign[1]}"
    return "No primary-foreign key mapping provided. Filter is invalid"

@app.route('/create_page', methods=['GET'])
def create_page():
    filename = 'filename'
    results_per_page = 10
    records_table = 'records_table'
    select_query = 'select_query'
    order = 'order'
    filters = {}
    mapping = {}

    mysql_cred = json.load(open(os.path.join(os.path.dirname(__file__), '../mysql_config.json')))
    connection = pymysql.connect(host=mysql_cred["servername"],
                                 user=mysql_cred["username"],
                                 password=mysql_cred["password"],
                                 db=mysql_cred["dbname"],
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        # TODO: Implement the logic to handle the GET parameters and construct the SQL query
        # similar logic as the PHP code to handle the GET parameters, construct the SQL query, execute it and fetch the results
        # ...
        pass
    
    # TODO: Implement the logic to construct the HTML table and pagination elements
    # similar logic as the PHP code to construct the HTML table and pagination elements
    # ...

    return render_template("fileset.html")

if __name__ == '__main__':
    app.run()