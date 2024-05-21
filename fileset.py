from flask import Flask, request, render_template, redirect, url_for
import pymysql.cursors
import json
import re

app = Flask(__name__)

# Load MySQL credentials
with open('mysql_config.json') as f:
    mysql_cred = json.load(f)

# Connect to the database
connection = pymysql.connect(host=mysql_cred["servername"],
                             user=mysql_cred["username"],
                             password=mysql_cred["password"],
                             db=mysql_cred["dbname"],
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor,
                             autocommit=False)

@app.route('/fileset', methods=['GET', 'POST'])
def fileset():
    try:
        with connection.cursor() as cursor:
            # Check connection
            cursor.execute("SELECT 1")
            
            # Get min and max id
            cursor.execute("SELECT MIN(id) FROM fileset")
            min_id = cursor.fetchone()['MIN(id)']
            cursor.execute("SELECT MAX(id) FROM fileset")
            max_id = cursor.fetchone()['MAX(id)']
            
            # Get id from GET parameters or use min_id
            id = request.args.get('id', min_id)
            id = max(min_id, min(int(id), max_id))
            
            # Check if id exists in fileset
            cursor.execute(f"SELECT id FROM fileset WHERE id = {id}")
            if cursor.rowcount == 0:
                cursor.execute(f"SELECT fileset FROM history WHERE oldfileset = {id}")
                id = cursor.fetchone()['fileset']
            
            # Get fileset details
            cursor.execute(f"SELECT * FROM fileset WHERE id = {id}")
            result = cursor.fetchone()
            
            # Get files in the fileset
            cursor.execute(f"SELECT file.id, name, size, checksum, detection FROM file WHERE fileset = {id}")
            files = cursor.fetchall()
            
            # Get history and logs
            cursor.execute(f"SELECT `timestamp`, oldfileset, log FROM history WHERE fileset = {id} ORDER BY `timestamp`")
            history = cursor.fetchall()
            
            cursor.execute(f"SELECT `timestamp`, category, `text`, id FROM log WHERE `text` REGEXP 'Fileset:{id}' ORDER BY `timestamp` DESC, id DESC")
            logs = cursor.fetchall()
            
            # Commit the transaction
            connection.commit()
            
            # Render the results in a template
            return render_template('fileset.html', result=result, files=files, history=history, logs=logs)
    finally:
        connection.close()

if __name__ == '__main__':
    app.run()