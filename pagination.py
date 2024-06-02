from urllib.parse import urlencode
import pymysql
import json
from flask import Flask, request, render_template_string
import os
import re
from math import ceil
import html

stylesheet = 'style.css'
jquery_file = 'https://code.jquery.com/jquery-3.7.0.min.js'
js_file = 'js_functions.js'
print(f"<link rel='stylesheet' href='{stylesheet}'>\n")
print(f"<script type='text/javascript' src='{jquery_file}'></script>\n")
print(f"<script type='text/javascript' src='{js_file}'></script>\n")


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


def get_join_columns(table1, table2, mapping):
    for primary, foreign in mapping.items():
        primary = primary.split('.')
        foreign = foreign.split('.')
        if (primary[0] == table1 and foreign[0] == table2) or (primary[0] == table2 and foreign[0] == table1):
            return f"{primary[0]}.{primary[1]} = {foreign[0]}.{foreign[1]}"
    raise ValueError("No primary-foreign key mapping provided. Filter is invalid")

def create_page(filename, results_per_page, records_table, select_query, order, filters = {}, mapping = {}):
    with open(os.path.join(os.path.dirname(__file__), '../mysql_config.json')) as f:
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

    # Check connection
    if not conn.open:
        print("Connect failed.")
        return

    # If there exist get variables that are for filtering
    get = {k: v for k, v in request.args.items() if v != ''}
    if 'sort' in get:
        column = get['sort'].split('-')
        order = "ORDER BY {}".format(column[0])

        if 'desc' in get['sort']:
            order += " DESC"

    if set(get.keys()) - set(['page', 'sort']):
        condition = "WHERE "
        tables = []
        for key, value in get.items():
            if key in ['page', 'sort'] or value == '':
                continue

            tables.append(filters[key])
            condition += " AND {}.{} REGEXP '{}'".format(filters[key], key, value) if condition != "WHERE " else "{}.{} REGEXP '{}'".format(filters[key], key, value)
        if condition == "WHERE ":
            condition = ""

        # If more than one table is to be searched
        from_query = records_table
        if len(tables) > 1 or tables[0] != records_table:
            for i in range(len(tables)):
                if tables[i] == records_table:
                    continue

                from_query += " JOIN {} ON {}".format(tables[i], get_join_columns(records_table, tables[i], mapping))

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT({}.id) FROM {} {}".format(records_table, from_query, condition))
        num_of_results = cursor.fetchone()[0]
    # If $records_table has a JOIN (multiple tables)
    elif re.search("JOIN", records_table):
        first_table = records_table.split(" ")[0]
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT({}.id) FROM {}".format(first_table, records_table))
        num_of_results = cursor.fetchone()[0]
    else:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(id) FROM {}".format(records_table))
        num_of_results = cursor.fetchone()[0]
    num_of_pages = ceil(num_of_results / results_per_page)
    if num_of_results == 0:
        print("No results for given filters")
        return

    if 'page' not in get:
        page = 1
    else:
        page = max(1, min(int(get['page']), num_of_pages))

    offset = (page - 1) * results_per_page

    # If there exist get variables that are for filtering
    if set(get.keys()) - set(['page']):
        condition = "WHERE "
        for key, value in get.items():
            value = conn.converter.escape(value)
            if key not in filters:
                continue

            condition += "AND {}.{} REGEXP '{}'".format(filters[key], key, value) if condition != "WHERE " else "{}.{} REGEXP '{}'".format(filters[key], key, value)
        if condition == "WHERE ":
            condition = ""

        query = "{} {} {} LIMIT {} OFFSET {}".format(select_query, condition, order, results_per_page, offset)
    else:
        query = "{} {} LIMIT {} OFFSET {}".format(select_query, order, results_per_page, offset)
    cursor = conn.cursor()
    cursor.execute(query)

    # Table
    print("<form id='filters-form' method='GET' onsubmit='remove_empty_inputs()'>")
    print("<table>")

    counter = offset + 1
    for row in cursor.fetchall():
        if counter == offset + 1: # If it is the first run of the loop
            if len(filters) > 0:
                print("<tr class=filter><td></td>")
                for key in row.keys():
                    if key not in filters:
                        print("<td class=filter />")
                        continue

                    # Filter textbox
                    filter_value = get[key] if key in get else ""

                    print("<td class=filter><input type=text class=filter placeholder='{}' name='{}' value='{}'/></td>".format(key, key, filter_value))
                print("</tr>")
                print("<tr class=filter><td></td><td class=filter><input type=submit value='Submit'></td></tr>")

            print("<th/>") # Numbering column
            for key in row.keys():
                if key == 'fileset':
                    continue

                # Preserve GET variables
                vars = ""
                for k, v in get.items():
                    if k == 'sort' and v == key:
                        vars += "&{}={}-desc".format(k, v)
                    elif k != 'sort':
                        vars += "&{}={}".format(k, v)

                if "&sort={}".format(key) not in vars:
                    print("<th><a href='{}?{}&sort={}'>{}</th>".format(filename, vars, key, key))
                else:
                    print("<th><a href='{}?{}'>{}</th>".format(filename, vars, key))

        if filename in ['games_list.php', 'user_games_list.php']:
            print("<tr class=games_list onclick='hyperlink(\"fileset.php?id={}\")'>".format(row['fileset']))
        else:
            print("<tr>")
        print("<td>{}.</td>".format(counter))
        for key, value in row.items():
            if key == 'fileset':
                continue

            # Add links to fileset in logs table
            matches = re.search("Fileset:(\d+)", value)
            if matches:
                value = value[:matches.start()] + "<a href='fileset.php?id={}'>{}</a>".format(matches.group(1), matches.group(0)) + value[matches.end():]

            print("<td>{}</td>".format(value))
        print("</tr>")

        counter += 1

    print("</table>")
    print("</form>")

    # Preserve GET variables
    vars = ""
    for key, value in get.items():
        if key == 'page':
            continue
        vars += "&{}={}".format(key, value)

    # Navigation elements
    if num_of_pages > 1:
        print("<form method='GET'>")

        # Preserve GET variables on form submit
        for key, value in get.items():
            if key == 'page':
                continue

            key = html.escape(key)
            value = html.escape(value)
            if value != "":
                print("<input type='hidden' name='{}' value='{}'>".format(key, value))

        print("<div class=pagination>")
        if page > 1:
            print("<a href={}{}>❮❮</a>".format(filename, vars))
            print("<a href={}page={}{}>❮</a>".format(filename, page - 1, vars))
        if page - 2 > 1:
            print("<div class=more>...</div>")

        for i in range(page - 2, page + 3):
            if i >= 1 and i <= num_of_pages:
                if i == page:
                    print("<a class=active href={}page={}{}>{}</a>".format(filename, i, vars, i))
                else:
                    print("<a href={}page={}{}>{}</a>".format(filename, i, vars, i))

        if page + 2 < num_of_pages:
            print("<div class=more>...</div>")
        if page < num_of_pages:
            print("<a href={}page={}{}>❯</a>".format(filename, page + 1, vars))
            print("<a href={}page={}{}>❯❯</a>".format(filename, num_of_pages, vars))

        print("<input type='text' name='page' placeholder='Page No'>")
        print("<input type='submit' value='Submit'>")
        print("</div>")

        print("</form>")
