from urllib.parse import urlencode
import pymysql
import json
import math
from flask import Flask, request, render_template_string

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

def create_page(filename, results_per_page, records_table, select_query, order, filters=None, mapping=None):
    if filters is None:
        filters = {}
    if mapping is None:
        mapping = {}

    with conn.cursor() as cursor:
        # If there exist get variables that are for filtering
        get_params = {k: v for k, v in request.args.items() if v}
        if 'sort' in get_params:
            column = get_params.pop('sort')
            order = f"ORDER BY {column.split('-')[0]}"
            if 'desc' in column:
                order += " DESC"

        tables = list(set(filters.values()))
        condition = "WHERE " + " AND ".join([f"{filters[k]}.{k} REGEXP '{v}'" for k, v in get_params.items() if k != 'page']) if get_params else ""
        
        from_query = records_table
        if len(tables) > 1 or (tables and tables[0] != records_table):
            for table in tables:
                if table != records_table:
                    from_query += f" JOIN {table} ON {get_join_columns(records_table, table, mapping)}"

        count_query = f"SELECT COUNT({records_table}.id) FROM {from_query} {condition}"
        cursor.execute(count_query)
        num_of_results = cursor.fetchone()['COUNT({records_table}.id)']
        num_of_pages = math.ceil(num_of_results / results_per_page)

        page = max(1, min(int(get_params.pop('page', 1)), num_of_pages))
        offset = (page - 1) * results_per_page

        query = f"{select_query} {condition} {order} LIMIT {results_per_page} OFFSET {offset}"
        cursor.execute(query)
        results = cursor.fetchall()

    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <link rel="stylesheet" href="{{ stylesheet }}">
        <script type="text/javascript" src="{{ jquery_file }}"></script>
        <script type="text/javascript" src="{{ js_file }}"></script>
    </head>
    <body>
        <form id='filters-form' method='GET' onsubmit='remove_empty_inputs()'>
        <table>
            {% if results %}
                <tr class="filter">
                    <td></td>
                    {% for key in results[0].keys() %}
                        {% if key in filters %}
                            <td class="filter">
                                <input type="text" class="filter" placeholder="{{ key }}" name="{{ key }}" value="{{ request.args.get(key, '') }}"/>
                            </td>
                        {% else %}
                            <td class="filter"></td>
                        {% endif %}
                    {% endfor %}
                </tr>
                <tr class="filter">
                    <td></td>
                    <td class="filter"><input type="submit" value="Submit"></td>
                </tr>
                <tr>
                    <th></th>
                    {% for key in results[0].keys() %}
                        {% if key != 'fileset' %}
                            <th><a href="{{ url_for('create_page', **{**request.args, 'sort': key}) }}">{{ key }}</a></th>
                        {% endif %}
                    {% endfor %}
                </tr>
                {% for i, row in enumerate(results, start=offset+1) %}
                    <tr>
                        <td>{{ i }}</td>
                        {% for key, value in row.items() %}
                            {% if key != 'fileset' %}
                                <td>{{ value }}</td>
                            {% endif %}
                        {% endfor %}
                    </tr>
                {% endfor %}
            {% endif %}
        </table>
        </form>

        <div class="pagination">
            {% if num_of_pages > 1 %}
                <form method="GET">
                    {% for key, value in request.args.items() %}
                        {% if key != 'page' %}
                            <input type="hidden" name="{{ key }}" value="{{ value }}">
                        {% endif %}
                    {% endfor %}
                    {% if page > 1 %}
                        <a href="{{ url_for('create_page', **{**request.args, 'page': 1}) }}">❮❮</a>
                        <a href="{{ url_for('create_page', **{**request.args, 'page': page-1}) }}">❮</a>
                    {% endif %}
                    {% if page - 2 > 1 %}
                        <div class="more">...</div>
                    {% endif %}
                    {% for i in range(max(1, page-2), min(num_of_pages+1, page+3)) %}
                        {% if i == page %}
                            <a class="active" href="{{ url_for('create_page', **{**request.args, 'page': i}) }}">{{ i }}</a>
                        {% else %}
                            <a href="{{ url_for('create_page', **{**request.args, 'page': i}) }}">{{ i }}</a>
                        {% endif %}
                    {% endfor %}
                    {% if page + 2 < num_of_pages %}
                        <div class="more">...</div>
                    {% endif %}
                    {% if page < num_of_pages %}
                        <a href="{{ url_for('create_page', **{**request.args, 'page': page+1}) }}">❯</a>
                        <a href="{{ url_for('create_page', **{**request.args, 'page': num_of_pages}) }}">❯❯</a>
                    {% endif %}
                    <input type="text" name="page" placeholder="Page No">
                    <input type="submit" value="Submit">
                </form>
            {% endif %}
        </div>
    </body>
    </html>
    """, results=results, filters=filters, request=request.args, offset=offset, num_of_pages=num_of_pages, page=page, filename=filename, stylesheet='style.css', jquery_file='https://code.jquery.com/jquery-3.7.0.min.js', js_file='js_functions.js')

