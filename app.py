import os, json, flask
from  flask_restful import reqparse
from flask import Flask, request, render_template
from flask_restful import Resource, Api
import pandas as pd
from fetch_data import fetch_data, fetch_data_custom
from dataviz.data_viz import generate_charts
from config import config

app = Flask(__name__)
api = Api(app)

sql_parser = reqparse.RequestParser()
sql_parser.add_argument('limit', type=int, help="Limit the returned rows of SQL Query")
sql_parser.add_argument('filter', type=str, help="filter the returned rows of SQL Query")
sql_parser.add_argument('per_page', type=int, help="Per Page limit")

# custom_sql_args = reqparse.RequestParser()
# custom_sql_args.add_argument("sql", type=str, help="Send SQL Query", required=True)

def get_paginated_list(results, url, start, per_page=10):
    start = int(start)
    per_page = int(per_page)
    count = len(results)
    if count < start or per_page < 0:
        flask.abort(404)

    obj = {}
    obj['start'] = start
    obj['per_page'] = per_page
    obj['count'] = count

    if start == 1:
        obj['previous'] = ''
    else:
        start_copy = max(1, start - per_page)
        per_page_copy = start - 1
        obj['previous'] = url + '?start=%d&per_page=%d' % (start_copy, per_page_copy)

    if start + per_page > count:
        obj['next'] = ''
    else:
        start_copy = start + per_page
        obj['next'] = url + '?start=%d&per_page=%d' % (start_copy, per_page)

    obj['results'] = results[(start - 1):(start - 1 + per_page)]
    return obj


@app.route('/')
@app.route("/home")
def home_page():
    return render_template('home.html')


@app.route("/list_api")
def list_api_page():
    return render_template('list_api.html', api_list_items=[fname.split('.')[0] for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False])


@app.route("/fetch_apps/<string:tablename>", methods=['GET'])
def fetchdata_apps(tablename):
    #tablename='accnt'
    sql_arg = sql_parser.parse_args()
    if sql_arg['filter'] is not None:
        sql_filter = ' where ' + sql_arg['filter'] + ' '
    else:
        sql_filter = ' where 1 = 1'
    if sql_arg['limit'] and sql_arg['limit'] > 0:
        sql_limit = ' limit ' + str(sql_arg['limit'])
    else:
        sql_limit = ''
        sql_arg['per_page'] = 10
    if sql_arg['per_page'] is None:
        sql_arg['per_page'] = 10
    df = pd.DataFrame(fetch_data(tablename, sql_filter + sql_limit))
    return flask.jsonify(get_paginated_list(
        json.loads(df.to_json(orient='records')),
        request.url_root + 'fetch_apps/' + tablename,
        start=request.args.get('start', 1),
        per_page=request.args.get('per_page', sql_arg['per_page'])))


class FetchApp(Resource):
    def __init__(self):
        pass

    def get(self):
        #directory = config.project_sql
        api_list = [fname.split('.')[0] for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False]
        return flask.jsonify(api_list)


class GetViz(Resource):
    def __init__(self):
        pass

    def get(self):

        #directory = config.project_sql
        sql_file_list = [fname for fname in os.listdir(config.project_sql) if os.path.isdir(fname) == False and fname.endswith('.sql')]
        viz_file = generate_charts(sql_file_list)
        #print(viz_file)
        if '.' in viz_file:
            if viz_file.split('.')[1] == 'pdf' or viz_file.split('.')[1] == 'zip':
                return flask.send_file(viz_file)
            else:
                return viz_file
        else:
            return viz_file



class CustomSql(Resource):
    def __init__(self):
        pass

    # def get(self, custsql):
    #    return custsql

    def post(self):
	obj={}	  
        data = request.get_json()
        sql_args = sql_parser.parse_args()
        if sql_arg['limit'] and sql_arg['limit'] > 0:
            sql_limit = ' limit ' + str(sql_arg['limit'])
        else:
            sql_limit = ''
            sql_filter = ' where 1 = 1'
        df = pd.DataFrame(fetch_data_custom(data, sql_filter + sql_limit))
        obj['results'] = json.loads(df.to_json(orient='records'))
        return flask.jsonify(obj)
    


api.add_resource(FetchApp, '/fetch_apps')
api.add_resource(CustomSql, '/customsql')
api.add_resource(GetViz, '/getviz')
if __name__ == '__main__':
    port = 5000
    app.run(debug=True, host='0.0.0.0', port=port)
